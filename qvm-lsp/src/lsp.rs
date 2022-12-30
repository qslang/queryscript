use serde::{Deserialize, Serialize};
use serde_json::json;
use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path as FilePath;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use tokio::{sync::Mutex as TokioMutex, sync::RwLock as TokioRwLock, task};

use tower_lsp::jsonrpc::{Error, Result};
use tower_lsp::{lsp_types, lsp_types::*, LspServiceBuilder};
use tower_lsp::{Client, LanguageServer};

use qvm::{
    ast,
    ast::{Location, Pretty, SourceLocation},
    compile,
    compile::{
        autocomplete::{loc_to_pos, pos_to_loc, AutoCompleter},
        error::CompileError,
        schema::{CRef, Decl, MFnType, MType, SType, SchemaEntry},
        Compiler, Schema, SchemaRef,
    },
    parser::{error::PrettyError, parse_schema},
    runtime,
    types::Type as QVMType,
};

// XXX Do we need any of these settings? If not, we can probably remove them.
#[derive(Debug)]
struct Configuration {
    pub has_configuration_capability: bool,
    pub has_workspace_folder_capability: bool,
    pub has_diagnostic_related_information_capability: bool,
}

#[derive(Debug)]
struct Settings {
    pub max_number_of_rows: i64,
}

// XXX Remove this or add real settings in
#[allow(unused)]
const DEFAULT_SETTINGS: Settings = Settings {
    max_number_of_rows: 1000,
};

#[derive(Debug, Clone)]
pub struct Symbol {
    pub name: ast::Ident,
    pub kind: compile::SymbolKind,
    pub type_: CRef<SType>,
    pub def: SourceLocation,
    pub decl: Option<Decl>,
    pub references: BTreeSet<SourceLocation>,
}

#[derive(Debug, Clone)]
pub struct Document {
    pub uri: Url,
    pub version: Option<i32>,
    pub text: String,
    pub schema: Option<SchemaRef>,
    pub symbols: BTreeMap<Location, Symbol>,
}

impl Document {
    pub fn get_symbol(&mut self, loc: Location) -> Option<&mut Symbol> {
        use std::ops::Bound::{Included, Unbounded};

        self.symbols
            .range_mut((Unbounded, Included(&loc)))
            .last()
            .map(|r| {
                if let Some(range) = r.1.name.loc.range() {
                    if range.end >= loc {
                        return Some(r.1);
                    }
                }

                return None;
            })
            .flatten()
    }
}

#[derive(Debug)]
pub struct Backend {
    client: Client,
    configuration: Arc<RwLock<Configuration>>,

    // XXX The LSP example from Microsoft implements this by remembering the settings each
    // file was analayzed with, and then changing it when the settings are updated. From
    // what I can tell, these settings are _not_ per file. Before we ship this, we should
    // either follow what the Microsoft example does or remove it.
    settings: Arc<RwLock<BTreeMap<String, Settings>>>,

    documents: Arc<TokioRwLock<BTreeMap<Url, Arc<TokioMutex<Document>>>>>,

    // We use a mutex for the compiler, because we want to serialize compilation
    // (not sure if the compiler can interleave multiple compilations correctly,
    // even though it uses thread-safe data structures).
    compiler: Arc<Mutex<Compiler>>,
}

impl Backend {
    pub async fn build_compiler() -> Arc<Mutex<Compiler>> {
        task::spawn_blocking(move || {
            Arc::new(Mutex::new(
                Compiler::new().expect("failed to initialize QVM compiler"),
            ))
        })
        .await
        .expect("failed to initialize QVM compiler (thread error)")
    }

    pub fn new(client: Client, compiler: Arc<Mutex<Compiler>>) -> Result<Backend> {
        let documents = Arc::new(TokioRwLock::new(BTreeMap::new()));
        compiler
            .lock()
            .map_err(log_internal_error)?
            .on_schema(Some(Box::new(SchemaRecorder {
                tokio_handle: tokio::runtime::Handle::current(),
                client: client.clone(),
                documents: documents.clone(),
            })))
            .map_err(log_internal_error)?;
        let backend = Backend {
            client,
            configuration: Arc::new(RwLock::new(Configuration {
                has_configuration_capability: false,
                has_workspace_folder_capability: false,
                has_diagnostic_related_information_capability: false,
            })),
            settings: Arc::new(RwLock::new(BTreeMap::new())),
            documents,
            compiler,
        };
        Ok(backend)
    }

    pub fn add_custom_methods(service: LspServiceBuilder<Backend>) -> LspServiceBuilder<Backend> {
        service.custom_method("qvm/runExpr", Backend::run_expr)
    }
}

fn log_internal_error<E: std::fmt::Debug>(e: E) -> Error {
    eprintln!("Internal error: {:?}", e);
    Error::internal_error()
}

fn log_result<T, E: std::fmt::Debug>(r: std::result::Result<T, E>) {
    match r {
        Ok(_) => {}
        Err(e) => {
            eprintln!("Internal error: {:?}", e);
        }
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        let capabilities = params.capabilities;

        let mut config = self.configuration.write().unwrap();

        config.has_configuration_capability = match &capabilities.workspace {
            Some(workspace) => workspace.configuration.map_or(false, |x| x),
            None => false,
        };

        config.has_workspace_folder_capability = match &capabilities.workspace {
            Some(workspace) => workspace.workspace_folders.map_or(false, |x| x),
            None => false,
        };

        config.has_diagnostic_related_information_capability = match &capabilities.text_document {
            Some(text_document) => text_document
                .publish_diagnostics
                .as_ref()
                .map_or(false, |publish_diagnostics| {
                    publish_diagnostics.related_information.map_or(false, |x| x)
                }),
            None => false,
        };

        let result = InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    // Although the tutorial uses INCREMENTAL, we have to use FULL because from
                    // what I can tell, TowerLSP doesn't help you apply INCREMENTAL updates.
                    // TextDocumentSyncKind::INCREMENTAL,
                    TextDocumentSyncKind::FULL,
                )),
                completion_provider: Some(CompletionOptions {
                    ..Default::default()
                }),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                definition_provider: Some(OneOf::Left(true)),
                references_provider: Some(OneOf::Left(true)),
                code_lens_provider: Some(CodeLensOptions {
                    resolve_provider: Some(true),
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(result)
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "server initialized!")
            .await;

        let (has_configuration_capability, has_workspace_folder_capability) = {
            let config = self.configuration.read().unwrap();
            (
                config.has_configuration_capability,
                config.has_workspace_folder_capability,
            )
        };
        if has_configuration_capability {
            self.client
                .register_capability(vec![Registration {
                    id: "config".to_string(),
                    method: "workspace/didChangeConfiguration".to_string(),
                    register_options: None,
                }])
                .await
                .unwrap();
        }

        if has_workspace_folder_capability {
            self.client
                .register_capability(vec![Registration {
                    id: "workspace".to_string(),
                    method: "workspace/didChangeWorkspaceFolders".to_string(),
                    register_options: None,
                }])
                .await
                .unwrap();
        }
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    // XXX REMOVE
    async fn did_change_configuration(&self, _params: DidChangeConfigurationParams) {
        if self
            .configuration
            .read()
            .unwrap()
            .has_configuration_capability
        {
            self.settings.write().unwrap().clear();
        }

        /* XXX FIX
        let mut futures = Vec::new();
        for (uri, document) in self.documents.read().unwrap().iter() {
            futures.push(self.on_change(Url::parse(uri).unwrap(), document.clone(), None));
        }

        for future in futures {
            future.await;
        }
        */
    }

    async fn did_change_workspace_folders(&self, _params: DidChangeWorkspaceFoldersParams) {
        eprintln!("Workspace folder change event received.");
    }

    async fn did_change(&self, mut params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri;
        let version = params.text_document.version;

        // NOTE: If we change this to INCREMENTAL we'll need to recompute the new document text
        let text = params.content_changes.swap_remove(0).text;

        log_result(self.on_change(uri, text, Some(version)).await);
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri;
        let version = params.text_document.version;
        let text = params.text_document.text;

        log_result(self.on_change(uri, text, Some(version)).await);
    }

    async fn did_change_watched_files(&self, _params: DidChangeWatchedFilesParams) {
        eprintln!("We receved a file change event.");
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        eprintln!("Starting completion");
        let position = params.text_document_position.position;
        let uri = params.text_document_position.text_document.uri.clone();
        let file = uri.path().to_string();

        let folder = FilePath::new(uri.path())
            .parent()
            .map(|p| p.to_str().unwrap())
            .map(String::from);

        let text = self.get_text(&uri).await?;
        let loc = qvm::ast::Location {
            line: (position.line + 1) as u64,
            column: (position.character + 1) as u64,
        };
        let pos = loc_to_pos(text.as_str(), loc.clone());
        let schema_ast = parse_schema(file.as_str(), text.as_str()).result;
        let mut stmt = None;
        for s in &schema_ast.stmts {
            if &s.start > &loc {
                break;
            }
            stmt = Some(s);
        }
        let (start_pos, start_loc, line) = if let Some(stmt) = stmt {
            let (start_pos, end_pos) = (
                loc_to_pos(text.as_str(), stmt.start.clone()),
                loc_to_pos(text.as_str(), stmt.end.clone()),
            );
            (
                start_pos,
                stmt.start.clone(),
                text[start_pos..end_pos + 1].to_string(),
            )
        } else {
            (pos, loc.clone(), String::new())
        };

        eprintln!("Line: {} {}", start_pos, line);
        let compiler = self.compiler.clone();
        let (suggestion_pos, suggestions) = task::spawn_blocking({
            move || -> Result<_> {
                let compiler = compiler.lock().map_err(log_internal_error)?;
                let schema = Schema::new(file, folder);
                compiler.compile_schema_ast(schema.clone(), &schema_ast);
                let autocompleter = AutoCompleter::new(
                    compiler.clone(),
                    schema,
                    Rc::new(RefCell::new(String::new())),
                );
                Ok(autocompleter.auto_complete(line.as_str(), pos - start_pos))
            }
        })
        .await
        .map_err(log_internal_error)?
        .map_err(log_internal_error)?;
        let suggestion_loc = pos_to_loc(text.as_str(), start_pos + suggestion_pos);
        eprintln!(
            "Location: {} {:?} {} {:?} {}",
            start_pos,
            start_loc,
            suggestion_pos,
            suggestion_loc,
            loc_to_pos(text.as_str(), suggestion_loc.clone())
        );

        Ok(Some(CompletionResponse::List(CompletionList {
            is_incomplete: true,
            items: suggestions
                .iter()
                .map(|s| CompletionItem {
                    label: s.clone(),
                    text_edit: Some(CompletionTextEdit::Edit(TextEdit {
                        range: Range {
                            start: suggestion_loc.normalize(),
                            end: position,
                        },
                        new_text: s.clone(),
                    })),
                    ..Default::default()
                })
                .collect(),
        })))
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let loc = {
            let Position { line, character } = params.text_document_position_params.position;
            Location {
                line: (line + 1) as u64,
                column: (character + 1) as u64,
            }
        };
        if let Some(symbol) = self
            .get_symbol(
                params.text_document_position_params.text_document.uri,
                loc.clone(),
            )
            .await?
        {
            let mut contents = vec![MarkedString::LanguageString(LanguageString {
                language: "".into(),
                value: symbol.def.pretty(),
            })];
            let formatted = format_symbol(&symbol)?;
            if formatted.len() > 0 {
                contents.push(MarkedString::LanguageString(LanguageString {
                    language: "tql".into(),
                    value: formatted,
                }));
            }

            Ok(symbol.name.loc.range().map(|range| Hover {
                contents: HoverContents::Array(contents),
                range: Some(range.normalize()),
            }))
        } else {
            Ok(None)
        }
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let loc = {
            let Position { line, character } = params.text_document_position_params.position;
            Location {
                line: (line + 1) as u64,
                column: (character + 1) as u64,
            }
        };
        if let Some(symbol) = self
            .get_symbol(
                params.text_document_position_params.text_document.uri,
                loc.clone(),
            )
            .await?
        {
            Ok(match &symbol.def {
                SourceLocation::File(f) => {
                    if let Ok(uri) = Url::from_file_path(FilePath::new(f)) {
                        Some(lsp_types::Location {
                            uri,
                            range: FULL_DOCUMENT_RANGE,
                        })
                    } else {
                        None
                    }
                }
                _ => symbol.def.normalize(),
            }
            .map(|l| GotoDefinitionResponse::Scalar(l)))
        } else {
            Ok(None)
        }
    }

    async fn references(
        &self,
        params: ReferenceParams,
    ) -> Result<Option<Vec<lsp_types::Location>>> {
        let loc = {
            let Position { line, character } = params.text_document_position.position;
            Location {
                line: (line + 1) as u64,
                column: (character + 1) as u64,
            }
        };
        if let Some(symbol) = self
            .get_symbol(params.text_document_position.text_document.uri, loc.clone())
            .await?
        {
            Ok(Some(
                symbol
                    .references
                    .iter()
                    .flat_map(|l| l.normalize())
                    .collect(),
            ))
        } else {
            Ok(None)
        }
    }

    async fn code_lens(&self, params: CodeLensParams) -> Result<Option<Vec<CodeLens>>> {
        let uri = params.text_document.uri;
        let schema = match self.get_schema(&uri).await? {
            Some(schema) => schema,
            None => return Ok(None),
        };

        let mut lenses = Vec::new();
        for (idx, expr) in schema
            .read()
            .map_err(log_internal_error)?
            .exprs
            .iter()
            .enumerate()
        {
            let loc = match expr.location().normalize() {
                Some(loc) => loc,
                None => continue,
            };

            let command = Command {
                title: "Run".to_string(),
                command: "runExpr.start".to_string(),
                arguments: Some(vec![json!(uri.to_string()), json!(RunExprType::Query(idx))]),
            };

            lenses.push(CodeLens {
                range: loc.range,
                command: Some(command),
                data: None,
            });
        }

        for (name, decl) in schema.read().map_err(log_internal_error)?.decls.iter() {
            if !is_runnable_decl(&decl.value)? {
                continue;
            }

            let loc = match decl.location().normalize() {
                Some(loc) => loc,
                None => continue,
            };

            let command = Command {
                title: "Preview".to_string(),
                command: "runExpr.start".to_string(),
                arguments: Some(vec![json!(uri), json!(RunExprType::Expr(name.clone()))]),
            };

            lenses.push(CodeLens {
                range: loc.range,
                command: Some(command),
                data: None,
            });
        }
        Ok(Some(lenses))
    }
}

fn get_fn_type(type_: CRef<SType>) -> Result<Option<(MFnType, BTreeSet<String>)>> {
    if type_.is_known().map_err(log_internal_error)? {
        let type_ = type_.must().map_err(log_internal_error)?;
        let type_ = type_.read().map_err(log_internal_error)?;
        if type_.body.is_known().map_err(log_internal_error)? {
            let body = type_.body.must().map_err(log_internal_error)?;
            let body = body.read().map_err(log_internal_error)?;
            match &*body {
                MType::Fn(mfn) => return Ok(Some((mfn.get().clone(), type_.variables.clone()))),
                _ => {}
            }
        }
    }
    Ok(None)
}

fn format_symbol(symbol: &Symbol) -> Result<String> {
    let is_public = symbol.decl.as_ref().map_or(false, |decl| decl.public);
    let mut parts = Vec::<String>::new();
    if is_public {
        parts.push("export ".into());
    }
    match symbol.kind {
        compile::SymbolKind::Value => {
            if let Some((mfn, variables)) = get_fn_type(symbol.type_.clone())? {
                parts.push("fn ".into());
                parts.push(symbol.name.value.clone());
                if variables.len() > 0 {
                    parts.push("<".into());
                    for (i, v) in variables.into_iter().enumerate() {
                        if i > 0 {
                            parts.push(", ".into());
                        }

                        parts.push(v);
                    }
                    parts.push(">".into());
                }
                parts.push("(\n".into());
                for f in mfn.args.into_iter() {
                    parts.push("\t".into());
                    parts.push(f.name.into());
                    parts.push(" ".into());
                    parts.push(format!("{:#?}", f.type_).replace("\n", "\n\t"));
                    parts.push(",\n".into());
                }
                parts.push(") ".into());
                parts.push(format!("{:#?}", mfn.ret));
            } else {
                parts.push("let ".into());
                parts.push(symbol.name.value.clone());
                parts.push(" ".into());
                parts.push(format!("{:#?}", symbol.type_));
            }
        }
        compile::SymbolKind::Type => {
            parts.push("type ".into());
            parts.push(symbol.name.value.clone());
            parts.push(" ".into());
            parts.push(format!("{:#?}", symbol.type_));
        }
        compile::SymbolKind::Argument | compile::SymbolKind::Field => {
            parts.push(symbol.name.value.clone());
            parts.push(" ".into());
            parts.push(format!("{:#?}", symbol.type_));
        }
        compile::SymbolKind::File => {}
    }
    Ok(parts.join(""))
}

fn is_runnable_decl(decl: &SchemaEntry) -> Result<bool> {
    Ok(match decl {
        SchemaEntry::Schema(_) | SchemaEntry::Type(_) => false,
        SchemaEntry::Expr(e) => match e.to_runtime_type() {
            Ok(e) => match *(e.type_.read().map_err(log_internal_error)?) {
                QVMType::Fn(_) => false,
                _ => true,
            },
            Err(_) => false,
        },
    })
}

fn get_runnable_expr_from_decl(
    decl: &SchemaEntry,
) -> Result<qvm::compile::schema::TypedExpr<qvm::compile::schema::Ref<QVMType>>> {
    let s_expr = match decl {
        SchemaEntry::Expr(ref expr) => expr.clone(),
        _ => {
            return Err(Error::invalid_params(format!(
                "Invalid expression (wrong kind of decl): {:?}",
                decl
            )))
        }
    };
    Ok(s_expr.to_runtime_type().map_err(|e| {
        eprintln!("Failed to convert expression to runtime type: {:?}", e);
        Error::internal_error()
    })?)
}

fn find_expr_by_location(
    schema: &Schema,
    loc: &qvm::ast::Location,
) -> Result<Option<qvm::compile::schema::TypedExpr<qvm::compile::schema::Ref<QVMType>>>> {
    if let Some(expr) = schema.exprs.iter().find(|expr| {
        let expr_loc = expr.location();
        expr_loc.contains(loc)
    }) {
        return Ok(Some(expr.clone().to_runtime_type().map_err(|e| {
            eprintln!("Failed to convert query to runtime type: {:?}", e);
            Error::internal_error()
        })?));
    }

    if let Some((_name, decl)) = schema.decls.iter().find(|(_name, decl)| {
        let runnable = match is_runnable_decl(&decl.value) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("Failed to determine runnability: {:?}", e);
                false
            }
        };
        runnable && decl.location().contains(loc)
    }) {
        return Ok(Some(get_runnable_expr_from_decl(&decl.value)?));
    }

    Ok(None)
}

struct SymbolRecorder {
    pub symbols: BTreeMap<Url, Vec<Symbol>>,
}
impl SymbolRecorder {
    pub fn new() -> Self {
        SymbolRecorder {
            symbols: BTreeMap::new(),
        }
    }

    pub fn claim_symbols(&mut self) -> BTreeMap<Url, Vec<Symbol>> {
        let mut symbols = BTreeMap::new();
        std::mem::swap(&mut self.symbols, &mut symbols);
        symbols
    }
}
impl compile::OnSymbol for SymbolRecorder {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn on_symbol(
        &mut self,
        name: ast::Ident,
        kind: compile::SymbolKind,
        type_: CRef<SType>,
        def: SourceLocation,
        decl: Option<Decl>,
    ) -> compile::Result<()> {
        let file = name.loc.file();
        if let Some(file) = file {
            let uri = match Url::from_file_path(FilePath::new(&file)) {
                Ok(uri) => uri,
                Err(_) => return Ok(()),
            };

            let symbol = Symbol {
                name,
                kind,
                type_,
                def,
                decl,
                references: BTreeSet::new(),
            };
            match self.symbols.entry(uri) {
                Entry::Vacant(e) => {
                    e.insert(vec![symbol]);
                }
                Entry::Occupied(mut e) => {
                    e.get_mut().push(symbol);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Deserialize, Serialize)]
enum RunExprType {
    Query(usize),
    Expr(String),
    Position { line: u64, character: u64 },
}

#[derive(Debug, Eq, PartialEq, Clone, Deserialize, Serialize)]
struct RunExprParams {
    uri: String,
    expr: RunExprType,
}

#[derive(Debug, Clone, Serialize)]
struct RunExprResult {
    value: serde_json::Value,
    r#type: QVMType,
}

impl Backend {
    async fn on_change(&self, uri: Url, text: String, _version: Option<i32>) -> Result<()> {
        let compiler = self.compiler.clone();

        let symbols = task::spawn_blocking({
            move || {
                let record_symbol = Box::new(SymbolRecorder::new());
                let path = uri.to_file_path().map_err(log_internal_error)?;
                let file = path.to_str().unwrap().to_string();

                let compiler = compiler.lock().map_err(log_internal_error)?;
                compiler
                    .set_file_contents(file, text)
                    .map_err(log_internal_error)?;
                let orig_on_symbol = compiler
                    .on_symbol(Some(record_symbol))
                    .map_err(log_internal_error)?;
                compiler.compile_schema_from_file(path.as_path());
                let mut record_symbol = compiler
                    .on_symbol(orig_on_symbol)
                    .map_err(log_internal_error)?
                    .unwrap();
                let symbols = record_symbol
                    .as_any_mut()
                    .downcast_mut::<SymbolRecorder>()
                    .unwrap()
                    .claim_symbols();

                Ok(symbols)
            }
        })
        .await
        .map_err(log_internal_error)??;

        self.register_symbols(symbols).await?;

        Ok(())
    }

    async fn register_symbols(&self, symbols: BTreeMap<Url, Vec<Symbol>>) -> Result<()> {
        let mut references = BTreeMap::new();
        for (uri, symbols) in symbols {
            for symbol in &symbols {
                let file = symbol.def.file();
                let range = symbol.def.range();
                if file.is_none() || range.is_none() {
                    continue;
                }
                let uri = Url::from_file_path(FilePath::new(&file.unwrap()));
                if let Err(_) = uri {
                    continue;
                }
                let start = range.unwrap().start.clone();
                let ref_loc = symbol.name.loc.clone();

                match references.entry(uri.unwrap()) {
                    Entry::Vacant(e) => {
                        e.insert(BTreeMap::from([(start, vec![ref_loc])]));
                    }
                    Entry::Occupied(mut e) => match e.get_mut().entry(start) {
                        Entry::Vacant(e) => {
                            e.insert(vec![ref_loc]);
                        }
                        Entry::Occupied(mut e) => {
                            e.get_mut().push(ref_loc);
                        }
                    },
                }
            }
            if let Some(document) = self.get_document(&uri).await? {
                let mut document = document.lock().await;
                document.symbols = symbols
                    .into_iter()
                    .filter_map(|s| s.name.loc.range().map(|r| (r.start.clone(), s.clone())))
                    .collect();
            }
        }

        for (uri, references) in references {
            if let Some(document) = self.get_document(&uri).await? {
                let mut document = document.lock().await;
                for (symbol_loc, ref_locs) in references {
                    if let Some(symbol) = document.get_symbol(symbol_loc) {
                        for ref_loc in ref_locs {
                            symbol.references.insert(ref_loc);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn get_document(&self, uri: &Url) -> Result<Option<Arc<TokioMutex<Document>>>> {
        Ok(self.documents.read().await.get(uri).cloned())
    }

    async fn get_schema(&self, uri: &Url) -> Result<Option<SchemaRef>> {
        if let Some(doc) = self.get_document(uri).await? {
            let schema = doc.lock().await.schema.clone();
            Ok(schema)
        } else {
            Ok(None)
        }
    }

    async fn get_text(&self, uri: &Url) -> Result<String> {
        let doc = self
            .get_document(uri)
            .await?
            .ok_or(Error::invalid_params("Invalid document URI.".to_string()))?;

        let text = doc.lock().await.text.clone();
        Ok(text)
    }

    async fn get_symbol(&self, uri: Url, loc: Location) -> Result<Option<Symbol>> {
        let doc = self
            .get_document(&uri)
            .await?
            .ok_or(Error::invalid_params("Invalid document URI.".to_string()))?;

        let mut doc = doc.lock().await;

        Ok(doc.get_symbol(loc).map(|s| s.clone()))
    }

    async fn run_expr(&self, params: RunExprParams) -> Result<RunExprResult> {
        let uri = Url::parse(&params.uri)
            .map_err(|_| Error::invalid_params("Invalid URI".to_string()))?;
        let schema_ref = self
            .get_schema(&uri)
            .await?
            .ok_or_else(|| Error::invalid_params("Document has not been compiled".to_string()))?;

        let limit = {
            let settings = self.settings.read().map_err(log_internal_error)?;
            settings.get(&uri.to_string()).map_or_else(
                || DEFAULT_SETTINGS.max_number_of_rows,
                |c| c.max_number_of_rows,
            )
        };

        let expr = {
            // We have to "stuff" schema into this block, because it cannot be held across an await point.
            let schema = schema_ref.read().map_err(log_internal_error)?;
            match params.expr {
                RunExprType::Expr(expr) => {
                    let decl = &schema
                        .decls
                        .get(&expr)
                        .ok_or_else(|| {
                            Error::invalid_params(format!(
                                "Invalid expression (not found): {}",
                                expr
                            ))
                        })?
                        .value;
                    get_runnable_expr_from_decl(&decl)?
                }
                RunExprType::Query(idx) => schema
                    .exprs
                    .get(idx)
                    .ok_or_else(|| Error::invalid_params(format!("Invalid query index: {}", idx)))?
                    .clone()
                    .to_runtime_type()
                    .map_err(|e| {
                        eprintln!("Failed to convert query to runtime type: {:?}", e);
                        Error::internal_error()
                    })?,
                RunExprType::Position { line, character } => {
                    let loc = qvm::ast::Location {
                        line: line + 1,
                        column: character + 1,
                    };

                    match find_expr_by_location(&schema, &loc)? {
                        Some(expr) => expr,
                        None => {
                            return Err(Error::invalid_params(format!(
                                "No runnable expression at location: {:?}",
                                loc
                            )));
                        }
                    }
                }
            }
        };

        let ctx = runtime::Context::new(&schema_ref, runtime::SQLEngineType::DuckDB);

        // XXX We should change this log_internal_error to return an error to the webview
        let value = runtime::eval(&ctx, &expr)
            .await
            .map_err(log_internal_error)?;

        // This is a bit of a hack to support unsafe expressions, which return a different type than what's
        // reported from the compiler. Ideally, we should either assert or have an Unknown type.
        let r#type = value.type_();

        // NOTE: Ideally we should be applying the limit to the query tree, but we don't have a
        // great way to do that (without unwrapping and mutating the compiled expression). We probably
        // want to add a transform feature to the compiler that allows us to inject transforms at certain
        // points in the compilation process (or generate expressions that _can_ be limited/paginated).
        let mut value = json!(value);
        if limit >= 0 {
            value = match json!(value) {
                serde_json::Value::Array(arr) => {
                    let arr = arr
                        .into_iter()
                        .take(limit as usize)
                        .collect::<Vec<serde_json::Value>>();
                    serde_json::Value::Array(arr)
                }
                other => other,
            };
        }

        Ok(RunExprResult { value, r#type })
    }
}

struct SchemaRecorder {
    tokio_handle: tokio::runtime::Handle,
    client: Client,
    documents: Arc<TokioRwLock<BTreeMap<Url, Arc<TokioMutex<Document>>>>>,
}

impl compile::OnSchema for SchemaRecorder {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn on_schema(
        &mut self,
        path: &FilePath,
        text: &str,
        ast: &ast::Schema,
        schema: compile::schema::Ref<Schema>,
        errors: &Vec<(Option<usize>, CompileError)>,
    ) -> compile::error::Result<()> {
        let documents = self.documents.clone();
        let uri = Url::from_file_path(path).map_err(|_| {
            CompileError::external(format!("bad path: {}", path.display()).as_str())
        })?;
        let text = text.to_string();
        let client = self.client.clone();

        let mut diagnostics = Vec::new();

        for (idx, err) in errors {
            let loc = match err.location().normalize() {
                Some(loc) => Some(loc),
                None => {
                    if let Some(idx) = idx {
                        let stmt = &ast.stmts[*idx];
                        let loc = SourceLocation::Range(
                            uri.to_string(),
                            ast::Range {
                                start: stmt.start.clone(),
                                end: stmt.end.clone(),
                            },
                        );
                        loc.normalize()
                    } else {
                        None
                    }
                }
            };

            // If we don't have a range, just use the whole document.
            let loc = match loc {
                Some(loc) => loc,
                None => lsp_types::Location {
                    uri: uri.clone(),
                    range: FULL_DOCUMENT_RANGE,
                },
            };

            if loc.uri == uri {
                diagnostics.push(Diagnostic {
                    severity: Some(DiagnosticSeverity::ERROR),
                    range: loc.range,
                    message: err.pretty(),
                    source: Some("qvm".to_string()),
                    ..Default::default()
                });
            }
        }

        let handle_document = async move {
            let mut documents = documents.write().await;
            match documents.entry(uri.clone()) {
                Entry::Occupied(mut e) => {
                    let mut document = e.get_mut().lock().await;
                    document.schema = Some(schema);
                }
                Entry::Vacant(e) => {
                    e.insert(Arc::new(TokioMutex::new(Document {
                        uri: uri.clone(),
                        schema: Some(schema),
                        version: None,
                        text,
                        symbols: BTreeMap::new(),
                    })));
                }
            }

            client.publish_diagnostics(uri, diagnostics, None).await;

            Ok::<(), Error>(())
        };
        self.tokio_handle.spawn(async move {
            log_result(handle_document.await);
        });
        Ok(())
    }
}

const FULL_DOCUMENT_RANGE: Range = Range {
    start: Position {
        line: 0,
        character: 0,
    },
    end: Position {
        line: u32::MAX,
        character: u32::MAX,
    },
};

trait NormalizePosition<T> {
    fn normalize(&self) -> T;
}

impl NormalizePosition<Position> for qvm::ast::Location {
    fn normalize(&self) -> Position {
        assert!(self.line > 0 && self.column > 0);
        // The locations are 1-indexed, but LSP diagnostics are 0-indexed
        Position {
            line: (self.line - 1) as u32,
            character: (self.column - 1) as u32,
        }
    }
}

impl NormalizePosition<Range> for ast::Range {
    fn normalize(&self) -> Range {
        let end = self.end.normalize();
        Range {
            start: self.start.normalize(),
            end: Position {
                line: end.line,
                character: end.character + 1,
            },
        }
    }
}

impl NormalizePosition<Option<lsp_types::Location>> for SourceLocation {
    fn normalize(&self) -> Option<lsp_types::Location> {
        let file = self.file();
        let range = self.range();
        if file.is_some() && range.is_some() {
            Some(lsp_types::Location {
                uri: Url::from_file_path(FilePath::new(&file.unwrap())).unwrap(),
                range: range.unwrap().normalize(),
            })
        } else {
            None
        }
    }
}
