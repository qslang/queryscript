use tower_lsp::{LspService, Server};

mod lsp;
use lsp::Backend;

#[tokio::main]
async fn main() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let compiler = Backend::build_compiler().await;

    let (service, socket) = Backend::add_custom_methods(LspService::build(|client| {
        Backend::new(client, compiler).unwrap()
    }))
    .finish();
    Server::new(stdin, stdout, socket).serve(service).await;
}
