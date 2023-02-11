ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

.PHONY: all
all: ${VENV_PRE_COMMIT} submodules cli lsp
	@true

.PHONY: cli
cli: ${VENV_PRE_COMMIT} submodules
	cd queryscript && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build --features "cli"

.PHONY: submodules
submodules: sqlparser-rs/Cargo.toml

sqlparser-rs/Cargo.toml:
	git submodule update --init --recursive

.PHONY: lsp
lsp: yarn-deps
	cd extension && yarn esbuild

.PHONY: lsp-rust
lsp-rust: ${VENV_PRE_COMMIT} submodules
	cd queryscript && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build --features "lsp"

.PHONY: yarn-deps
yarn-deps: ts-bindings
	cd extension && yarn install

.PHONY: ts-bindings
ts-bindings:
	cd queryscript/src && cargo test --features ts export_bindings


.PHONY: test lfs refresh-test-data
test: lfs submodules
	cd queryscript/src/ && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo test -- --nocapture

lfs:
	git lfs install && git lfs fetch

refresh-test-data: ${VENV_PYTHON_PACKAGES}
	source venv/bin/activate && nba-scraper ${ROOT_DIR}/queryscript/tests/nba/data

VENV_INITIALIZED := venv/.initialized

${VENV_INITIALIZED}:
	rm -rf venv && python3 -m venv venv
	@touch ${VENV_INITIALIZED}

VENV_PYTHON_PACKAGES := venv/.python_packages

${VENV_PYTHON_PACKAGES}: ${VENV_INITIALIZED} qsutils/setup.py
	bash -c 'source venv/bin/activate && python -m pip install --upgrade pip setuptools'
	bash -c 'source venv/bin/activate && python -m pip install -e ./qsutils[dev]'
	@touch $@

VENV_PRE_COMMIT := venv/.pre_commit

${VENV_PRE_COMMIT}: ${VENV_PYTHON_PACKAGES}
	bash -c 'source venv/bin/activate && pre-commit install'
	@touch $@

develop: ${VENV_PRE_COMMIT} lsp qs lfs
	@echo "--\nRun "source env.sh" to enter development mode!"

fixup:
	pre-commit run --all-files
