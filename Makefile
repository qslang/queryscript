ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
VENV_PRE_COMMIT := ${ROOT_DIR}/venv/.pre_commit


.PHONY: all
all: ${VENV_PRE_COMMIT} extension qs submodules

.PHONY: qs
qs: submodules
	cargo build ${CARGO_FLAGS} --features 'cli lsp'

.PHONY: submodules
submodules: sqlparser-rs/Cargo.toml

sqlparser-rs/Cargo.toml:
	git submodule update --init --recursive

.PHONY: extension yarn-deps ts-bindings

extension: qs yarn-deps
	cd extension && yarn esbuild

yarn-deps: ts-bindings
	cd extension && yarn install

.PHONY: ts-bindings
ts-bindings:
	cd queryscript/src && cargo test ${CARGO_FLAGS} --features ts export_bindings


.PHONY: test lfs refresh-test-data
test: lfs submodules
	cd queryscript/src/ && cargo test ${CARGO_FLAGS} -- --nocapture

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

${VENV_PRE_COMMIT}: ${VENV_PYTHON_PACKAGES}
	bash -c 'source venv/bin/activate && pre-commit install'
	@touch $@

develop: ${VENV_PRE_COMMIT} extension qs lfs
	@echo "--\nRun "source env.sh" to enter development mode!"

fixup:
	pre-commit run --all-files
