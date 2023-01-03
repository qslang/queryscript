all:
	cd qvm-cli && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build

lsp:
	cd qvm-lsp && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build

test:
	cd qvm/src/ && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo test -- --nocapture

deps:
	cd qvm-lsp && yarn install
	cd qvm-lsp/client && yarn install
	cd qvm-lsp/webview && yarn install

VENV_INITIALIZED := venv/.initialized

${VENV_INITIALIZED}:
	rm -rf venv && python3 -m venv venv
	@touch ${VENV_INITIALIZED}

VENV_PYTHON_PACKAGES := venv/.python_packages

${VENV_PYTHON_PACKAGES}: ${VENV_INITIALIZED} py/setup.py
	bash -c 'source venv/bin/activate && python -m pip install --upgrade pip setuptools'
	bash -c 'source venv/bin/activate && python -m pip install -e ./py[dev]'
	@touch $@

VENV_PRE_COMMIT := venv/.pre_commit

${VENV_PRE_COMMIT}: ${VENV_PYTHON_PACKAGES}
	bash -c 'source venv/bin/activate && pre-commit install'
	@touch $@

.PHONY: develop fixup test
develop: ${VENV_PRE_COMMIT}
	@echo 'Run "source venv/bin/activate" to enter development mode'

fixup:
	pre-commit run --all-files

test:
	python -m pytest -s -v ./tests/
