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
