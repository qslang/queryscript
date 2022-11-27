all:
	cd qvm-cli && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo build

test:
	cd qvm/src/ && CARGO_NET_GIT_FETCH_WITH_CLI=true cargo test -- --nocapture
