SRC_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PATH="${SRC_ROOT}/target/debug/:$PATH"
export PS1="(qvm) $PS1"
