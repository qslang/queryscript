SRC_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ps1_old="$PS1"
source $SRC_ROOT/venv/bin/activate
export PATH="${SRC_ROOT}/target/debug/:$PATH"
export PS1="(qs) $ps1_old"
