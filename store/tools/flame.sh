#! /usr/bin/env bash

# flame.sh converts a `perf record` output to a flame graph:
#
#   bash flame.sh /mnt/log/server_0_perf.data
#   chrome /tmp/server.svg
#
# The script assumes you have the FlameGraph repo [1] installed in your home
# directory. If you don't, just tweak the script.
#
# [1]: https://github.com/brendangregg/FlameGraph

set -euo pipefail

main() {
    if [[ $# -lt 1 ]]; then
        echo "usage: flame.sh <perf file>"
        return 1
    fi
    perf script --header -i "$1" > /tmp/out.perf
    $HOME/FlameGraph/stackcollapse-perf.pl /tmp/out.perf > /tmp/out.folded
    $HOME/FlameGraph/flamegraph.pl /tmp/out.folded > /tmp/server.svg
}

main "$@"
