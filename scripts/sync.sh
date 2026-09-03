#!/usr/bin/env bash

set -euo pipefail

# Pulls a pruned copy of a remote benchmarkoor results tree into the runs
# checkout's results/, then runs scripts/desensitize.sh over it. Runner logs
# and JSON-RPC request payloads are excluded at transfer time.

# shellcheck source=scripts/config.sh
. "$(dirname -- "${BASH_SOURCE[0]}")/config.sh"

RUNS_DIR="${REPO_DIR}/provoor-runs"
RESULTS_DIR="${RUNS_DIR}/results"

SSH_DESTINATION=""
REMOTE_RESULTS_DIR=""

usage() {
    echo "Usage: $0 [--ssh USER@HOST] [--remote-results-dir DIR]"
    echo ""
    echo "Options:"
    echo "  --ssh USER@HOST           SSH destination holding the results, COORDINATOR_SSH in .env by default"
    echo "  --remote-results-dir DIR  Results directory on the remote, relative to the SSH home,"
    echo "                            REMOTE_RESULTS_DIR in .env by default"
    echo "  --help, -h                Show this help"
    exit 1
}

# Runs in the main shell so a flag without its value stops the script rather
# than tripping over an unbound parameter.
require_value() {
    if (( $1 < 2 )); then
        echo "error: $2 needs a value" >&2
        exit 1
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ssh)
            require_value $# "$1"
            SSH_DESTINATION="$2"
            shift 2
            ;;
        --remote-results-dir)
            require_value $# "$1"
            REMOTE_RESULTS_DIR="$2"
            shift 2
            ;;
        --help|-h)
            usage
            ;;
        *)
            echo "error: unknown option '$1'" >&2
            usage
            ;;
    esac
done

# .env is read after the arguments, so --help answers without one.
read_env_file
if [[ -z "${SSH_DESTINATION}" ]]; then
    env_lookup COORDINATOR_SSH
    SSH_DESTINATION="${env_value}"
fi
if [[ -z "${REMOTE_RESULTS_DIR}" ]]; then
    env_lookup REMOTE_RESULTS_DIR
    REMOTE_RESULTS_DIR="${env_value}"
fi

# rsync copies a directory's contents only when the source ends in exactly one
# slash, so trailing slashes come off before one goes back on.
while [[ "${REMOTE_RESULTS_DIR}" == */ ]]; do
    REMOTE_RESULTS_DIR="${REMOTE_RESULTS_DIR%/}"
done

if [[ -z "${SSH_DESTINATION}" ]]; then
    echo "error: pass --ssh or set COORDINATOR_SSH in ${ENV_FILE}" >&2
    exit 1
fi
if [[ -z "${REMOTE_RESULTS_DIR}" ]]; then
    echo "error: pass --remote-results-dir or set REMOTE_RESULTS_DIR in ${ENV_FILE}" >&2
    exit 1
fi

mkdir -p "${RESULTS_DIR}"
rsync -a \
    --exclude container.log \
    --exclude benchmarkoor.log \
    --exclude '*.request' \
    "${SSH_DESTINATION}:${REMOTE_RESULTS_DIR}/" "${RESULTS_DIR}/"

"${REPO_DIR}/scripts/desensitize.sh"
