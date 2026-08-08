#!/usr/bin/env bash

set -euo pipefail

# Pulls a pruned copy of a remote benchmarkoor results tree into the runs
# checkout's results/. Raw runner logs and JSON-RPC request payloads are
# excluded at transfer time, so the repository only ever holds the
# publishable subset. The synced files then pass through
# scripts/desensitize.sh, which replaces real addresses with their .env
# variable names.

REPO_DIR="$(cd "$(dirname "$(dirname "${BASH_SOURCE[0]}")")" && pwd)"
RUNS_DIR="$(cd "${PROVOOR_RUNS:-${REPO_DIR}/provoor-runs}" && pwd)"
RESULTS_DIR="${RUNS_DIR}/results"

HOST=""
REMOTE_PATH=""

usage() {
    echo "Usage: $0 --host USER@HOST --path REMOTE_RESULTS_DIR"
    echo ""
    echo "Options:"
    echo "  --host USER@HOST               SSH destination holding the results"
    echo "  --path REMOTE_RESULTS_DIR      Results directory on the remote, relative to the SSH home"
    echo "  --help, -h                     Show this help"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)
            HOST="$2"
            shift 2
            ;;
        --path)
            REMOTE_PATH="$2"
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

if [[ -z "${HOST}" || -z "${REMOTE_PATH}" ]]; then
    echo "error: --host and --path are required" >&2
    usage
fi

mkdir -p "${RESULTS_DIR}"
rsync -a \
    --exclude container.log \
    --exclude benchmarkoor.log \
    --exclude '*.request' \
    "${HOST}:${REMOTE_PATH}/" "${RESULTS_DIR}/"

"${REPO_DIR}/scripts/desensitize.sh"
