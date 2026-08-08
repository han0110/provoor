#!/usr/bin/env bash

set -euo pipefail

# Proxy for the benchmarkoor CLI. The run happens in the runs checkout so a
# config's relative results_dir lands there, which means --config values
# resolve to absolute paths first and every other relative path argument
# resolves against the runs checkout rather than the caller's directory. A
# --config naming a *.example.yaml template is materialized next to the
# template with the placeholders substituted from .env and swapped for the
# generated file.

REPO_DIR="$(cd "$(dirname "$(dirname "${BASH_SOURCE[0]}")")" && pwd)"
RUNS_DIR="${REPO_DIR}/provoor-runs"
ENV_FILE="${REPO_DIR}/.env"

# Runs in the main shell so a missing config stops the script rather than
# reaching benchmarkoor as a silently rewritten path.
require_config() {
    if [[ ! -f "$1" ]]; then
        echo "error: no such config: $1" >&2
        exit 1
    fi
}

absolute() {
    printf '%s/%s' "$(cd "$(dirname "$1")" && pwd)" "$(basename "$1")"
}

materialize() {
    local template="$1"
    local generated="${template%.example.yaml}.yaml"
    if [[ ! -f "${ENV_FILE}" ]]; then
        echo "error: missing ${ENV_FILE}; copy .env.example and fill it in" >&2
        exit 1
    fi
    set -a
    # shellcheck disable=SC1090
    . "${ENV_FILE}"
    set +a
    : "${CLUSTER_IP:?CLUSTER_IP missing in .env}"
    sed "s|\${CLUSTER_IP}|${CLUSTER_IP}|g" "${template}" > "${generated}"
    absolute "${generated}"
}

args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --config)
            args+=("$1")
            shift
            require_config "$1"
            case "$1" in
                *.example.yaml)
                    args+=("$(materialize "$1")")
                    ;;
                *)
                    args+=("$(absolute "$1")")
                    ;;
            esac
            ;;
        --config=*)
            value="${1#--config=}"
            require_config "${value}"
            case "${value}" in
                *.example.yaml)
                    args+=("--config=$(materialize "${value}")")
                    ;;
                *)
                    args+=("--config=$(absolute "${value}")")
                    ;;
            esac
            ;;
        *)
            args+=("$1")
            ;;
    esac
    shift
done

# benchmarkoor resolves a config's results_dir against the working
# directory, so the run happens in the runs checkout.
cd "${RUNS_DIR}"
exec benchmarkoor "${args[@]+"${args[@]}"}"
