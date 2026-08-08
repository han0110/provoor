#!/usr/bin/env bash

set -euo pipefail

# Replaces the value of every variable defined in .env with the variable
# name across the runs checkout's results/, so published files never carry
# real addresses. Longer values apply first, so a value containing another,
# like a fully qualified host containing the short hostname, replaces before
# its substring. Ties keep .env order and empty values are skipped. Running
# it again finds nothing to replace.

REPO_DIR="$(cd "$(dirname "$(dirname "${BASH_SOURCE[0]}")")" && pwd)"
RUNS_DIR="$(cd "${PROVOOR_RUNS:-${REPO_DIR}/provoor-runs}" && pwd)"
ENV_FILE="${REPO_DIR}/.env"
RESULTS_DIR="${RUNS_DIR}/results"

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "error: missing ${ENV_FILE}; copy .env.example and fill it in" >&2
    exit 1
fi

entries=()
while IFS='=' read -r key value; do
    case "${key}" in
        ''|'#'*)
            continue
            ;;
    esac
    if [[ -z "${value}" ]]; then
        continue
    fi
    entries+=("${#value}"$'\t'"${key}"$'\t'"${value}")
done < "${ENV_FILE}"

if [[ ${#entries[@]} -eq 0 ]]; then
    exit 0
fi

patterns=()
program=""
while IFS=$'\t' read -r _ key value; do
    patterns+=(-e "${value}")
    program+="s|${value}|${key}|g;"
done < <(printf '%s\n' "${entries[@]}" | sort -s -t$'\t' -k1,1nr)

grep -rlZF "${patterns[@]}" "${RESULTS_DIR}" | xargs -0 -r sed -i "${program}" || true
