#!/usr/bin/env bash

set -euo pipefail

# Replaces the value of every variable defined in .env with the variable
# name across the runs checkout's results/, so published files never carry
# real addresses. Longer values apply first, so a value containing another,
# like a fully qualified host containing the short hostname, replaces before
# its substring. Ties keep .env order and empty values are skipped. Running
# it again finds nothing to replace.

# shellcheck source=scripts/config.sh
. "$(dirname "${BASH_SOURCE[0]}")/config.sh"

RUNS_DIR="${REPO_DIR}/provoor-runs"
RESULTS_DIR="${RUNS_DIR}/results"

if ! command -v python3 > /dev/null; then
    echo "error: python3 is required" >&2
    exit 1
fi

read_env_file

entries=()
for (( index = 0; index < ${#env_names[@]}; index++ )); do
    value="${env_values[index]}"
    if [[ -z "${value}" ]]; then
        continue
    fi
    entries+=("${#value}"$'\t'"${env_names[index]}"$'\t'"${value}")
done

if [[ ${#entries[@]} -eq 0 ]]; then
    exit 0
fi

patterns=()
substitutions=()
while IFS=$'\t' read -r _ key value; do
    patterns+=(-e "${value}")
    substitutions+=("${value}" "${key}")
done < <(printf '%s\n' "${entries[@]}" | sort -s -t$'\t' -k1,1nr)

matches="$(mktemp)"
trap 'rm -f "${matches}"' EXIT

# grep exits 1 when nothing matches, which is the already-clean case, and
# above 1 on a real failure that must not be mistaken for it. The file list
# stays null separated, so it is passed through a file rather than a
# variable.
scan() {
    local status=0
    grep -rlZF "${patterns[@]}" "${RESULTS_DIR}" > "${matches}" || status=$?
    if (( status > 1 )); then
        echo "error: scanning ${RESULTS_DIR} failed" >&2
        exit 1
    fi
}

scan

# python3 rather than sed, because sed reads a value as a regular expression
# that over-matches unrelated bytes and chains its rules into names an earlier
# rule inserted. One left to right pass over exact bytes does neither.
python3 - "${matches}" "${substitutions[@]}" <<'PYTHON'
import os
import sys

with open(sys.argv[1], "rb") as handle:
    paths = [path for path in handle.read().split(b"\0") if path]
pairs = [
    (os.fsencode(value), os.fsencode(name))
    for value, name in zip(sys.argv[2::2], sys.argv[3::2])
]


def substitute(text):
    """Replaces each value with its name, taking the earliest occurrence and
    the earliest pair on a tie, and resuming past every replacement."""
    positions = [text.find(value) for value, _ in pairs]
    parts = []
    cursor = 0
    while True:
        chosen = -1
        for index, position in enumerate(positions):
            if position >= 0 and (chosen < 0 or position < positions[chosen]):
                chosen = index
        if chosen < 0:
            break
        value, name = pairs[chosen]
        parts.append(text[cursor:positions[chosen]])
        parts.append(name)
        cursor = positions[chosen] + len(value)
        for index, (candidate, _) in enumerate(pairs):
            if 0 <= positions[index] < cursor:
                positions[index] = text.find(candidate, cursor)
    parts.append(text[cursor:])
    return b"".join(parts)


for path in paths:
    with open(path, "rb") as handle:
        text = handle.read()
    replaced = substitute(text)
    if replaced != text:
        with open(path, "wb") as handle:
            handle.write(replaced)
PYTHON

# A rewrite that did not take must stop the publish rather than pass as done.
scan
if [[ -s "${matches}" ]]; then
    echo "error: ${RESULTS_DIR} still holds values from ${ENV_FILE}" >&2
    exit 1
fi
