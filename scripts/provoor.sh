#!/usr/bin/env bash

set -euo pipefail

# Proxy for the provoor CLI, preferring a provoor built in the repository
# root over one on PATH.

# shellcheck source=scripts/config.sh
. "$(dirname "${BASH_SOURCE[0]}")/config.sh"

rewrite_config_args "$@"

PROVOOR="${REPO_DIR}/provoor"
if [[ ! -x "${PROVOOR}" ]]; then
    PROVOOR=provoor
fi

exec "${PROVOOR}" "${args[@]+"${args[@]}"}"
