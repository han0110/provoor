#!/usr/bin/env bash

set -euo pipefail

# Proxy for the benchmarkoor CLI, run from the submodule rather than taken from
# PATH, so the version stamped into every result is the one the repository
# records. scripts/build.sh benchmarkoor produces it. The run happens in the
# runs checkout so a config's relative results_dir lands there, which is why
# --config values resolve to absolute paths first and every other relative path
# argument resolves against the runs checkout rather than the caller's
# directory.

# shellcheck source=scripts/config.sh
. "$(dirname "${BASH_SOURCE[0]}")/config.sh"

rewrite_config_args "$@"

BENCHMARKOOR="${REPO_DIR}/benchmarkoor/bin/benchmarkoor"
if [[ ! -x "${BENCHMARKOOR}" ]]; then
    echo "error: ${BENCHMARKOOR} is missing, run scripts/build.sh benchmarkoor" >&2
    exit 1
fi

cd "${REPO_DIR}/provoor-runs"
exec "${BENCHMARKOOR}" "${args[@]+"${args[@]}"}"
