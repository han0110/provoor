#!/usr/bin/env bash

set -euo pipefail

# Proxy for the benchmarkoor CLI that runs the submodule build in the
# provoor-runs checkout. Every --config resolves to an absolute path first, so
# a relative results_dir lands in that checkout.

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
