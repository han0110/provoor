#!/usr/bin/env bash

set -euo pipefail

# Proxy for the benchmarkoor CLI, built from the submodule rather than taken
# from PATH. The run happens in the runs checkout so a config's relative
# results_dir lands there, which is why --config values resolve to absolute
# paths first and every other relative path argument resolves against the
# runs checkout rather than the caller's directory.

# shellcheck source=scripts/config.sh
. "$(dirname "${BASH_SOURCE[0]}")/config.sh"

# Every submodule is reset to the revision the repository records, so neither
# a stale build nor a stale runs checkout survives a pull that forgot
# --recurse-submodules. It runs before the arguments are rewritten, since the
# run configurations a --config value names live in the benchmarkoor
# submodule.
git -C "${REPO_DIR}" submodule update --init --recursive --force

rewrite_config_args "$@"

BENCHMARKOOR_DIR="${REPO_DIR}/benchmarkoor"

# The build runs on every call, so the binary and the version it stamps into
# every result stay in step with the submodule.
echo "building benchmarkoor" >&2
make -C "${BENCHMARKOOR_DIR}" --no-print-directory -s build-core

cd "${REPO_DIR}/provoor-runs"
exec "${BENCHMARKOOR_DIR}/bin/benchmarkoor" "${args[@]+"${args[@]}"}"
