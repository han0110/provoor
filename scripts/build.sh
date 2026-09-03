#!/usr/bin/env bash

set -euo pipefail

# Builds a binary this repository runs, so a caller needs neither the source
# location nor the build tool of a target.

REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
BENCHMARKOOR_DIR="${REPO_DIR}/benchmarkoor"

usage() {
    echo "Usage: $0 TARGET"
    echo ""
    echo "Targets:"
    echo "  provoor       Build the CLI into the repository root"
    echo "  benchmarkoor  Reset the submodules and build benchmarkoor from the revision they record"
    exit 1
}

# cgo links the ere verifier that scripts/fetch-verifier.sh leaves in
# internal/ereverifier/lib.
build_provoor() {
    echo "building provoor" >&2
    cd "${REPO_DIR}"
    go build -o provoor ./cmd/provoor
}

# Every submodule is reset to the recorded revision, so a pull without
# --recurse-submodules leaves no stale build or runs checkout.
build_benchmarkoor() {
    git -C "${REPO_DIR}" submodule update --init --recursive --force
    echo "building benchmarkoor" >&2
    make -C "${BENCHMARKOOR_DIR}" --no-print-directory -s build-core
}

(( $# == 1 )) || usage
case "$1" in
    provoor)
        build_provoor
        ;;
    benchmarkoor)
        build_benchmarkoor
        ;;
    *)
        usage
        ;;
esac
