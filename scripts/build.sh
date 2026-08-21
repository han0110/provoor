#!/usr/bin/env bash

set -euo pipefail

# Builds a binary this repository runs, so a caller needs to know neither where
# a target's sources live nor which build tool it uses.

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

# cgo links the ere verifier, which scripts/fetch-verifier.sh leaves in
# pkg/ereverifier/lib.
build_provoor() {
    echo "building provoor" >&2
    cd "${REPO_DIR}"
    go build -o provoor ./cmd/provoor
}

# Every submodule is reset to the revision the repository records, so neither a
# stale build nor a stale runs checkout survives a pull that forgot
# --recurse-submodules.
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
