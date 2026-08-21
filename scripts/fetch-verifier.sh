#!/usr/bin/env bash

set -euo pipefail

# Downloads the ere verifier static library and header into the directory the
# cgo directives of pkg/ereverifier resolve against. The archive is a release
# asset of the same ere version the binding is vendored from, so the library
# and the binding always move together. Its contents stay out of version
# control, which is why a fresh checkout runs this before building.
#
# The digest of every published archive is pinned here. This library decides
# whether a proof is accepted, and a release asset can be replaced after it is
# published, so the bytes are checked rather than trusted. Moving ERE_VERSION
# means refreshing these alongside the vendored binding.

ERE_VERSION=v0.16.2
REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
LIB_DIR="${REPO_DIR}/pkg/ereverifier/lib"

case "$(uname -s)" in
    Linux)  os=linux ;;
    Darwin) os=darwin ;;
    *)      echo "unsupported operating system $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
    x86_64)         arch=amd64 ;;
    aarch64|arm64)  arch=arm64 ;;
    *)              echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

case "${os}-${arch}" in
    linux-amd64)  DIGEST=242d99943cd1b6071664c523861c6d6759a7a4944378fddd08a986726b3a0c11 ;;
    linux-arm64)  DIGEST=11b12a874d9d0d3a48ab9fb6d38ff6ce26decac615834504c5dd42cf8574c5d5 ;;
    darwin-arm64) DIGEST=f6997c96c62225ae099d9464887647f432482c7cb71d5edb40ad4fc58a254e00 ;;
    *)            echo "ere ${ERE_VERSION} publishes no ${os}-${arch} verifier" >&2; exit 1 ;;
esac

ARCHIVE="libere_verifier_c.${os}-${arch}.tar.gz"
URL="https://github.com/eth-act/ere/releases/download/${ERE_VERSION}/${ARCHIVE}"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

# The two platforms name their digest tool differently, and neither ships the
# other, so the one that is present decides how the archive is hashed.
if command -v sha256sum >/dev/null 2>&1; then
    digest_of() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
    digest_of() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
    echo "neither sha256sum nor shasum is available to check the archive" >&2
    exit 1
fi

echo "fetching ${ARCHIVE} from ere ${ERE_VERSION}" >&2
curl -fsSL "${URL}" -o "${WORK_DIR}/${ARCHIVE}"
FOUND="$(digest_of "${WORK_DIR}/${ARCHIVE}")"
if [[ "${FOUND}" != "${DIGEST}" ]]; then
    echo "${ARCHIVE} hashes to ${FOUND}, expected the pinned ${DIGEST}" >&2
    exit 1
fi

# The archive lands in the destination only once its bytes are vouched for, so
# an interrupted or rejected download never leaves a half-written library that
# a later build would link.
mkdir -p "${LIB_DIR}"
tar -xzf "${WORK_DIR}/${ARCHIVE}" -C "${LIB_DIR}"

for artifact in libere_verifier_c.a ere_verifier.h; do
    if [[ ! -e "${LIB_DIR}/${artifact}" ]]; then
        echo "${ARCHIVE} did not carry ${artifact}" >&2
        exit 1
    fi
done
echo "verifier library ready in ${LIB_DIR}" >&2
