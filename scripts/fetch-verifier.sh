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

ERE_VERSION=v0.15.0
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
    linux-amd64)  DIGEST=f441c47ae627385280efc94c1478ae489815c3d0fc79de1f4025e2ea49d005bb ;;
    linux-arm64)  DIGEST=12dfe76f6a0633375849a3e5b8218befd9246a584f168c71265bd353fdcb4051 ;;
    darwin-amd64) DIGEST=8f519eeac49d221978387bd7c7db7c22e6f0786feda86c961230655b2b8bd148 ;;
    darwin-arm64) DIGEST=65a3de5bb654127bae577e6899abf8671b79241b9f44a94c8c314e438982779c ;;
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
