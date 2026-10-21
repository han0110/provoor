#!/usr/bin/env bash

set -euo pipefail

# Downloads the ere verifier static library and header into the directory the
# cgo directives of internal/ereverifier resolve against. The digest of every
# published archive is pinned here, because a release asset can be replaced
# after it is published.

ERE_VERSION=v0.19.3
REPO_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
LIB_DIR="${REPO_DIR}/internal/ereverifier/lib"

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
    linux-amd64)  DIGEST=0bce4f6131d1b7981cd20707055975677422499d6279325b9828dcd3f8a3cd4c ;;
    linux-arm64)  DIGEST=dfba5bfa4e257b73194c1025eb6e0b460820835689b94eab3fe4f7e814b00f5d ;;
    darwin-arm64) DIGEST=15863be245b745f46e91f7b9f18840bc333b3b9c5ae507b0e0a6fa138ce0afca ;;
    *)            echo "ere ${ERE_VERSION} publishes no ${os}-${arch} verifier" >&2; exit 1 ;;
esac

ARCHIVE="libere_verifier_c.${os}-${arch}.tar.gz"
URL="https://github.com/eth-act/ere/releases/download/${ERE_VERSION}/${ARCHIVE}"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

# The two platforms name their digest tool differently, so the one present
# decides how the archive is hashed.
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

# The archive lands in the destination only once its digest matches, so a
# rejected download never leaves a half-written library.
mkdir -p "${LIB_DIR}"
tar -xzf "${WORK_DIR}/${ARCHIVE}" -C "${LIB_DIR}"

for artifact in libere_verifier_c.a ere_verifier.h; do
    if [[ ! -e "${LIB_DIR}/${artifact}" ]]; then
        echo "${ARCHIVE} did not carry ${artifact}" >&2
        exit 1
    fi
done
echo "verifier library ready in ${LIB_DIR}" >&2
