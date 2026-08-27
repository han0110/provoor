#!/usr/bin/env bash
# Builds one guest program's ROM merkle setup and assembly emulator into the
# shared ZisK cache, then checks the verifying key the setup derived against
# the configured one. ELF_PATH names the guest ELF, VK_PATH the expected key,
# PROVING_KEY_DIR the proving key the setup reads, and CACHE_DIR the cache the
# artifacts land in, all supplied as container environment.
set -euo pipefail

# The verifying key is written to a file whose name encodes the ELF hash and
# the proving key's hash-mode parameters, none of which the caller derives. The
# setup rewrites it on every run, so the file newer than this marker is the one
# this run produced. More than one match means the cache holds artifacts of
# another concurrent setup, which no longer identifies the guest.
marker=/tmp/zisk-program-setup-marker
touch "${marker}"

cargo-zisk-dev-gpu program-setup --elf "${ELF_PATH}" --proving-key "${PROVING_KEY_DIR}" -g

derived=$(find "${CACHE_DIR}" -maxdepth 1 -name '*.verkey.bin' -newer "${marker}")
if [ "$(printf '%s' "${derived}" | grep -c .)" -ne 1 ]; then
    echo "program setup produced $(printf '%s' "${derived}" | grep -c .) verifying keys, expected exactly one" >&2
    exit 1
fi

if ! cmp -s "${derived}" "${VK_PATH}"; then
    echo "guest verifying key mismatch, the cluster derives $(od -An -tx1 -v "${derived}" | tr -d ' \n')" >&2
    echo "and the configured vk is $(od -An -tx1 -v "${VK_PATH}" | tr -d ' \n')" >&2
    exit 1
fi
