#!/usr/bin/env bash
# Builds one guest program's ROM merkle setup and assembly emulator into the
# shared ZisK cache, then checks the verifying key the setup derived against
# the configured one. ELF_PATH names the guest ELF, VK_PATH the expected key,
# PROVING_KEY_DIR the proving key the setup reads, and CACHE_DIR the cache the
# artifacts land in, all supplied as container environment.
set -euo pipefail

# The setup rewrites the verifying key file on every run under a name the
# caller cannot derive, so the one newer than this marker is this run's. More
# than one match means a concurrent setup shares the cache and no longer
# identifies the guest.
marker=/tmp/zisk-program-setup-marker
touch "${marker}"

cargo-zisk-dev-gpu program-setup --elf "${ELF_PATH}" --proving-key "${PROVING_KEY_DIR}" -g

derived=$(find "${CACHE_DIR}" -maxdepth 1 -name '*.verkey.bin' -newer "${marker}")
count=$(printf '%s' "${derived}" | grep -c .)
if [ "${count}" -ne 1 ]; then
    echo "program setup produced ${count} verifying keys, expected exactly one" >&2
    exit 1
fi

if ! cmp -s "${derived}" "${VK_PATH}"; then
    echo "guest verifying key mismatch, the cluster derives $(od -An -tx1 -v "${derived}" | tr -d ' \n') and the configured vk is $(od -An -tx1 -v "${VK_PATH}" | tr -d ' \n')" >&2
    exit 1
fi
