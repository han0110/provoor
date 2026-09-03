#!/usr/bin/env bash
# Derives the proving keyset of one guest program into the mounted artifacts
# volume, with GUEST_ELF, PROGRAM_NAME, PROGRAM_VERSION, and ARTIFACTS_DIR
# supplied as container environment. The shared proving keys are written once
# and baseline.bin moves last, so its presence marks a completed derivation.
set -euo pipefail

# The volume is a different filesystem from /tmp, so a direct move is a copy
# an interrupt can truncate. Landing beside the target first keeps the
# publishing rename atomic.
install_artifact() {
    mv "$1" "$2.partial"
    mv "$2.partial" "$2"
}

mkdir -p /tmp/keygen "${ARTIFACTS_DIR}/programs/${PROGRAM_NAME}/${PROGRAM_VERSION}"
./convert_fixtures keygen --elf "${GUEST_ELF}" --output-dir /tmp/keygen
install_artifact /tmp/keygen/program.vmexe "${ARTIFACTS_DIR}/programs/${PROGRAM_NAME}/${PROGRAM_VERSION}/program.vmexe"
for pk in app_pk agg_stark_pk; do
    [ -e "${ARTIFACTS_DIR}/${pk}" ] || install_artifact "/tmp/keygen/${pk}" "${ARTIFACTS_DIR}/${pk}"
done
install_artifact /tmp/keygen/baseline.bin "${ARTIFACTS_DIR}/programs/${PROGRAM_NAME}/${PROGRAM_VERSION}/baseline.bin"
