#!/usr/bin/env bash
# Downloads the ZisK proving key into the mounted volume and builds its
# const-trees. KEY_URL names the proving-key archive and PROVING_KEY_DIR the
# in-container volume path, both supplied as container environment.
set -euo pipefail
curl -fSL "${KEY_URL}" -o /tmp/pk.tar.gz
tar --no-same-owner --overwrite -xf /tmp/pk.tar.gz -C /root/.zisk
rm -f /tmp/pk.tar.gz
cargo-zisk-dev-gpu check-setup --proving-key "${PROVING_KEY_DIR}" -g
