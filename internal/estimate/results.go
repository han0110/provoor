// Copied from https://github.com/ethpandaops/benchmarkoor/blob/3a17faa08a5c7871836113a6042fb20e3b95dc24/pkg/executor/results.go#L45-L77.

package estimate

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// maxResultPathComponent caps each path component of a test result directory
// below the common 255-byte filename limit, leaving headroom for suffixes like
// ".request". EEST benchmark node ids — especially the verbose stateful/bloatnet
// params — can exceed the limit and make MkdirAll fail with "file name too long".
const maxResultPathComponent = 200

// sanitizeResultPath caps each "/"-separated component of a test result path to
// maxResultPathComponent bytes. Over-long components are truncated and suffixed
// with a short content hash so they stay unique and stable; components within
// the limit are returned unchanged (so existing layouts don't move). Apply it
// consistently wherever a test name becomes a filesystem path (suite output,
// step results, post-test dir) so the directories match. The result.json inside
// still records the full test name, and result aggregation walks the tree, so
// truncating the directory name is safe.
func sanitizeResultPath(name string) string {
	parts := strings.Split(name, "/")

	changed := false

	for i, p := range parts {
		if len(p) > maxResultPathComponent {
			sum := sha256.Sum256([]byte(p))
			parts[i] = p[:maxResultPathComponent-17] + "-" + hex.EncodeToString(sum[:8])
			changed = true
		}
	}

	if !changed {
		return name
	}

	return strings.Join(parts, "/")
}
