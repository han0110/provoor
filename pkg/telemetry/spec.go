package telemetry

import (
	_ "embed"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
)

// Image is NVIDIA's dcgm-exporter. Release 4.8.3 is the floor, because it is
// the first that names the cumulative DCGM_FI_PROF_*_TOTAL fields the window
// arithmetic subtracts. NVIDIA publishes that release for DCGM 4.6 only as a
// distroless variant.
const Image = "nvcr.io/nvidia/k8s/dcgm-exporter:4.6.0-4.8.3-distroless"

// ExporterPort is where the sidecar publishes /metrics. The DCGM host engine
// itself listens on loopback, so this port is the only way a benchmark host
// reaches a node's GPU metrics.
const ExporterPort = 9401

// HostEngine is the address the sidecar samples. Every node runs
// nvidia-dcgm.service, which binds loopback, so the sidecar shares the host
// network namespace and reaches it there.
const HostEngine = "127.0.0.1:5555"

// FieldsPath is where the sidecar reads its field list. Start copies the file
// in through the archive API, so no file touches the remote host filesystem.
const FieldsPath = "/etc/dcgm-exporter/provoor.csv"

// Fields lists the DCGM fields to publish, one per line, as a name, a
// Prometheus type and help text. The type travels to the consumer in the
// exposition, which is what lets a scraper subtract counters and average
// gauges without knowing anything about DCGM.
//
//go:embed fields.csv
var Fields []byte

// SidecarName labels one node's telemetry container. A node name comes from
// an SSH destination, which Docker does not constrain, so anything outside the
// character set Docker accepts for a container name becomes a dash.
func SidecarName(node string) string {
	var b strings.Builder
	b.WriteString("provoor-dcgm-")
	for _, r := range node {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '_', r == '.', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return b.String()
}

// Spec builds the telemetry sidecar.
//
// The container needs no GPU of its own and no capabilities. It never touches
// a device, because the node's own DCGM service already holds the profiling
// watches and the sidecar only reads them. Host networking is what reaches
// that service, since it binds loopback.
func Spec(interval time.Duration) (*container.Config, *container.HostConfig) {
	containerCfg := &container.Config{
		Image: Image,
		User:  "65534:65534",
		Cmd: []string{
			"-f", FieldsPath,
			"-r", HostEngine,
			"-a", ":" + strconv.Itoa(ExporterPort),
			"-c", strconv.Itoa(int(interval.Milliseconds())),
			// The startup check runs ldconfig, which fails in the distroless
			// image because it carries no loader cache. Skipping it is what
			// lets the sidecar run with no GPU device attached.
			"--disable-startup-validate",
			// The default output labels every series with the real hostname.
			// Node identity comes from the scraper, which knows the host it
			// dialled, so no hostname reaches published results.
			"--no-hostname",
		},
	}
	// The root filesystem stays writable, because Docker refuses to extract
	// the field list into a container marked read-only.
	hostCfg := &container.HostConfig{
		NetworkMode:   "host",
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
		CapDrop:       []string{"ALL"},
		SecurityOpt:   []string{"no-new-privileges"},
	}
	return containerCfg, hostCfg
}
