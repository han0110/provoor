package telemetry

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/han0110/provoor/pkg/cluster"
)

func TestSpecNeedsNoDeviceAndNoCapability(t *testing.T) {
	_, hostCfg := Spec(100 * time.Millisecond)
	if len(hostCfg.Resources.DeviceRequests) != 0 {
		t.Errorf("got %d device requests, want none - the sidecar reads the node's own dcgm service",
			len(hostCfg.Resources.DeviceRequests))
	}
	if len(hostCfg.CapAdd) != 0 {
		t.Errorf("got capabilities %v, want none", hostCfg.CapAdd)
	}
	if len(hostCfg.CapDrop) != 1 || hostCfg.CapDrop[0] != "ALL" {
		t.Errorf("got cap drop %v, want ALL", hostCfg.CapDrop)
	}
}

func TestSpecUsesHostNetworkingToReachTheLoopbackEngine(t *testing.T) {
	_, hostCfg := Spec(100 * time.Millisecond)
	if hostCfg.NetworkMode != "host" {
		t.Errorf("network mode is %q, want host so 127.0.0.1:5555 resolves to the node", hostCfg.NetworkMode)
	}
	if len(hostCfg.PortBindings) != 0 {
		t.Errorf("got %d port bindings, want none under host networking", len(hostCfg.PortBindings))
	}
}

// TestSpecSkipsTheStartupCheck covers the flag that lets the sidecar run with
// no GPU attached. The check shells out to ldconfig, which fails in the
// distroless image because it carries no loader cache.
func TestSpecSkipsTheStartupCheck(t *testing.T) {
	containerCfg, _ := Spec(100 * time.Millisecond)
	if !strings.Contains(strings.Join(containerCfg.Cmd, " "), "--disable-startup-validate") {
		t.Errorf("command %v does not skip the startup check", containerCfg.Cmd)
	}
}

// TestSpecPublishesNoHostname guards the publish path. The default output
// labels every series with the real hostname, which the results scanner
// rejects.
func TestSpecPublishesNoHostname(t *testing.T) {
	containerCfg, _ := Spec(100 * time.Millisecond)
	if !strings.Contains(strings.Join(containerCfg.Cmd, " "), "--no-hostname") {
		t.Errorf("command %v does not suppress the hostname label", containerCfg.Cmd)
	}
}

func TestSpecCarriesTheEngineThePortAndTheInterval(t *testing.T) {
	containerCfg, _ := Spec(250 * time.Millisecond)
	cmd := strings.Join(containerCfg.Cmd, " ")
	for _, want := range []string{
		"-f " + FieldsPath,
		"-r " + HostEngine,
		"-a :" + strconv.Itoa(ExporterPort),
		"-c 250",
	} {
		if !strings.Contains(cmd, want) {
			t.Errorf("command %q is missing %q", cmd, want)
		}
	}
}

// TestFieldsAreWellFormed guards the file the exporter reads. A malformed
// line is rejected at startup, which a deployment only discovers on the rig.
// The type column is load-bearing, because it reaches the consumer in the
// exposition and decides whether a series is subtracted or averaged.
func TestFieldsAreWellFormed(t *testing.T) {
	lines := strings.Split(strings.TrimSpace(string(Fields)), "\n")
	if len(lines) < 14 {
		t.Fatalf("got %d fields, want the full set", len(lines))
	}
	seen := map[string]bool{}
	for _, line := range lines {
		columns := strings.Split(line, ",")
		if len(columns) != 3 {
			t.Errorf("line %q has %d columns, want a name, a type and help text", line, len(columns))
			continue
		}
		name := strings.TrimSpace(columns[0])
		if !strings.HasPrefix(name, "DCGM_FI_") {
			t.Errorf("field %q is not a DCGM field name", name)
		}
		if seen[name] {
			t.Errorf("field %q repeats", name)
		}
		seen[name] = true
		switch kind := strings.TrimSpace(columns[1]); kind {
		case "counter", "gauge":
		default:
			t.Errorf("field %s declares type %q, want counter or gauge", name, kind)
		}
		if strings.TrimSpace(columns[2]) == "" {
			t.Errorf("field %s carries no help text", name)
		}
	}
}

// TestFieldsCarryTheCumulativeCounters guards the arithmetic the consumer
// depends on. The window totals come from subtracting these, so a rate field
// substituted for a total would read as a plausible wrong answer.
func TestFieldsCarryTheCumulativeCounters(t *testing.T) {
	for _, want := range []string{
		"DCGM_FI_PROF_SM_CYCLES_ELAPSED_TOTAL",
		"DCGM_FI_PROF_SM_CYCLES_ACTIVE_TOTAL",
		"DCGM_FI_PROF_INT_CYCLES_ACTIVE_TOTAL",
		"DCGM_FI_PROF_PCIE_RX_BYTES_TOTAL",
		"DCGM_FI_PROF_PCIE_TX_BYTES_TOTAL",
	} {
		if !strings.Contains(string(Fields), want+",") {
			t.Errorf("field list is missing %s", want)
		}
	}
}

// TestFieldsMatchTheCollector keeps the sidecar and the collector in step. A
// field served but never recorded is scraped for nothing, and a field
// recorded but never served leaves an empty column in every artifact.
func TestFieldsMatchTheCollector(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "benchmarkoor", "pkg", "remotemetrics", "exporter.go"))
	if err != nil {
		t.Fatal(err)
	}
	recorded := map[string]bool{}
	for _, match := range regexp.MustCompile(`"(DCGM_FI_[A-Z0-9_]+)":\s*\{`).FindAllStringSubmatch(string(source), -1) {
		recorded[match[1]] = true
	}
	served := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(Fields)), "\n") {
		served[strings.TrimSpace(strings.Split(line, ",")[0])] = true
	}
	for name := range served {
		if !recorded[name] {
			t.Errorf("field %s is served but never recorded", name)
		}
	}
	for name := range recorded {
		if !served[name] {
			t.Errorf("field %s is recorded but never served", name)
		}
	}
}

func TestSidecarNamesDiffer(t *testing.T) {
	if SidecarName(cluster.SidecarDCGM, "node0") == SidecarName(cluster.SidecarDCGM, "node1") {
		t.Error("two nodes share one sidecar name")
	}
	if SidecarName(cluster.SidecarDCGM, "node0") == SidecarName(cluster.SidecarNode, "node0") {
		t.Error("two kinds share one sidecar name on a node")
	}
}

// TestNodeSpecMountsNothingAndRestrictsCollectors keeps the node sidecar to
// the two collectors the consumer reads. Every other collector would add
// hundreds of lines to a scrape taken ten times a second from every node.
func TestNodeSpecMountsNothingAndRestrictsCollectors(t *testing.T) {
	containerCfg, hostCfg := NodeSpec()
	cmd := strings.Join(containerCfg.Cmd, " ")
	for _, want := range []string{
		"--web.listen-address=:" + strconv.Itoa(NodePort),
		"--collector.disable-defaults",
		"--collector.cpu",
		"--collector.meminfo",
	} {
		if !strings.Contains(cmd, want) {
			t.Errorf("command %q is missing %q", cmd, want)
		}
	}
	if hostCfg.NetworkMode != "host" {
		t.Errorf("network mode is %q, want host so the port lands on the node's addresses", hostCfg.NetworkMode)
	}
	if len(hostCfg.Mounts) != 0 || len(hostCfg.Binds) != 0 {
		t.Errorf("got mounts %v binds %v, want none", hostCfg.Mounts, hostCfg.Binds)
	}
	if len(hostCfg.CapDrop) != 1 || hostCfg.CapDrop[0] != "ALL" {
		t.Errorf("got cap drop %v, want ALL", hostCfg.CapDrop)
	}
	if !hostCfg.ReadonlyRootfs {
		t.Error("root filesystem is writable, want read only since nothing is copied in")
	}
}

// TestSidecarNameSurvivesEverySSHDestinationShape guards the first container
// in provoor whose name comes from a node rather than a constant. Docker
// accepts only [a-zA-Z0-9][a-zA-Z0-9_.-]* and rejects the container outright
// otherwise, which would fail a rig deployment well after keygen.
func TestSidecarNameSurvivesEverySSHDestinationShape(t *testing.T) {
	valid := regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)
	// cluster.HostName strips the scheme, the user and the port, so these are
	// the shapes it can hand over.
	for _, node := range []string{
		"local", "rig-01", "10.0.0.1", "node_1", "RIG.example.com",
		"fe80::1", "host with space", "a/b", "",
	} {
		for kind := range sidecarPrefix {
			got := SidecarName(kind, node)
			if !valid.MatchString(got) {
				t.Errorf("node %q yields %q, which docker rejects", node, got)
			}
		}
	}
}

func TestSidecarNameKeepsOrdinaryHostsReadable(t *testing.T) {
	for node, want := range map[string]string{
		"local":    "provoor-dcgm-local",
		"rig-01":   "provoor-dcgm-rig-01",
		"10.0.0.1": "provoor-dcgm-10.0.0.1",
	} {
		if got := SidecarName(cluster.SidecarDCGM, node); got != want {
			t.Errorf("node %q yields %q, want %q", node, got, want)
		}
	}
	if got := SidecarName(cluster.SidecarNode, "rig-01"); got != "provoor-node-rig-01" {
		t.Errorf("node sidecar is named %q, want provoor-node-rig-01", got)
	}
}
