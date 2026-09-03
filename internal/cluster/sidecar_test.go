package cluster

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDCGMSidecarSpec(t *testing.T) {
	spec := sidecarSpec(sidecarDCGM, 250*time.Millisecond)
	if len(spec.HostConfig.Resources.DeviceRequests) != 0 || len(spec.HostConfig.CapAdd) != 0 {
		t.Errorf("the sidecar must need no device and no capability, got %+v", spec.HostConfig.Resources)
	}
	if spec.HostConfig.NetworkMode != "host" || len(spec.HostConfig.PortBindings) != 0 {
		t.Errorf("network mode = %q with %d bindings, want host and none", spec.HostConfig.NetworkMode, len(spec.HostConfig.PortBindings))
	}
	if len(spec.HostConfig.CapDrop) != 1 || spec.HostConfig.CapDrop[0] != "ALL" {
		t.Errorf("cap drop = %v, want ALL", spec.HostConfig.CapDrop)
	}
	cmd := strings.Join(spec.Config.Cmd, " ")
	for _, want := range []string{
		"-f " + dcgmFieldsPath,
		"-r " + dcgmHostEngine,
		"-a :" + strconv.Itoa(dcgmPort),
		"-c 250",
		"--disable-startup-validate",
		"--no-hostname",
	} {
		if !strings.Contains(cmd, want) {
			t.Errorf("command %q is missing %q", cmd, want)
		}
	}
	if string(spec.Files[dcgmFieldsPath]) != string(dcgmFields) {
		t.Error("the field list is not written into the container")
	}
}

func TestNodeSidecarSpec(t *testing.T) {
	spec := sidecarSpec(sidecarNode, 0)
	cmd := strings.Join(spec.Config.Cmd, " ")
	for _, want := range []string{
		"--web.listen-address=:" + strconv.Itoa(nodePort),
		"--collector.disable-defaults",
		"--collector.cpu",
		"--collector.meminfo",
	} {
		if !strings.Contains(cmd, want) {
			t.Errorf("command %q is missing %q", cmd, want)
		}
	}
	if spec.HostConfig.NetworkMode != "host" || !spec.HostConfig.ReadonlyRootfs {
		t.Errorf("network mode = %q, read only = %v, want host and true", spec.HostConfig.NetworkMode, spec.HostConfig.ReadonlyRootfs)
	}
	if len(spec.HostConfig.Mounts) != 0 || len(spec.HostConfig.Binds) != 0 || len(spec.Files) != 0 {
		t.Errorf("got mounts %v binds %v files %v, want none", spec.HostConfig.Mounts, spec.HostConfig.Binds, spec.Files)
	}
	if len(spec.HostConfig.CapDrop) != 1 || spec.HostConfig.CapDrop[0] != "ALL" {
		t.Errorf("cap drop = %v, want ALL", spec.HostConfig.CapDrop)
	}
}

// TestDCGMFieldsAreWellFormed guards the file the exporter reads, which
// rejects a malformed line at startup.
func TestDCGMFieldsAreWellFormed(t *testing.T) {
	lines := strings.Split(strings.TrimSpace(string(dcgmFields)), "\n")
	if len(lines) < 14 {
		t.Fatalf("got %d fields, want the full set", len(lines))
	}
	seen := map[string]bool{}
	for _, line := range lines {
		columns := strings.Split(line, ",")
		if len(columns) != 3 {
			t.Errorf("line %q has %d columns, want a name, a type, and help text", line, len(columns))
			continue
		}
		name := strings.TrimSpace(columns[0])
		if !strings.HasPrefix(name, "DCGM_FI_") || seen[name] {
			t.Errorf("field %q is not a unique DCGM field name", name)
		}
		seen[name] = true
		if kind := strings.TrimSpace(columns[1]); kind != "counter" && kind != "gauge" {
			t.Errorf("field %s declares type %q, want counter or gauge", name, kind)
		}
		if strings.TrimSpace(columns[2]) == "" {
			t.Errorf("field %s carries no help text", name)
		}
	}
	for _, want := range []string{
		"DCGM_FI_PROF_SM_CYCLES_ELAPSED_TOTAL",
		"DCGM_FI_PROF_SM_CYCLES_ACTIVE_TOTAL",
		"DCGM_FI_PROF_INT_CYCLES_ACTIVE_TOTAL",
		"DCGM_FI_PROF_PCIE_RX_BYTES_TOTAL",
		"DCGM_FI_PROF_PCIE_TX_BYTES_TOTAL",
	} {
		if !seen[want] {
			t.Errorf("field list is missing the cumulative counter %s", want)
		}
	}
}

// TestDCGMFieldsMatchTheCollector keeps the sidecar and the benchmarkoor
// collector in step.
func TestDCGMFieldsMatchTheCollector(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "benchmarkoor", "pkg", "remotemetrics", "exporter.go"))
	if err != nil {
		t.Fatal(err)
	}
	recorded := map[string]bool{}
	for _, match := range regexp.MustCompile(`"(DCGM_FI_[A-Z0-9_]+)":\s*\{`).FindAllStringSubmatch(string(source), -1) {
		recorded[match[1]] = true
	}
	served := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(dcgmFields)), "\n") {
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

// TestSidecarName guards the one container name derived from a node, since
// Docker accepts only [a-zA-Z0-9][a-zA-Z0-9_.-]*.
func TestSidecarName(t *testing.T) {
	valid := regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)
	for _, node := range []string{"local", "rig-01", "10.0.0.1", "node_1", "RIG.example.com", "fe80::1", "host with space", "a/b", ""} {
		for kind := range sidecarPrefix {
			if got := sidecarName(kind, node); !valid.MatchString(got) {
				t.Errorf("node %q yields %q, which docker rejects", node, got)
			}
		}
	}
	for node, want := range map[string]string{"local": "provoor-dcgm-local", "rig-01": "provoor-dcgm-rig-01", "10.0.0.1": "provoor-dcgm-10.0.0.1"} {
		if got := sidecarName(sidecarDCGM, node); got != want {
			t.Errorf("node %q yields %q, want %q", node, got, want)
		}
	}
	if got := sidecarName(sidecarNode, "rig-01"); got != "provoor-node-rig-01" {
		t.Errorf("node sidecar is named %q, want provoor-node-rig-01", got)
	}
}
