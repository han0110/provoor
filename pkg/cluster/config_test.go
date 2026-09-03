package cluster

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestZkvm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	if err := os.WriteFile(path, []byte("zkvm: zisk\nunrelated: ignored\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	zkvm, err := Zkvm(path)
	if err != nil || zkvm != "zisk" {
		t.Errorf("Zkvm = %q, err = %v", zkvm, err)
	}
	if _, err := Zkvm(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("expected error for a missing file")
	}
}

// TestWorkerName pins the worker identity, which a worker registers under and
// every progress line carries, so a run's logs stay greppable across releases.
func TestWorkerName(t *testing.T) {
	cases := []struct {
		gpu  GPU
		want string
	}{
		{GPU{Count: 4}, "worker_0-gpu_x4"},
		{GPU{DeviceIDs: []int{0, 1}}, "worker_0-gpu_0_1"},
		{GPU{DeviceIDs: []int{3}}, "worker_0-gpu_3"},
	}
	for _, tc := range cases {
		if got := WorkerName(0, tc.gpu); got != tc.want {
			t.Errorf("WorkerName(0, %+v) = %q, want %q", tc.gpu, got, tc.want)
		}
	}
	if got := WorkerName(2, GPU{Count: 1}); got != "worker_2-gpu_x1" {
		t.Errorf("WorkerName(2, ...) = %q", got)
	}
}

func TestGPULenAndDeviceRequest(t *testing.T) {
	count := GPU{Count: 4}
	if count.Len() != 4 {
		t.Errorf("Len() = %d", count.Len())
	}
	request := count.DeviceRequest()
	if request.Count != 4 || len(request.DeviceIDs) != 0 {
		t.Errorf("DeviceRequest() = %+v", request)
	}

	devices := GPU{DeviceIDs: []int{0, 2}}
	if devices.Len() != 2 {
		t.Errorf("Len() = %d", devices.Len())
	}
	// The daemon takes device ids as strings, the same shape docker --gpus
	// device=0,2 produces.
	request = devices.DeviceRequest()
	if request.Count != 0 || !reflect.DeepEqual(request.DeviceIDs, []string{"0", "2"}) {
		t.Errorf("DeviceRequest() = %+v", request)
	}
	for _, request := range []GPU{count, devices} {
		if got := request.DeviceRequest().Capabilities; !reflect.DeepEqual(got, [][]string{{"gpu"}}) {
			t.Errorf("Capabilities = %+v", got)
		}
	}
}

func TestGPUValidate(t *testing.T) {
	cases := []struct {
		name    string
		gpu     GPU
		wantErr string
	}{
		{"count", GPU{Count: 4}, ""},
		{"device ids", GPU{DeviceIDs: []int{0, 1}}, ""},
		{"neither", GPU{}, "expects a count or device_ids"},
		{"both", GPU{Count: 1, DeviceIDs: []int{0}}, "not both"},
		{"negative count", GPU{Count: -1}, "positive integer"},
		{"negative id", GPU{DeviceIDs: []int{-1}}, "non-negative"},
		{"repeated id", GPU{DeviceIDs: []int{1, 1}}, "repeats"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.gpu.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Validate() = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

// TestValidateColocation covers the rule that a worker off the coordinator's
// host needs an address to dial it by, co-location being exact now that a host
// is its SSH destination.
func TestValidateColocation(t *testing.T) {
	coordinator := Node{SSH: "user@10.0.0.1"}
	if err := ValidateColocation(coordinator, Node{SSH: "user@10.0.0.1"}); err != nil {
		t.Errorf("a co-located worker needs no coordinator ip, got %v", err)
	}
	if err := ValidateColocation(coordinator, Node{SSH: "user@10.0.0.2"}); err == nil {
		t.Error("a remote worker without a coordinator ip should be rejected")
	}
	withIP := Node{SSH: "user@10.0.0.1", IP: "10.0.0.1"}
	if err := ValidateColocation(withIP, Node{SSH: "user@10.0.0.2"}); err != nil {
		t.Errorf("a remote worker with a coordinator ip is fine, got %v", err)
	}
}

// TestTelemetryValidate covers the three ways a sidecar list can name
// something the deployment cannot run.
func TestTelemetryValidate(t *testing.T) {
	hosts := []string{"", "user@rig-02"}
	for name, telemetry := range map[string]Telemetry{
		"unknown kind": {Sidecars: []Sidecar{{Kind: "cadvisor"}}},
		"unknown host": {Sidecars: []Sidecar{{SSH: "user@rig-03", Kind: SidecarDCGM}}},
		"repeat":       {Sidecars: []Sidecar{{Kind: SidecarNode}, {Kind: SidecarNode}}},
	} {
		if err := telemetry.Validate(hosts); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	both := Telemetry{Sidecars: []Sidecar{{Kind: SidecarDCGM}, {Kind: SidecarNode}, {SSH: "user@rig-02", Kind: SidecarDCGM}}}
	if err := both.Validate(hosts); err != nil {
		t.Errorf("both kinds on one host and one on another: %v", err)
	}
	if err := (Telemetry{}).Validate(hosts); err != nil {
		t.Errorf("an empty list: %v", err)
	}
}
