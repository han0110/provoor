package cluster

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
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
		t.Error("expected an error for a missing file")
	}
}

func TestDecodeRejectsUnknownField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	if err := os.WriteFile(path, []byte("zkvm: zisk\nbogus: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := Decode(path, &cfg); err == nil {
		t.Error("expected an error for an unknown field")
	}
}

func TestConfigDefaultsAndValidate(t *testing.T) {
	cfg := Config{Zkvm: "zisk", ZkvmVersion: "1.2.0-alpha", Guests: []Guest{{ELF: "a.elf", VK: "a.vk"}}}
	cfg.SetDefaults()
	if cfg.ImageRef() != "ghcr.io/han0110/provoor/zisk:1.2.0-alpha" {
		t.Errorf("ImageRef = %q", cfg.ImageRef())
	}
	if err := cfg.Validate("zisk"); err != nil {
		t.Errorf("Validate = %v", err)
	}
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"wrong zkvm", func(c *Config) { c.Zkvm = "openvm" }, "zkvm"},
		{"missing version", func(c *Config) { c.ZkvmVersion = "" }, "zkvm_version is required"},
		{"verbose out of range", func(c *Config) { c.Verbose = 3 }, "verbose"},
		{"no guests", func(c *Config) { c.Guests = nil }, "guest"},
		{"guest without elf", func(c *Config) { c.Guests[0].ELF = "" }, "guest 0 is missing elf"},
		{"guest without vk", func(c *Config) { c.Guests[0].VK = "" }, "guest 0 is missing vk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := cfg
			c.Guests = []Guest{{ELF: "a.elf", VK: "a.vk"}}
			tc.mutate(&c)
			if err := c.Validate("zisk"); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Validate = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestWorkerName(t *testing.T) {
	cases := []struct {
		index int
		gpu   GPU
		want  string
	}{
		{0, GPU{Count: 4}, "worker_0-gpu_x4"},
		{0, GPU{DeviceIDs: []int{0, 1}}, "worker_0-gpu_0_1"},
		{0, GPU{DeviceIDs: []int{3}}, "worker_0-gpu_3"},
		{2, GPU{Count: 1}, "worker_2-gpu_x1"},
	}
	for _, tc := range cases {
		if got := WorkerName(tc.index, tc.gpu); got != tc.want {
			t.Errorf("WorkerName(%d, %+v) = %q, want %q", tc.index, tc.gpu, got, tc.want)
		}
	}
}

func TestGPULenAndDeviceRequest(t *testing.T) {
	count := GPU{Count: 4}
	if count.Len() != 4 {
		t.Errorf("Len() = %d", count.Len())
	}
	if request := count.DeviceRequest(); request.Count != 4 || len(request.DeviceIDs) != 0 {
		t.Errorf("DeviceRequest() = %+v", request)
	}
	devices := GPU{DeviceIDs: []int{0, 2}}
	if devices.Len() != 2 {
		t.Errorf("Len() = %d", devices.Len())
	}
	if request := devices.DeviceRequest(); request.Count != 0 || !reflect.DeepEqual(request.DeviceIDs, []string{"0", "2"}) {
		t.Errorf("DeviceRequest() = %+v", request)
	}
	for _, gpu := range []GPU{count, devices} {
		if got := gpu.DeviceRequest().Capabilities; !reflect.DeepEqual(got, [][]string{{"gpu"}}) {
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
			if tc.wantErr == "" && err != nil {
				t.Errorf("Validate() = %v, want nil", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Errorf("Validate() = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateColocation(t *testing.T) {
	coordinator := Node{SSH: "user@10.0.0.1"}
	if err := ValidateColocation(coordinator, Node{SSH: "user@10.0.0.1"}); err != nil {
		t.Errorf("a co-located worker needs no coordinator ip, got %v", err)
	}
	if err := ValidateColocation(coordinator, Node{SSH: "user@10.0.0.2"}); err == nil {
		t.Error("a remote worker without a coordinator ip must be rejected")
	}
	if err := ValidateColocation(Node{SSH: "user@10.0.0.1", IP: "10.0.0.1"}, Node{SSH: "user@10.0.0.2"}); err != nil {
		t.Errorf("a remote worker with a coordinator ip is fine, got %v", err)
	}
}

func TestTelemetryValidate(t *testing.T) {
	hosts := []string{"", "user@rig-02"}
	for name, telemetry := range map[string]Telemetry{
		"unknown kind": {Sidecars: []Sidecar{{Kind: "cadvisor"}}},
		"unknown host": {Sidecars: []Sidecar{{SSH: "user@rig-03", Kind: sidecarDCGM}}},
		"repeat":       {Sidecars: []Sidecar{{Kind: sidecarNode}, {Kind: sidecarNode}}},
	} {
		if err := telemetry.Validate(hosts); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	both := Telemetry{Sidecars: []Sidecar{{Kind: sidecarDCGM}, {Kind: sidecarNode}, {SSH: "user@rig-02", Kind: sidecarDCGM}}}
	if err := both.Validate(hosts); err != nil {
		t.Errorf("both kinds on one host and one on another: %v", err)
	}
	if err := (Telemetry{}).Validate(hosts); err != nil {
		t.Errorf("an empty list: %v", err)
	}
	if got := (Telemetry{}).interval(); got != 100*time.Millisecond {
		t.Errorf("interval = %s, want 100ms by default", got)
	}
	if got := (Telemetry{IntervalMs: 250}).interval(); got != 250*time.Millisecond {
		t.Errorf("interval = %s, want 250ms", got)
	}
}
