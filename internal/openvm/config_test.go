package openvm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/han0110/provoor/internal/cluster"
)

const minimalConfig = `
zkvm: openvm
zkvm_version: 2.1.0-preview
guests:
  - elf: /guests/stateless-validator-ethrex-openvm-v2.1.0-preview.elf
    vk: /guests/stateless-validator-ethrex-openvm-v2.1.0-preview.vk
coordinator:
  ssh: user@10.0.0.1
  ip: 10.0.0.1
workers:
  - ssh: user@10.0.0.1
    gpu:
      device_ids: [0]
  - ssh: user@10.0.0.2
    ip: 10.0.0.2
    gpu:
      device_ids: [0]
`

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// Every knob left out has to reach the image as an absent key rather than a
// zero. The prover capacities and the watchdog are the exceptions, since the
// image has no usable fallback for them.
func TestDefaultsOnlyConfigDeploys(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimalConfig))
	if err != nil {
		t.Fatal(err)
	}
	manager := managerTOML(cfg)
	if !strings.Contains(manager, "timeout_secs = 600") {
		t.Errorf("the manager must carry the watchdog deadline, got\n%s", manager)
	}
	if strings.Contains(manager, "default_segment_memory") {
		t.Errorf("the manager table must not carry a segment memory, got\n%s", manager)
	}
	for _, want := range []string{"max_app_provers = 2", "max_leaf_provers = 2", "max_internal_provers = 1"} {
		if !strings.Contains(manager, want) {
			t.Errorf("the manager config must carry %q, it has no fallback for it, got\n%s", want, manager)
		}
	}
	if worker := workerTOML(cfg, cfg.Workers[0], 0); !strings.Contains(worker, "default_segment_memory = 16106127360") {
		t.Errorf("a worker must carry the default segment memory, got\n%s", worker)
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimalConfig))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Image != "ghcr.io/han0110/provoor/openvm" {
		t.Errorf("Image = %q", cfg.Image)
	}
	if cfg.ImageTag != cfg.ZkvmVersion || cfg.ImageTag != "2.1.0-preview" {
		t.Errorf("ImageTag = %q, want the zkvm version %q", cfg.ImageTag, cfg.ZkvmVersion)
	}
	want := ProverConfig{
		AppProvers:      2,
		LeafProvers:     2,
		InternalProvers: 1,
		TimeoutSecs:     600,
		ShmSizeGB:       2,
		SegmentMemory:   15 << 30,
	}
	if cfg.Prover != want {
		t.Errorf("Prover = %+v", cfg.Prover)
	}
	tagged, err := Load(writeConfig(t, minimalConfig+"image_tag: local\n"))
	if err != nil {
		t.Fatal(err)
	}
	if tagged.ImageTag != "local" || tagged.ZkvmVersion != "2.1.0-preview" {
		t.Errorf("ImageTag = %q, ZkvmVersion = %q", tagged.ImageTag, tagged.ZkvmVersion)
	}
	if got := artifactsVolume(tagged.ZkvmVersion); got != "openvm-artifacts-2.1.0-preview" {
		t.Errorf("artifacts volume = %q, want it keyed on the release", got)
	}
}

func TestLoadFull(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
zkvm: openvm
zkvm_version: 2.1.0-preview
image: ghcr.io/example/openvm
image_tag: 3.0.0
verbose: 1
guests:
  - elf: /guests/a.elf
    vk: /guests/a.vk
  - elf: https://example.com/b.elf
    vk: https://example.com/b.vk
coordinator:
  ssh: ssh://user@203.0.113.1:2222
  ip: 10.0.0.1
workers:
  - ssh: ssh://user@203.0.113.1:2222
    gpu:
      device_ids: [0]
  - ssh: ssh://user@203.0.113.1:2222
    gpu:
      device_ids: [1]
  - ssh: ssh://user@203.0.113.2:2222
    ip: 10.0.0.2
    gpu:
      device_ids: [0]
config:
  app_provers: 4
  leaf_provers: 4
  internal_provers: 2
  segment_memory: 15569256448
  timeout_secs: 3600
  shm_size_gb: 4
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ImageTag != "3.0.0" || cfg.Verbose != 1 {
		t.Errorf("ImageTag = %q, Verbose = %d", cfg.ImageTag, cfg.Verbose)
	}
	wantGuest := cluster.Guest{ELF: "https://example.com/b.elf", VK: "https://example.com/b.vk"}
	if len(cfg.Guests) != 2 || cfg.Guests[1] != wantGuest {
		t.Errorf("Guests = %v", cfg.Guests)
	}
	if cfg.Workers[2].IP != "10.0.0.2" || cfg.Workers[1].deviceID() != 1 || workerName(1, cfg.Workers[1]) != "worker_1-gpu_1" {
		t.Errorf("Workers = %+v", cfg.Workers)
	}
	want := ProverConfig{
		AppProvers:      4,
		LeafProvers:     4,
		InternalProvers: 2,
		SegmentMemory:   15569256448,
		TimeoutSecs:     3600,
		ShmSizeGB:       4,
	}
	if cfg.Prover != want {
		t.Errorf("Prover = %+v", cfg.Prover)
	}
}

func TestLoadValidation(t *testing.T) {
	cases := []struct {
		name    string
		config  string
		wantErr string
	}{
		{
			name:    "wrong zkvm",
			config:  strings.Replace(minimalConfig, "zkvm: openvm", "zkvm: zisk", 1),
			wantErr: "zkvm",
		},
		{
			name:    "remote worker without ip",
			config:  strings.Replace(minimalConfig, "    ip: 10.0.0.2\n", "", 1),
			wantErr: "worker ip",
		},
		{
			name:    "colliding advertised worker url",
			config:  minimalConfig + "  - ssh: user@10.0.0.3\n    ip: 10.0.0.2\n    gpu:\n      device_ids: [0]\n",
			wantErr: "already advertised by another worker",
		},
		{
			name:    "overlapping device ids on one host",
			config:  minimalConfig + "  - ssh: user@10.0.0.1\n    gpu:\n      device_ids: [0]\n",
			wantErr: "duplicate worker gpu 0 on host",
		},
		{
			name:    "two device ids",
			config:  strings.Replace(minimalConfig, "device_ids: [0]", "device_ids: [0, 1]", 1),
			wantErr: "gpu device_ids expects exactly one id",
		},
		{
			name:    "device ids checked before the worker ip",
			config:  minimalConfig + "  - ssh: user@10.0.0.3\n    gpu:\n      device_ids: []\n",
			wantErr: "worker 2: gpu device_ids is required",
		},
		{
			name:    "negative gpu",
			config:  strings.Replace(minimalConfig, "device_ids: [0]", "device_ids: [-1]", 1),
			wantErr: "gpu device_ids expects non-negative ids",
		},
		{
			name:    "non positive app provers",
			config:  minimalConfig + "config:\n  app_provers: -1\n",
			wantErr: "app_provers",
		},
		{
			name:    "negative segment memory",
			config:  minimalConfig + "config:\n  segment_memory: -1\n",
			wantErr: "segment_memory",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.config))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadExamples(t *testing.T) {
	for _, path := range []string{"../../examples/openvm-4x4.example.yaml", "../../examples/openvm-1x1-local.example.yaml"} {
		if _, err := Load(path); err != nil {
			t.Errorf("%s must load, got %v", path, err)
		}
	}
}
