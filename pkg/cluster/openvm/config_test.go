package openvm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/han0110/provoor/pkg/cluster"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const minimalGuest = `guests:
  - elf: /guests/stateless-validator-ethrex-openvm-v2.1.0-preview.elf
    vk: /guests/stateless-validator-ethrex-openvm-v2.1.0-preview.vk
`

const minimalConfig = `
zkvm: openvm
` + minimalGuest + `coordinator:
  ssh: user@10.0.0.1
  ip: 10.0.0.1
workers:
  - ssh: user@10.0.0.1
    gpu: 0
  - ssh: user@10.0.0.2
    ip: 10.0.0.2
    gpu: 0
`

// TestDefaultsOnlyConfigDeploys pins that a configuration carrying no config
// block renders a usable deployment. Every knob it leaves out has to reach the
// image as an absent key rather than a zero, which the manager rejects, and
// the prover capacities have to be rendered because the manager alone has no
// fallback for them.
func TestDefaultsOnlyConfigDeploys(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimalConfig))
	if err != nil {
		t.Fatal(err)
	}
	manager := managerTOML(cfg)
	if strings.Contains(manager, "timeout_secs") {
		t.Errorf("timeout_secs must be omitted so the manager applies its own, got\n%s", manager)
	}
	// Only the workers carry a segment memory, so the manager's table drops it
	// whatever the configuration says.
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
	if cfg.Image != "ghcr.io/han0110/openvm" {
		t.Errorf("Image = %q", cfg.Image)
	}
	if cfg.ImageTag != "2.1.0-preview" {
		t.Errorf("ImageTag = %q", cfg.ImageTag)
	}
	want := ProverConfig{
		AppProvers:      2,
		LeafProvers:     2,
		InternalProvers: 1,
		// Left at zero so the manager applies its own deadline.
		TimeoutSecs:   0,
		ShmSizeGB:     2,
		SegmentMemory: 15 << 30,
	}
	if cfg.Config != want {
		t.Errorf("Config = %+v", cfg.Config)
	}
}

func TestLoadFull(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
zkvm: openvm
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
    gpu: 0
  - ssh: ssh://user@203.0.113.1:2222
    gpu: 1
  - ssh: ssh://user@203.0.113.2:2222
    ip: 10.0.0.2
    gpu: 0
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
	if cfg.Workers[2].IP != "10.0.0.2" || cfg.Workers[1].GPU != 1 {
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
	if cfg.Config != want {
		t.Errorf("Config = %+v", cfg.Config)
	}
}

func TestLoadRejectsUnknownField(t *testing.T) {
	_, err := Load(writeConfig(t, minimalConfig+"cluster_endpoint: http://x\n"))
	if err == nil {
		t.Fatal("expected error for unknown field")
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
			name:    "no guests",
			config:  strings.Replace(minimalConfig, minimalGuest, "", 1),
			wantErr: "guest",
		},
		{
			name:    "guest without elf",
			config:  strings.Replace(minimalConfig, minimalGuest, "guests:\n  - vk: /guests/a.vk\n", 1),
			wantErr: "guest 0 is missing elf",
		},
		{
			name:    "guest without vk",
			config:  strings.Replace(minimalConfig, minimalGuest, "guests:\n  - elf: /guests/a.elf\n", 1),
			wantErr: "guest 0 is missing vk",
		},
		{
			name: "remote worker without ip",
			config: strings.Replace(minimalConfig, `  - ssh: user@10.0.0.2
    ip: 10.0.0.2
    gpu: 0
`, "  - ssh: user@10.0.0.2\n    gpu: 0\n", 1),
			wantErr: "worker ip",
		},
		{
			// Two hosts reaching the same address is the copy-paste that the
			// ssh and gpu pair does not catch, and the manager would dial one
			// worker twice.
			name: "colliding advertised worker url",
			config: strings.Replace(minimalConfig, `  - ssh: user@10.0.0.2
    ip: 10.0.0.2
    gpu: 0
`, "  - ssh: user@10.0.0.2\n    ip: 10.0.0.2\n    gpu: 0\n  - ssh: user@10.0.0.3\n    ip: 10.0.0.2\n    gpu: 0\n", 1),
			wantErr: "already advertised by another worker",
		},
		{
			name: "duplicate gpu on one host",
			config: strings.Replace(minimalConfig, "  - ssh: user@10.0.0.1\n    gpu: 0\n",
				"  - ssh: user@10.0.0.1\n    gpu: 0\n  - ssh: user@10.0.0.1\n    gpu: 0\n", 1),
			wantErr: "duplicate worker gpu",
		},
		{
			name:    "negative gpu",
			config:  strings.Replace(minimalConfig, "  - ssh: user@10.0.0.1\n    gpu: 0\n", "  - ssh: user@10.0.0.1\n    gpu: -1\n", 1),
			wantErr: "device id",
		},
		{
			name:    "verbose out of range",
			config:  minimalConfig + "verbose: 3\n",
			wantErr: "verbose",
		},
		{
			name:    "non-positive app_provers",
			config:  minimalConfig + "config:\n  app_provers: -1\n",
			wantErr: "app_provers",
		},
		{
			name:    "negative segment_memory",
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

func TestLoadExampleTemplate(t *testing.T) {
	if _, err := Load("../../../examples/openvm-4x4.example.yaml"); err != nil {
		t.Errorf("shipped template must load, got %v", err)
	}
}
