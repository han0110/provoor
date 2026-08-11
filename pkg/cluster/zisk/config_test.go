package zisk

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const minimalConfig = `
zkvm: zisk
coordinator:
  ssh: user@10.0.0.1
  ip: 10.0.0.1
workers:
  - ssh: user@10.0.0.1
  - ssh: user@10.0.0.2
`

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimalConfig))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Image != "ghcr.io/han0110/zisk/zisk" {
		t.Errorf("Image = %q", cfg.Image)
	}
	if cfg.ImageTag != "1.0.0-alpha" {
		t.Errorf("ImageTag = %q", cfg.ImageTag)
	}
	if cfg.Verbose != 0 {
		t.Errorf("Verbose = %d", cfg.Verbose)
	}
	if cfg.Coordinator.Name != "10.0.0.1" {
		t.Errorf("Coordinator.Name = %q", cfg.Coordinator.Name)
	}
	if cfg.Workers[1].Name != "10.0.0.2" {
		t.Errorf("Workers[1].Name = %q", cfg.Workers[1].Name)
	}
	if cfg.Workers[0].Gpus != "all" {
		t.Errorf("Workers[0].Gpus = %q", cfg.Workers[0].Gpus)
	}
	if cfg.Config.ShmSizeGB != 64 {
		t.Errorf("Config.ShmSizeGB = %d", cfg.Config.ShmSizeGB)
	}
	if cfg.Config.MPINp != 1 {
		t.Errorf("Config.MPINp = %d", cfg.Config.MPINp)
	}
}

func TestLoadFull(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
zkvm: zisk
image: ghcr.io/example/zisk
image_tag: 2.0.0
verbose: 1
coordinator:
  name: node1
  ssh: ssh://user@203.0.113.1:2222
  ip: 10.0.0.1
workers:
  - name: node1
    ssh: ssh://user@203.0.113.1:2222
  - name: node2
    ssh: ssh://user@203.0.113.2:2222
    gpus: device=0,1
config:
  shm_size_gb: 32
  mpi_np: 2
  mpi_numa_ppr: 1
  mpi_threads: 32
  max_streams: 4
  number_threads_witness: 8
  max_witness_stored: 4
  minimal_memory: true
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ImageTag != "2.0.0" || cfg.Verbose != 1 {
		t.Errorf("ImageTag = %q, Verbose = %d", cfg.ImageTag, cfg.Verbose)
	}
	if cfg.Coordinator.Name != "node1" || cfg.Workers[1].Name != "node2" {
		t.Errorf("names = %q, %q", cfg.Coordinator.Name, cfg.Workers[1].Name)
	}
	if cfg.Workers[0].Gpus != "all" || cfg.Workers[1].Gpus != "device=0,1" {
		t.Errorf("worker gpus = %q, %q", cfg.Workers[0].Gpus, cfg.Workers[1].Gpus)
	}
	want := WorkerConfig{
		ShmSizeGB:            32,
		MPINp:                2,
		MPINumaPpr:           1,
		MPIThreads:           32,
		MaxStreams:           4,
		NumberThreadsWitness: 8,
		MaxWitnessStored:     4,
		MinimalMemory:        true,
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
			config:  strings.Replace(minimalConfig, "zkvm: zisk", "zkvm: openvm", 1),
			wantErr: "zkvm",
		},
		{
			name:    "no workers",
			config:  "zkvm: zisk\ncoordinator:\n  ssh: user@10.0.0.1\nworkers: []\n",
			wantErr: "worker",
		},
		{
			name: "duplicate worker host",
			config: `
zkvm: zisk
coordinator:
  ssh: user@10.0.0.1
  ip: 10.0.0.1
workers:
  - ssh: user@10.0.0.2
  - ssh: user@10.0.0.2
`,
			wantErr: "duplicate",
		},
		{
			name: "remote worker without coordinator ip",
			config: `
zkvm: zisk
coordinator:
  ssh: user@10.0.0.1
workers:
  - ssh: user@10.0.0.2
`,
			wantErr: "coordinator ip",
		},
		{
			name: "coordinator name on a different host",
			config: `
zkvm: zisk
coordinator:
  name: node1
  ssh: user@10.0.0.1
  ip: 10.0.0.1
workers:
  - name: node1
    ssh: user@10.0.0.2
`,
			wantErr: "co-location",
		},
		{
			name:    "verbose out of range",
			config:  minimalConfig + "verbose: 3\n",
			wantErr: "verbose",
		},
		{
			name:    "non-positive mpi_np",
			config:  minimalConfig + "config:\n  mpi_np: -1\n",
			wantErr: "mpi_np",
		},
		{
			name:    "invalid worker gpus",
			config:  strings.Replace(minimalConfig, "  - ssh: user@10.0.0.1\n", "  - ssh: user@10.0.0.1\n    gpus: none\n", 1),
			wantErr: "gpus",
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
	if _, err := Load("../../../examples/zisk-4x4.example.yaml"); err != nil {
		t.Errorf("shipped template must load, got %v", err)
	}
}

func TestLoadSingleHostWithoutIP(t *testing.T) {
	_, err := Load(writeConfig(t, `
zkvm: zisk
coordinator:
  ssh: user@10.0.0.1
workers:
  - ssh: user@10.0.0.1
`))
	if err != nil {
		t.Errorf("co-located single host should not require a coordinator ip, got %v", err)
	}
}
