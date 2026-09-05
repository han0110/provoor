package zisk

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const minimalConfig = `
zkvm: zisk
zkvm_version: 1.2.0-alpha
guests:
  - elf: /guests/a.elf
    vk: /guests/a.vk
coordinator:
  ssh: user@10.0.0.1
  ip: 10.0.0.1
workers:
  - ssh: user@10.0.0.1
    gpu:
      device_ids: [0]
  - ssh: user@10.0.0.2
    gpu:
      device_ids: [0, 1]
`

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimalConfig))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Image != "ghcr.io/han0110/provoor/zisk" {
		t.Errorf("Image = %q", cfg.Image)
	}
	if cfg.ImageTag != cfg.ZkvmVersion || cfg.ImageTag != "1.2.0-alpha" {
		t.Errorf("ImageTag = %q, want the zkvm version %q", cfg.ImageTag, cfg.ZkvmVersion)
	}
	if cfg.Verbose != 0 {
		t.Errorf("Verbose = %d", cfg.Verbose)
	}
	if want := (ProverConfig{ShmSizeGB: 64, MPINp: 1}); cfg.Prover != want {
		t.Errorf("Prover = %+v, want %+v", cfg.Prover, want)
	}
	if len(cfg.Guests) != 1 || cfg.Guests[0].ELF != "/guests/a.elf" || cfg.Guests[0].VK != "/guests/a.vk" {
		t.Errorf("Guests = %v", cfg.Guests)
	}
	// A tag names the image alone, so the volumes stay keyed on the release.
	tagged, err := Load(writeConfig(t, minimalConfig+"image_tag: local\n"))
	if err != nil {
		t.Fatal(err)
	}
	if tagged.ImageTag != "local" || tagged.ZkvmVersion != "1.2.0-alpha" {
		t.Errorf("ImageTag = %q, ZkvmVersion = %q", tagged.ImageTag, tagged.ZkvmVersion)
	}
	if got := provingKeyVolume(tagged.ZkvmVersion); got != "zisk-proving-key-1.2.0-alpha" {
		t.Errorf("proving-key volume = %q, want it keyed on the release", got)
	}
}

func TestLoadFull(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
zkvm: zisk
zkvm_version: 1.2.0-alpha
image: ghcr.io/example/zisk
image_tag: 2.0.0
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
      device_ids: [0, 1, 2, 3]
  - ssh: ssh://user@203.0.113.2:2222
    gpu:
      device_ids: [0, 1]
config:
  shm_size_gb: 32
  mpi_np: 2
  mpi_numa_ppr: 1
  mpi_threads: 32
  max_streams: 4
  number_threads_witness: 8
  max_witness_stored: 4
  minimal_memory: true
  cpu_mops: true
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ImageTag != "2.0.0" || cfg.Verbose != 1 {
		t.Errorf("ImageTag = %q, Verbose = %d", cfg.ImageTag, cfg.Verbose)
	}
	if len(cfg.Guests) != 2 || cfg.Guests[1].VK != "https://example.com/b.vk" {
		t.Errorf("Guests = %v", cfg.Guests)
	}
	if !slices.Equal(cfg.Workers[0].GPU.DeviceIDs, []int{0, 1, 2, 3}) || !slices.Equal(cfg.Workers[1].GPU.DeviceIDs, []int{0, 1}) {
		t.Errorf("worker gpus = %+v, %+v", cfg.Workers[0].GPU, cfg.Workers[1].GPU)
	}
	want := ProverConfig{
		ShmSizeGB:            32,
		MPINp:                2,
		MPINumaPpr:           1,
		MPIThreads:           32,
		MaxStreams:           4,
		NumberThreadsWitness: 8,
		MaxWitnessStored:     4,
		MinimalMemory:        true,
		CPUMops:              true,
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
			config:  strings.Replace(minimalConfig, "zkvm: zisk", "zkvm: openvm", 1),
			wantErr: "zkvm",
		},
		{
			name:    "no workers",
			config:  "zkvm: zisk\nzkvm_version: 1.2.0-alpha\nguests:\n  - elf: a.elf\n    vk: a.vk\ncoordinator:\n  ssh: user@10.0.0.1\nworkers: []\n",
			wantErr: "worker",
		},
		{
			name: "duplicate worker host",
			config: `
zkvm: zisk
zkvm_version: 1.2.0-alpha
guests:
  - elf: /guests/a.elf
    vk: /guests/a.vk
coordinator:
  ssh: user@10.0.0.1
  ip: 10.0.0.1
workers:
  - ssh: user@10.0.0.2
    gpu:
      device_ids: [0]
  - ssh: user@10.0.0.2
    gpu:
      device_ids: [0]
`,
			wantErr: "duplicate",
		},
		{
			name: "remote worker without coordinator ip",
			config: `
zkvm: zisk
zkvm_version: 1.2.0-alpha
guests:
  - elf: /guests/a.elf
    vk: /guests/a.vk
coordinator:
  ssh: user@10.0.0.1
workers:
  - ssh: user@10.0.0.2
    gpu:
      device_ids: [0]
`,
			wantErr: "coordinator ip",
		},
		{
			name: "single host without ip",
			config: `
zkvm: zisk
zkvm_version: 1.2.0-alpha
guests:
  - elf: /guests/a.elf
    vk: /guests/a.vk
coordinator:
  ssh: user@10.0.0.1
workers:
  - ssh: user@10.0.0.1
    gpu:
      device_ids: [0]
`,
		},
		{
			name:    "negative mpi_np",
			config:  minimalConfig + "config:\n  mpi_np: -1\n",
			wantErr: "mpi_np",
		},
		{
			name:    "worker without a gpu selection",
			config:  strings.Replace(minimalConfig, "    gpu:\n      device_ids: [0]\n", "", 1),
			wantErr: "worker 0: gpu device_ids is required",
		},
		{
			name:    "worker naming a gpu count",
			config:  strings.Replace(minimalConfig, "      device_ids: [0]\n", "      count: 1\n", 1),
			wantErr: "field count not found in type cluster.GPU",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.config))
			if tc.wantErr == "" {
				if err != nil {
					t.Errorf("err = %v, want the config to load", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadExamples(t *testing.T) {
	for _, path := range []string{"../../examples/zisk-4x4.example.yaml", "../../examples/zisk-1x1-local.example.yaml"} {
		if _, err := Load(path); err != nil {
			t.Errorf("%s must load, got %v", path, err)
		}
	}
}
