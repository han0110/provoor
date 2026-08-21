package zisk

import (
	"os"
	"path/filepath"
	"slices"
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

const minimalConfig = `
zkvm: zisk
zkvm_version: 1.1.0-alpha
guests:
  - elf: /guests/a.elf
    vk: /guests/a.vk
coordinator:
  ssh: user@10.0.0.1
  ip: 10.0.0.1
workers:
  - ssh: user@10.0.0.1
    gpu:
      count: 1
  - ssh: user@10.0.0.2
    gpu:
      count: 2
`

// TestDefaultsOnlyConfigOmitsWorkerFlags pins that a configuration carrying no
// config block leaves every optional knob to the worker image, so a deployment
// inherits the image's own defaults rather than ones this repository picked.
func TestDefaultsOnlyConfigOmitsWorkerFlags(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimalConfig))
	if err != nil {
		t.Fatal(err)
	}
	args := workerArgs(cfg, cfg.Workers[0], cluster.WorkerName(0, cfg.Workers[0].GPU), 1)
	optional := []string{
		"--cpu-mops", "--minimal-memory", "--max-witness-stored",
		"--max-streams", "--number-threads-witness",
	}
	for _, flag := range optional {
		if slices.Contains(args, flag) {
			t.Errorf("%s must be left to the worker image, got %v", flag, args)
		}
	}
	for _, arg := range args {
		if strings.HasPrefix(arg, "RAYON_NUM_THREADS=") {
			t.Errorf("thread count must be left to the worker image, got %v", args)
		}
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimalConfig))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Image != "ghcr.io/han0110/provoor/zisk" {
		t.Errorf("Image = %q", cfg.Image)
	}
	if cfg.ImageTag != "1.1.0-alpha" {
		t.Errorf("ImageTag = %q", cfg.ImageTag)
	}
	if cfg.Verbose != 0 {
		t.Errorf("Verbose = %d", cfg.Verbose)
	}
	if cfg.Config.ShmSizeGB != 64 {
		t.Errorf("Config.ShmSizeGB = %d", cfg.Config.ShmSizeGB)
	}
	if cfg.Config.MPINp != 1 {
		t.Errorf("Config.MPINp = %d", cfg.Config.MPINp)
	}
	// Left off as the worker image has it, so a deployment that wants the CPU
	// planner asks for it rather than inheriting a choice made here.
	if cfg.Config.CPUMops {
		t.Error("Config.CPUMops should default off, matching the worker image")
	}
	if len(cfg.Guests) != 1 || cfg.Guests[0].ELF != "/guests/a.elf" || cfg.Guests[0].VK != "/guests/a.vk" {
		t.Errorf("Guests = %v", cfg.Guests)
	}
}

func TestLoadFull(t *testing.T) {
	cfg, err := Load(writeConfig(t, `
zkvm: zisk
zkvm_version: 1.1.0-alpha
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
      count: 4
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
	if cfg.Workers[0].GPU.Count != 4 || !slices.Equal(cfg.Workers[1].GPU.DeviceIDs, []int{0, 1}) {
		t.Errorf("worker gpus = %+v, %+v", cfg.Workers[0].GPU, cfg.Workers[1].GPU)
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
		CPUMops:              true,
	}
	if cfg.Config != want {
		t.Errorf("Config = %+v", cfg.Config)
	}
}

// TestImageTagDefaultsToTheZkvmVersion pins the tie between the two, since a
// deployment names one release and the image carrying it is tagged after that
// release unless it says otherwise.
func TestImageTagDefaultsToTheZkvmVersion(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimalConfig))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ImageTag != cfg.ZkvmVersion {
		t.Errorf("ImageTag = %q, want the zkvm version %q", cfg.ImageTag, cfg.ZkvmVersion)
	}
	// A tag names the image alone, so the volumes and the proving key stay on
	// the release even when an image is rebuilt under another one.
	tagged, err := Load(writeConfig(t, minimalConfig+"image_tag: local\n"))
	if err != nil {
		t.Fatal(err)
	}
	if tagged.ImageTag != "local" || tagged.ZkvmVersion != "1.1.0-alpha" {
		t.Errorf("ImageTag = %q, ZkvmVersion = %q", tagged.ImageTag, tagged.ZkvmVersion)
	}
	if got := provingKeyVolume(tagged.ZkvmVersion); got != "zisk-proving-key-1.1.0-alpha" {
		t.Errorf("proving-key volume = %q, want it keyed on the release", got)
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
			name:    "no guests",
			config:  strings.Replace(minimalConfig, "guests:\n  - elf: /guests/a.elf\n    vk: /guests/a.vk\n", "", 1),
			wantErr: "guest",
		},
		{
			name:    "guest without a vk",
			config:  strings.Replace(minimalConfig, "    vk: /guests/a.vk\n", "", 1),
			wantErr: "vk",
		},
		{
			name:    "guest without an elf",
			config:  strings.Replace(minimalConfig, "  - elf: /guests/a.elf\n    vk", "  - vk", 1),
			wantErr: "elf",
		},
		{
			name:    "no workers",
			config:  "zkvm: zisk\nzkvm_version: 1.1.0-alpha\nguests:\n  - elf: a.elf\n    vk: a.vk\ncoordinator:\n  ssh: user@10.0.0.1\nworkers: []\n",
			wantErr: "worker",
		},
		{
			name: "duplicate worker host",
			config: `
zkvm: zisk
zkvm_version: 1.1.0-alpha
guests:
  - elf: /guests/a.elf
    vk: /guests/a.vk
coordinator:
  ssh: user@10.0.0.1
  ip: 10.0.0.1
workers:
  - ssh: user@10.0.0.2
    gpu:
      count: 1
  - ssh: user@10.0.0.2
    gpu:
      count: 1
`,
			wantErr: "duplicate",
		},
		{
			name: "remote worker without coordinator ip",
			config: `
zkvm: zisk
zkvm_version: 1.1.0-alpha
guests:
  - elf: /guests/a.elf
    vk: /guests/a.vk
coordinator:
  ssh: user@10.0.0.1
workers:
  - ssh: user@10.0.0.2
    gpu:
      count: 1
`,
			wantErr: "coordinator ip",
		},
		{
			name:    "missing zkvm_version",
			config:  strings.Replace(minimalConfig, "zkvm_version: 1.1.0-alpha\n", "", 1),
			wantErr: "zkvm_version is required",
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
			name:    "worker without a gpu selection",
			config:  strings.Replace(minimalConfig, "    gpu:\n      count: 1\n", "", 1),
			wantErr: "gpu expects",
		},
		{
			name:    "worker naming both gpu forms",
			config:  strings.Replace(minimalConfig, "      count: 1\n", "      count: 1\n      device_ids: [0]\n", 1),
			wantErr: "not both",
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
zkvm_version: 1.1.0-alpha
guests:
  - elf: /guests/a.elf
    vk: /guests/a.vk
coordinator:
  ssh: user@10.0.0.1
workers:
  - ssh: user@10.0.0.1
    gpu:
      count: 1
`))
	if err != nil {
		t.Errorf("co-located single host should not require a coordinator ip, got %v", err)
	}
}
