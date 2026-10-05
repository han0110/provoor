package estimate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ethpandaops/benchmarkoor/pkg/config"
	"github.com/ethpandaops/benchmarkoor/pkg/executor"
	"google.golang.org/protobuf/proto"

	"github.com/han0110/provoor/internal/cluster"
	"github.com/han0110/provoor/internal/ereserver"
	"github.com/han0110/provoor/internal/ereserver/api"
)

func TestPendingTests(t *testing.T) {
	names := []string{"first", "second", "third"}
	cases := []struct {
		name   string
		result *artifact
		want   []string
	}{
		{
			name:   "nothing estimated",
			result: &artifact{},
			want:   names,
		},
		{
			name:   "one estimated",
			result: &artifact{Tests: map[string]*ereserver.CostEstimation{"second": {}}},
			want:   []string{"first", "third"},
		},
		{
			name: "a failure is final",
			result: &artifact{
				Tests:    map[string]*ereserver.CostEstimation{"first": {}},
				Failures: map[string]string{"second": "guest panicked"},
			},
			want: []string{"third"},
		},
		{
			name: "everything estimated",
			result: &artifact{
				Tests: map[string]*ereserver.CostEstimation{"first": {}, "second": {}, "third": {}},
			},
			want: []string{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := slices.Sorted(maps.Keys(pendingTests(names, tc.result)))
			if !slices.Equal(got, tc.want) {
				t.Errorf("pending = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCheckResumable(t *testing.T) {
	const (
		image     = "ghcr.io/eth-act/ere/ere-server-zisk:0.18.0"
		elfSHA256 = "8f2c"
	)
	cases := []struct {
		name    string
		result  *artifact
		wantErr bool
	}{
		{
			name:   "nothing estimated yet",
			result: &artifact{},
		},
		{
			name: "same image and elf",
			result: &artifact{
				Image: image, ELFSHA256: elfSHA256,
				Tests: map[string]*ereserver.CostEstimation{"first": {}},
			},
		},
		{
			name: "another image",
			result: &artifact{
				Image: "ghcr.io/eth-act/ere/ere-server-zisk:0.17.0", ELFSHA256: elfSHA256,
				Tests: map[string]*ereserver.CostEstimation{"first": {}},
			},
			wantErr: true,
		},
		{
			name: "another elf",
			result: &artifact{
				Image: image, ELFSHA256: "1b0d",
				Failures: map[string]string{"first": "guest panicked"},
			},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkResumable(artifactName, tc.result, image, elfSHA256)
			if (err != nil) != tc.wantErr {
				t.Errorf("err = %v, want error %v", err, tc.wantErr)
			}
		})
	}
}

func TestEstimateAll(t *testing.T) {
	const (
		okTest   = "benchmark/example/test_input.py::test_ok[fork_Amsterdam]"
		failTest = "benchmark/example/test_input.py::test_fail[fork_Amsterdam]"
		fixture  = `{
			"tests/` + okTest + `": {"_info": {"fixture-format": "blockchain_test"},
				"blocks": [{"statelessInputBytes": "0xccdd"}]},
			"tests/` + failTest + `": {"_info": {"fixture-format": "blockchain_test"},
				"blocks": [{"statelessInputBytes": "0xeeff"}]}}`
	)
	fixtures := t.TempDir()
	if err := os.WriteFile(filepath.Join(fixtures, "example.json"), []byte(fixture), 0o644); err != nil {
		t.Fatal(err)
	}

	peakHeapBytes := uint64(4096)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var request api.ExecuteEstimatedCostRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			t.Error(err)
			return
		}
		response := &api.ExecuteEstimatedCostResponse{
			Result: &api.ExecuteEstimatedCostResponse_Err{Err: "guest panicked"},
		}
		if bytes.Equal(request.InputStdin, []byte{0xcc, 0xdd}) {
			response.Result = &api.ExecuteEstimatedCostResponse_Ok{Ok: &api.ExecuteEstimatedCostOk{
				Cost:          map[string]uint64{"rv64": 12},
				PeakHeapBytes: &peakHeapBytes,
			}}
		}
		encoded, err := proto.Marshal(response)
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = w.Write(encoded)
	}))
	t.Cleanup(server.Close)

	result := &artifact{
		Tests:    map[string]*ereserver.CostEstimation{},
		Failures: map[string]string{},
	}
	path := filepath.Join(t.TempDir(), artifactName)
	client := &ereserver.Client{BaseURL: server.URL, HTTP: server.Client()}
	pending := pendingTests([]string{okTest, failTest}, result)
	if err := estimateAll(t.Context(), client, fixtures, pending, 2, result, path, cluster.NewOutput(io.Discard)); err != nil {
		t.Fatal(err)
	}

	want := &artifact{
		Tests:    map[string]*ereserver.CostEstimation{okTest: {Cost: map[string]uint64{"rv64": 12}, PeakHeapBytes: &peakHeapBytes}},
		Failures: map[string]string{failTest: "guest panicked"},
	}
	if !reflect.DeepEqual(result, want) {
		t.Errorf("result = %+v, want %+v", result, want)
	}
	written, err := readArtifact(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(written, want) {
		t.Errorf("artifact = %+v, want %+v", written, want)
	}
}

func TestEstimateAllSavesNoEmptyArtifact(t *testing.T) {
	result := &artifact{
		Tests:    map[string]*ereserver.CostEstimation{},
		Failures: map[string]string{},
	}
	path := filepath.Join(t.TempDir(), artifactName)
	// The fixtures directory is empty, so the estimation fails before it
	// contacts the server.
	pending := pendingTests([]string{"first"}, result)
	err := estimateAll(t.Context(), &ereserver.Client{}, t.TempDir(), pending, 1, result, path, cluster.NewOutput(io.Discard))
	if err == nil || !strings.Contains(err.Error(), "holds no fixture") {
		t.Fatalf("estimateAll = %v, want the error of the missing fixture", err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Errorf("stat = %v, want no artifact", err)
	}
}

func TestEreServerTag(t *testing.T) {
	cases := []struct {
		name        string
		zkvm        string
		zkvmVersion string
		ereTag      string
		want        string
		wantErr     bool
	}{
		{name: "openvm v2.1.0-preview", zkvm: "openvm", zkvmVersion: "v2.1.0-preview", want: "0.19.0"},
		{name: "zisk v1.2.0-alpha", zkvm: "zisk", zkvmVersion: "v1.2.0-alpha", want: "0.18.0"},
		{name: "zisk v1.3.0-alpha", zkvm: "zisk", zkvmVersion: "v1.3.0-alpha", want: "77e2aae"},
		{name: "zisk v1.3.1-alpha", zkvm: "zisk", zkvmVersion: "v1.3.1-alpha", want: "0.19.0"},
		{name: "unknown version", zkvm: "zisk", zkvmVersion: "v1.4.0-alpha", wantErr: true},
		{name: "version without the v prefix", zkvm: "zisk", zkvmVersion: "1.3.0-alpha", wantErr: true},
		{name: "unknown zkvm", zkvm: "sp1", zkvmVersion: "v5.2.1", wantErr: true},
		{name: "tag replaces a mapped tag", zkvm: "zisk", zkvmVersion: "v1.3.0-alpha", ereTag: "0.18.0", want: "0.18.0"},
		{name: "tag of an unknown version", zkvm: "zisk", zkvmVersion: "v1.4.0-alpha", ereTag: "0.19.0", want: "0.19.0"},
		{name: "tag without a zkvm label", zkvmVersion: "v1.3.0-alpha", ereTag: "0.19.0", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ereServerTag(tc.zkvm, tc.zkvmVersion, tc.ereTag)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("tag = %q, %v, want %q, error %v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestRunRejectsArtifactOfAnotherImage(t *testing.T) {
	const name = "benchmark/example/test_input.py::test_ok[fork_Amsterdam]"
	results, runDir, _ := writeRun(t, `{"instance": {"extra_args": ["--elf=%s"]},
		"metadata": {"labels": {"zkvm": "zisk", "zkvm_version": "v1.3.0-alpha"}}}`, name)
	// Every test is estimated already, but under another image.
	estimated := &artifact{
		Image: "ghcr.io/eth-act/ere/ere-server-zisk:0.17.0",
		Tests: map[string]*ereserver.CostEstimation{name: {}},
	}
	path := artifactPath(results, "run")
	if err := writeArtifact(path, estimated); err != nil {
		t.Fatal(err)
	}
	err := Run(t.Context(), runDir, "", "", 1, io.Discard)
	if err == nil || !strings.Contains(err.Error(), path+" holds estimations of image") {
		t.Fatalf("Run = %v, want a rejection of the artifact", err)
	}
}

// writeRun writes the guest ELF and a run of the tests into a new results
// directory. The config.json of the run is config formatted with the ELF path.
func writeRun(t *testing.T, config string, names ...string) (results, runDir, elfPath string) {
	t.Helper()
	results = t.TempDir()
	runDir = filepath.Join(results, "runs", "run")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	elfPath = filepath.Join(results, "guest.elf")
	files := map[string]string{
		elfPath:                              "guest program",
		filepath.Join(runDir, "config.json"): fmt.Sprintf(config, elfPath),
		filepath.Join(runDir, "result.json"): `{"tests": {"` + strings.Join(names, `": {}, "`) + `": {}}}`,
	}
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return results, runDir, elfPath
}

func TestReuseEstimates(t *testing.T) {
	const (
		image     = "ghcr.io/eth-act/ere/ere-server-zisk:0.18.0"
		elfSHA256 = "8f2c"
		suiteHash = "f585281e3fea89a8"
	)
	names := []string{"first", "second", "third", "fourth"}
	results := t.TempDir()
	artifacts := map[string]*artifact{
		"same-guest": {
			SuiteHash: suiteHash, Image: image, ELFSHA256: elfSHA256,
			Tests:    map[string]*ereserver.CostEstimation{"first": {Cost: map[string]uint64{"rv64": 1}}, "second": {Cost: map[string]uint64{"rv64": 2}}, "foreign": {}},
			Failures: map[string]string{"third": "guest panicked"},
		},
		"other-elf": {
			SuiteHash: suiteHash, Image: image, ELFSHA256: "1b0d",
			Tests: map[string]*ereserver.CostEstimation{"fourth": {}},
		},
		"other-suite": {
			SuiteHash: "0c1d2e3f4a5b6c7d", Image: image, ELFSHA256: elfSHA256,
			Tests: map[string]*ereserver.CostEstimation{"fourth": {}},
		},
		// The estimate itself is skipped, so its fourth test stays pending.
		"run": {
			SuiteHash: suiteHash, Image: image, ELFSHA256: elfSHA256,
			Tests: map[string]*ereserver.CostEstimation{"second": {Cost: map[string]uint64{"rv64": 3}}, "fourth": {}},
		},
	}
	for id, estimated := range artifacts {
		if err := writeArtifact(artifactPath(results, id), estimated); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(results, estimatesDir, "index.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	result := &artifact{
		SuiteHash: suiteHash, Image: image, ELFSHA256: elfSHA256,
		Tests:    map[string]*ereserver.CostEstimation{"second": {Cost: map[string]uint64{"rv64": 3}}},
		Failures: map[string]string{},
	}
	reused, err := reuseEstimates(results, "run", names, result)
	if err != nil {
		t.Fatal(err)
	}
	if reused != 2 {
		t.Errorf("reused = %d, want 2", reused)
	}
	want := &artifact{
		SuiteHash: suiteHash, Image: image, ELFSHA256: elfSHA256,
		Tests:    map[string]*ereserver.CostEstimation{"first": {Cost: map[string]uint64{"rv64": 1}}, "second": {Cost: map[string]uint64{"rv64": 3}}},
		Failures: map[string]string{"third": "guest panicked"},
	}
	if !reflect.DeepEqual(result, want) {
		t.Errorf("artifact = %+v, want %+v", result, want)
	}
}

func TestRunReusesCompleteSibling(t *testing.T) {
	const name = "benchmark/example/test_input.py::test_ok[fork_Amsterdam]"
	results, runDir, _ := writeRun(t, `{"timestamp": 1790390784, "suite_hash": "f585281e3fea89a8",
		"instance": {"id": "reth-zisk", "client": "provoor", "extra_args": ["--elf=%s"]},
		"metadata": {"labels": {"zkvm": "zisk", "zkvm_version": "v1.3.0-alpha"}}}`, name)
	// The sibling is an earlier run of the same guest program.
	digest := sha256.Sum256([]byte("guest program"))
	estimated := &artifact{
		Timestamp: 1788831226,
		SuiteHash: "f585281e3fea89a8",
		Instance:  instance{ID: "reth-zisk", Client: "provoor"},
		Metadata:  config.MetadataConfig{Labels: map[string]string{"zkvm": "zisk", "zkvm_version": "v1.3.0-alpha"}},
		Zkvm:      "zisk",
		Image:     "ghcr.io/eth-act/ere/ere-server-zisk:77e2aae",
		ELFURL:    "guest.elf",
		ELFSHA256: hex.EncodeToString(digest[:]),
		Tests:     map[string]*ereserver.CostEstimation{name: {Cost: map[string]uint64{"rv64": 1}}},
		Failures:  map[string]string{},
	}
	if err := writeArtifact(artifactPath(results, "earlier-run"), estimated); err != nil {
		t.Fatal(err)
	}
	t.Chdir(results)
	var output bytes.Buffer
	if err := Run(t.Context(), runDir, "", "", 1, &output); err != nil {
		t.Fatal(err)
	}
	written, err := readArtifact(filepath.Join(results, estimatesDir, "run", artifactName))
	if err != nil {
		t.Fatal(err)
	}
	want := *estimated
	want.Timestamp = 1790390784
	want.Command = []string{"provoor", "estimate", filepath.Join("runs", "run"), "--ere-tag", "77e2aae", "--elf", "guest.elf"}
	if !reflect.DeepEqual(written, &want) {
		t.Errorf("artifact = %+v, want %+v", written, &want)
	}
	if !strings.Contains(output.String(), "reused 1 estimations") || !strings.Contains(output.String(), "nothing to do") {
		t.Errorf("output = %q, want the reuse and the nothing to do lines", output.String())
	}
}

func TestRunSavesNoArtifactBeforeEstimating(t *testing.T) {
	results, runDir, _ := writeRun(t, `{"suite_hash": "f585281e3fea89a8", "instance": {"extra_args": ["--elf=%s"]},
		"metadata": {"labels": {"zkvm": "zisk", "zkvm_version": "v1.3.0-alpha"}}}`, "first", "second")
	// The earlier run estimated the first test, and the suite has no summary.json.
	digest := sha256.Sum256([]byte("guest program"))
	estimated := &artifact{
		SuiteHash: "f585281e3fea89a8",
		Image:     "ghcr.io/eth-act/ere/ere-server-zisk:77e2aae",
		ELFSHA256: hex.EncodeToString(digest[:]),
		Tests:     map[string]*ereserver.CostEstimation{"first": {}},
	}
	if err := writeArtifact(artifactPath(results, "earlier-run"), estimated); err != nil {
		t.Fatal(err)
	}
	err := Run(t.Context(), runDir, "", "", 1, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "summary.json") {
		t.Fatalf("Run = %v, want the error of the missing suite summary", err)
	}
	if _, err := os.Stat(filepath.Join(results, estimatesDir, "run")); err == nil {
		t.Errorf("stat = %v, want no estimate directory", err)
	}
}

func TestRunRefreshesHeaderOfCompleteEstimate(t *testing.T) {
	const name = "benchmark/example/test_input.py::test_ok[fork_Amsterdam]"
	results, runDir, _ := writeRun(t, `{"timestamp": 1790390784, "instance": {"extra_args": ["--elf=%s"]},
		"metadata": {"labels": {"stateless_validator": "ethrex", "zkvm": "zisk", "zkvm_version": "v1.3.0-alpha"}}}`, name)
	// Every test is estimated already, under the header of an earlier
	// config.json.
	digest := sha256.Sum256([]byte("guest program"))
	estimated := &artifact{
		Timestamp: 1788831226,
		Metadata:  config.MetadataConfig{Labels: map[string]string{"stateless_validator": "reth", "zkvm": "zisk", "zkvm_version": "v1.3.0-alpha"}},
		Zkvm:      "zisk",
		Image:     "ghcr.io/eth-act/ere/ere-server-zisk:77e2aae",
		ELFURL:    "guest.elf",
		ELFSHA256: hex.EncodeToString(digest[:]),
		Tests:     map[string]*ereserver.CostEstimation{name: {Cost: map[string]uint64{"rv64": 1}}},
		Failures:  map[string]string{},
	}
	path := artifactPath(results, "run")
	if err := writeArtifact(path, estimated); err != nil {
		t.Fatal(err)
	}
	t.Chdir(results)
	var output bytes.Buffer
	if err := Run(t.Context(), runDir, "", "", 1, &output); err != nil {
		t.Fatal(err)
	}
	written, err := readArtifact(path)
	if err != nil {
		t.Fatal(err)
	}
	want := *estimated
	want.Timestamp = 1790390784
	want.Metadata.Labels = map[string]string{"stateless_validator": "ethrex", "zkvm": "zisk", "zkvm_version": "v1.3.0-alpha"}
	want.Command = []string{"provoor", "estimate", filepath.Join("runs", "run"), "--ere-tag", "77e2aae", "--elf", "guest.elf"}
	if !reflect.DeepEqual(written, &want) {
		t.Errorf("artifact = %+v, want %+v", written, &want)
	}
	if output.String() != "estimate: nothing to do\n" {
		t.Errorf("output = %q, want the nothing to do line", output.String())
	}
}

func TestRunReplacesELFOfRun(t *testing.T) {
	const name = "benchmark/example/test_input.py::test_ok[fork_Amsterdam]"
	// The config.json of the run names an ELF that is gone.
	results, runDir, elfPath := writeRun(t, `{"instance": {"extra_args": ["--elf=%s.gone"]},
		"metadata": {"labels": {"zkvm": "zisk", "zkvm_version": "v1.3.0-alpha"}}}`, name)
	digest := sha256.Sum256([]byte("guest program"))
	estimated := &artifact{
		Metadata:  config.MetadataConfig{Labels: map[string]string{"zkvm": "zisk", "zkvm_version": "v1.3.0-alpha"}},
		Zkvm:      "zisk",
		Image:     "ghcr.io/eth-act/ere/ere-server-zisk:77e2aae",
		ELFURL:    elfPath + ".gone",
		ELFSHA256: hex.EncodeToString(digest[:]),
		Tests:     map[string]*ereserver.CostEstimation{name: {Cost: map[string]uint64{"rv64": 1}}},
		Failures:  map[string]string{},
	}
	path := artifactPath(results, "run")
	if err := writeArtifact(path, estimated); err != nil {
		t.Fatal(err)
	}
	t.Chdir(results)
	if err := Run(t.Context(), runDir, "", elfPath, 1, io.Discard); err != nil {
		t.Fatal(err)
	}
	written, err := readArtifact(path)
	if err != nil {
		t.Fatal(err)
	}
	want := *estimated
	want.ELFURL = "guest.elf"
	want.Command = []string{"provoor", "estimate", filepath.Join("runs", "run"), "--ere-tag", "77e2aae", "--elf", "guest.elf"}
	if !reflect.DeepEqual(written, &want) {
		t.Errorf("artifact = %+v, want %+v", written, &want)
	}
}

func TestRunConfig(t *testing.T) {
	// Only the forwarder reads the coordinator address, so it stays unset.
	t.Setenv("COORDINATOR_IP", "")
	fixtures := writeFixtures(t)
	digest := sha256.Sum256([]byte("guest program"))
	const image = "ghcr.io/eth-act/ere/ere-server-zisk:77e2aae"
	cases := []struct {
		name       string
		filter     string
		instance   string
		continued  bool
		wantOutput string
	}{
		{
			name:       "continues the estimate of the instance",
			instance:   "reth-zisk",
			continued:  true,
			wantOutput: "estimate: %s\nestimate: reused 1 estimations of earlier estimates\nestimate: nothing to do\n",
		},
		{
			name:       "reuses the estimate of another instance",
			instance:   "ethrex-zisk",
			wantOutput: "estimate: %s\nestimate: reused 3 estimations of earlier estimates\nestimate: nothing to do\n",
		},
		{
			// No test passes the filter, so nothing is pending and no
			// ere-server starts.
			name:       "starts the first estimate of a results directory",
			filter:     "gas-value_60M",
			wantOutput: "estimate: %s\nestimate: nothing to do\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			results := t.TempDir()
			configPath := filepath.Join(results, "config.yaml")
			elfPath := filepath.Join(results, "guest.elf")
			if err := os.WriteFile(elfPath, []byte("guest program"), 0o644); err != nil {
				t.Fatal(err)
			}
			yaml := `runner:
  benchmark:
    results_dir: ` + results + `
    tests:
      filter: ` + tc.filter + `
      source:
        eest_fixtures:
          local_fixtures_dir: ` + fixtures + `
          fixtures_subdir: blockchain_tests
  client:
    config:
      metadata:
        labels:
          stateless_validator: reth
          zkvm: openvm
  instances:
    - id: reth-zisk
      client: provoor
      metadata:
        labels:
          zkvm: zisk
          zkvm_version: v1.3.0-alpha
      extra_args:
        - --elf=` + elfPath + `
        - --coordinator-endpoint=http://${COORDINATOR_IP}:7000
`
			if err := os.WriteFile(configPath, []byte(yaml), 0o644); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(configPath)
			if err != nil {
				t.Fatal(err)
			}
			suite, err := prepareSuite(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			tests := map[string]*ereserver.CostEstimation{}
			for _, name := range suite.names {
				tests[name] = &ereserver.CostEstimation{Cost: map[string]uint64{"main": 1}}
			}
			seededID := "1790390784_ccecd858_" + tc.instance
			seeded := &artifact{
				Timestamp: 1790390784,
				SuiteHash: suite.hash,
				Instance:  instance{ID: tc.instance, Client: "provoor"},
				Metadata:  config.MetadataConfig{Labels: map[string]string{"zkvm": "zisk"}},
				Zkvm:      "zisk",
				Image:     image,
				ELFURL:    elfPath,
				ELFSHA256: hex.EncodeToString(digest[:]),
				Tests:     tests,
				Failures:  map[string]string{},
			}
			seeds := map[string]*artifact{}
			if tc.instance != "" {
				seeds[seededID] = seeded
			}
			if tc.continued {
				// The continued estimate lacks one test that a donor of another
				// instance holds, so the command rewrites the continued artifact.
				// The command continues the later of two estimates of the instance.
				partial, donor, older := *seeded, *seeded, *seeded
				partial.Tests = maps.Clone(tests)
				delete(partial.Tests, suite.names[0])
				donor.Timestamp, donor.Instance.ID = 1780000000, "ethrex-zisk"
				older.Timestamp = 1785000000
				seeds = map[string]*artifact{seededID: &partial, "1780000000_aaaaaaaa_ethrex-zisk": &donor, "1785000000_bbbbbbbb_reth-zisk": &older}
			}
			for id, estimated := range seeds {
				if err := writeArtifact(artifactPath(results, id), estimated); err != nil {
					t.Fatal(err)
				}
			}

			t.Chdir(results)
			var output bytes.Buffer
			if err := Run(t.Context(), configPath, "", "", 1, &output); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(filepath.Join(results, estimatesDir))
			if err != nil {
				t.Fatal(err)
			}
			var created []string
			for _, entry := range entries {
				if seeds[entry.Name()] == nil {
					created = append(created, entry.Name())
				}
			}
			id, want := seededID, seeded
			if tc.continued && len(created) > 0 {
				t.Fatalf("created %v, want no new estimate", created)
			}
			if !tc.continued {
				if len(created) != 1 {
					t.Fatalf("created %v, want one new estimate", created)
				}
				id = created[0]
				match := regexp.MustCompile(`^(\d+)_[0-9a-f]{8}_reth-zisk$`).FindStringSubmatch(id)
				if match == nil {
					t.Fatalf("estimate id %s is not in the run id format", id)
				}
				timestamp, err := strconv.ParseInt(match[1], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				want = &artifact{
					Timestamp: timestamp,
					SuiteHash: suite.hash,
					Instance:  instance{ID: "reth-zisk", Client: "provoor"},
					Metadata: config.MetadataConfig{Labels: map[string]string{
						"stateless_validator": "reth", "zkvm": "zisk", "zkvm_version": "v1.3.0-alpha",
					}},
					Zkvm:      "zisk",
					Image:     image,
					ELFURL:    "guest.elf",
					ELFSHA256: hex.EncodeToString(digest[:]),
					Tests:     tests,
					Failures:  map[string]string{},
				}
			}
			written, err := readArtifact(artifactPath(results, id))
			if err != nil {
				t.Fatal(err)
			}
			expected := *want
			expected.Command = []string{"provoor", "estimate", "config.yaml", "--ere-tag", "77e2aae"}
			if !reflect.DeepEqual(written, &expected) {
				t.Errorf("artifact = %+v, want %+v", written, &expected)
			}
			if wantOutput := fmt.Sprintf(tc.wantOutput, id); output.String() != wantOutput {
				t.Errorf("output = %q, want %q", output.String(), wantOutput)
			}
		})
	}
}

func TestRunConfigRequiresEESTFixtures(t *testing.T) {
	// The instance has no --elf argument, so the error shows that the
	// eest_fixtures check comes first.
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("runner:\n  instances:\n    - id: reth-zisk\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := Run(t.Context(), configPath, "", "", 1, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "eest_fixtures is not set") {
		t.Fatalf("Run = %v, want the error of the missing eest_fixtures source", err)
	}
}

func TestRunConfigRejectsELF(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("runner:\n  instances:\n    - id: reth-zisk\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := Run(t.Context(), configPath, "", "guest.elf", 1, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "--elf applies only to a run directory") {
		t.Fatalf("Run = %v, want the rejection of --elf", err)
	}
}

func TestLink(t *testing.T) {
	const id = "1790500000_aaaaaaaa_reth-zisk"
	results, runDir, _ := writeRun(t, `{"suite_hash": "f585281e3fea89a8", "instance": {"extra_args": ["--elf=%s"]},
		"metadata": {"labels": {"zkvm": "zisk", "zkvm_version": "v1.3.0-alpha"}}}`, "first")
	// The estimate of a configuration has the suite and ELF of the run.
	digest := sha256.Sum256([]byte("guest program"))
	estimated := &artifact{
		Timestamp: 1790500000,
		SuiteHash: "f585281e3fea89a8",
		Instance:  instance{ID: "reth-zisk", Client: "provoor"},
		Zkvm:      "zisk",
		Image:     "ghcr.io/eth-act/ere/ere-server-zisk:77e2aae",
		ELFSHA256: hex.EncodeToString(digest[:]),
		Tests:     map[string]*ereserver.CostEstimation{"first": {Cost: map[string]uint64{"main": 1}}},
	}
	if err := writeArtifact(artifactPath(results, id), estimated); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(artifactPath(results, id))
	if err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(results, estimatesDir, "index.json")
	if err := os.WriteFile(indexPath, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := Link(t.Context(), runDir, filepath.Join(results, estimatesDir, id), &output); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(results, estimatesDir, id)); err == nil {
		t.Errorf("stat = %v, want no estimate under the old id", err)
	}
	linked, err := os.ReadFile(artifactPath(results, "run"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(linked, written) {
		t.Errorf("artifact = %s, want %s", linked, written)
	}
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	var index executor.EstimateIndex
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatal(err)
	}
	if len(index.Entries) != 1 || index.Entries[0].EstimateID != "run" {
		t.Errorf("index = %s, want one entry with the run id", data)
	}
	if want := "estimate: linked " + id + " to run run\n"; output.String() != want {
		t.Errorf("output = %q, want %q", output.String(), want)
	}
}

func TestLinkRejects(t *testing.T) {
	const (
		id        = "1790500000_aaaaaaaa_reth-zisk"
		suiteHash = "f585281e3fea89a8"
	)
	digest := sha256.Sum256([]byte("guest program"))
	elfSHA256 := hex.EncodeToString(digest[:])
	cases := []struct {
		name      string
		suiteHash string
		elfSHA256 string
		taken     bool
		outside   bool
		wantErr   string
	}{
		{name: "other suite", suiteHash: "0c1d2e3f4a5b6c7d", elfSHA256: elfSHA256, wantErr: "suite 0c1d2e3f4a5b6c7d, not of suite " + suiteHash},
		{name: "other elf", suiteHash: suiteHash, elfSHA256: "1b0d", wantErr: "elf 1b0d, not of elf " + elfSHA256},
		{name: "existing destination", suiteHash: suiteHash, elfSHA256: elfSHA256, taken: true, wantErr: "exists already"},
		{name: "estimate outside the results directory of the run", suiteHash: suiteHash, elfSHA256: elfSHA256, outside: true, wantErr: "is not in"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			results, runDir, _ := writeRun(t, `{"suite_hash": "f585281e3fea89a8", "instance": {"extra_args": ["--elf=%s"]},
				"metadata": {"labels": {"zkvm": "zisk"}}}`, "first")
			estimatesParent := results
			if tc.outside {
				estimatesParent = t.TempDir()
			}
			path := artifactPath(estimatesParent, id)
			estimated := &artifact{SuiteHash: tc.suiteHash, ELFSHA256: tc.elfSHA256, Tests: map[string]*ereserver.CostEstimation{"first": {}}}
			if err := writeArtifact(path, estimated); err != nil {
				t.Fatal(err)
			}
			if tc.taken {
				if err := os.MkdirAll(filepath.Join(results, estimatesDir, "run"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			err := Link(t.Context(), runDir, filepath.Dir(path), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Link = %v, want an error with %q", err, tc.wantErr)
			}
			if _, err := os.Stat(path); err != nil {
				t.Errorf("stat = %v, want the estimate in place", err)
			}
		})
	}
}

func TestInvocation(t *testing.T) {
	workingDir := t.TempDir()
	t.Chdir(workingDir)
	cases := []struct {
		target string
		want   string
	}{
		{target: "provoor-runs/results/runs/run/", want: "provoor-runs/results/runs/run"},
		{target: "../benchmarkoor/examples/provoor/zisk.example.yaml", want: "../benchmarkoor/examples/provoor/zisk.example.yaml"},
		// An absolute target would put a home path into the artifact.
		{target: filepath.Join(workingDir, "results", "runs", "run"), want: filepath.Join("results", "runs", "run")},
	}
	for _, tc := range cases {
		if got, want := invocation(tc.target, "77e2aae"), []string{"provoor", "estimate", tc.want, "--ere-tag", "77e2aae"}; !slices.Equal(got, want) {
			t.Errorf("invocation(%q) = %q, want %q", tc.target, got, want)
		}
	}
	// A URL stays as it is, since filepath.Clean turns https:// into https:/.
	const source = "https://example.com/guest.elf"
	if got := relativeToWorkingDir(source); got != source {
		t.Errorf("relativeToWorkingDir(%q) = %q, want it unchanged", source, got)
	}
}

func TestWriteArtifact(t *testing.T) {
	const (
		header = `{
  "timestamp": 0,
  "suite_hash": "",
  "instance": {
    "id": "",
    "client": ""
  },
  "metadata": {},
  "zkvm": "zisk",
  "image": "ghcr.io/eth-act/ere/ere-server-zisk:77e2aae",
`
		recorded = `  "image_sha256": "sha256:8f2c",
  "command": [
    "provoor",
    "estimate",
    "provoor-runs/results/runs/run"
  ],
`
		tail = `  "elf_url": "",
  "elf_sha256": "",
  "tests": {}
}
`
	)
	cases := []struct {
		name        string
		imageSHA256 string
		command     []string
		want        string
	}{
		{name: "without the image digest and the command", want: header + tail},
		{name: "with the image digest and the command", imageSHA256: "sha256:8f2c", command: []string{"provoor", "estimate", "provoor-runs/results/runs/run"}, want: header + recorded + tail},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), artifactName)
			result := &artifact{
				Zkvm: "zisk", Image: "ghcr.io/eth-act/ere/ere-server-zisk:77e2aae",
				ImageSHA256: tc.imageSHA256, Command: tc.command,
				Tests: map[string]*ereserver.CostEstimation{},
			}
			if err := writeArtifact(path, result); err != nil {
				t.Fatal(err)
			}
			written, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(written) != tc.want {
				t.Errorf("artifact = %s, want %s", written, tc.want)
			}
		})
	}
}
