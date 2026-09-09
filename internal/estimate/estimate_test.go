package estimate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

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
			err := checkResumable(tc.result, image, elfSHA256)
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

func TestRunRejectsArtifactOfAnotherImage(t *testing.T) {
	const name = "benchmark/example/test_input.py::test_ok[fork_Amsterdam]"
	runDir := t.TempDir()
	elfPath := filepath.Join(runDir, "guest.elf")
	files := map[string]string{
		elfPath: "guest program",
		filepath.Join(runDir, "config.json"): `{"instance": {"extra_args": ["--elf=` + elfPath + `"]},
			"metadata": {"labels": {"zkvm": "zisk"}}}`,
		filepath.Join(runDir, "result.json"): `{"tests": {"` + name + `": {}}}`,
	}
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Every test is estimated already, but under another image.
	estimated := &artifact{
		Image: "ghcr.io/eth-act/ere/ere-server-zisk:0.17.0",
		Tests: map[string]*ereserver.CostEstimation{name: {}},
	}
	if err := writeArtifact(filepath.Join(runDir, artifactName), estimated); err != nil {
		t.Fatal(err)
	}
	err := Run(t.Context(), runDir, "", 1, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "holds estimations of image") {
		t.Fatalf("Run = %v, want a rejection of the artifact", err)
	}
}

func TestReuseSiblings(t *testing.T) {
	const (
		image     = "ghcr.io/eth-act/ere/ere-server-zisk:0.18.0"
		elfSHA256 = "8f2c"
	)
	names := []string{"first", "second", "third", "fourth"}
	results := t.TempDir()
	runDir := filepath.Join(results, "run")
	artifacts := map[string]*artifact{
		"same-guest": {
			Image: image, ELFSHA256: elfSHA256,
			Tests:    map[string]*ereserver.CostEstimation{"first": {Cost: map[string]uint64{"rv64": 1}}, "second": {Cost: map[string]uint64{"rv64": 2}}, "foreign": {}},
			Failures: map[string]string{"third": "guest panicked"},
		},
		"other-elf": {
			Image: image, ELFSHA256: "1b0d",
			Tests: map[string]*ereserver.CostEstimation{"fourth": {}},
		},
		"run": {
			Image: image, ELFSHA256: elfSHA256,
			Tests: map[string]*ereserver.CostEstimation{"second": {Cost: map[string]uint64{"rv64": 3}}},
		},
	}
	for name, estimated := range artifacts {
		dir := filepath.Join(results, name)
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := writeArtifact(filepath.Join(dir, artifactName), estimated); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(results, "index.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	result := artifacts["run"]
	result.Failures = map[string]string{}
	reused, err := reuseSiblings(runDir, names, result, image, elfSHA256)
	if err != nil {
		t.Fatal(err)
	}
	if reused != 2 {
		t.Errorf("reused = %d, want 2", reused)
	}
	want := &artifact{
		Image: image, ELFSHA256: elfSHA256,
		Tests:    map[string]*ereserver.CostEstimation{"first": {Cost: map[string]uint64{"rv64": 1}}, "second": {Cost: map[string]uint64{"rv64": 3}}},
		Failures: map[string]string{"third": "guest panicked"},
	}
	if !reflect.DeepEqual(result, want) {
		t.Errorf("artifact = %+v, want %+v", result, want)
	}
}

func TestRunReusesCompleteSibling(t *testing.T) {
	const name = "benchmark/example/test_input.py::test_ok[fork_Amsterdam]"
	results := t.TempDir()
	runDir := filepath.Join(results, "run")
	sibling := filepath.Join(results, "earlier-run")
	for _, dir := range []string{runDir, sibling} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	elfPath := filepath.Join(results, "guest.elf")
	files := map[string]string{
		elfPath: "guest program",
		filepath.Join(runDir, "config.json"): `{"instance": {"extra_args": ["--elf=` + elfPath + `"]},
			"metadata": {"labels": {"zkvm": "zisk"}}}`,
		filepath.Join(runDir, "result.json"): `{"tests": {"` + name + `": {}}}`,
	}
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	digest := sha256.Sum256([]byte("guest program"))
	estimated := &artifact{
		Zkvm:      "zisk",
		Image:     ereserver.Image("zisk", ereVersion),
		ELFURL:    elfPath,
		ELFSHA256: hex.EncodeToString(digest[:]),
		Tests:     map[string]*ereserver.CostEstimation{name: {Cost: map[string]uint64{"rv64": 1}}},
		Failures:  map[string]string{},
	}
	if err := writeArtifact(filepath.Join(sibling, artifactName), estimated); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := Run(t.Context(), runDir, "", 1, &output); err != nil {
		t.Fatal(err)
	}
	written, err := readArtifact(filepath.Join(runDir, artifactName))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(written, estimated) {
		t.Errorf("artifact = %+v, want %+v", written, estimated)
	}
	if !strings.Contains(output.String(), "reused 1 estimations") || !strings.Contains(output.String(), "nothing to do") {
		t.Errorf("output = %q, want the reuse and the nothing to do lines", output.String())
	}
}
