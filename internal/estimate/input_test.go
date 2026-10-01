package estimate

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/ethpandaops/benchmarkoor/pkg/config"
	"github.com/ethpandaops/benchmarkoor/pkg/eest"
	"github.com/ethpandaops/benchmarkoor/pkg/executor"
)

func TestParseRunConfig(t *testing.T) {
	cases := []struct {
		name    string
		config  string
		want    *runConfig
		wantErr bool
	}{
		{
			name: "elf as one argument",
			config: `{"timestamp": 1788880286, "suite_hash": "f585281e3fea89a8",
				"instance": {"id": "reth-openvm", "client": "provoor", "extra_args": ["--zkvm=openvm", "--elf=https://example.com/guest.elf"]},
				"metadata": {"labels": {"zkvm": "openvm", "zkvm_version": "v2.1.0-preview"}}}`,
			want: &runConfig{
				Timestamp: 1788880286,
				SuiteHash: "f585281e3fea89a8",
				Instance:  instance{ID: "reth-openvm", Client: "provoor"},
				Metadata:  config.MetadataConfig{Labels: map[string]string{"zkvm": "openvm", "zkvm_version": "v2.1.0-preview"}},
				ELFSource: "https://example.com/guest.elf",
			},
		},
		{
			name: "elf as two arguments",
			config: `{"suite_hash": "f585281e3fea89a8",
				"instance": {"extra_args": ["--elf", "/guests/guest.elf", "--zkvm=zisk"]},
				"metadata": {"labels": {"zkvm": "zisk"}}}`,
			want: &runConfig{
				SuiteHash: "f585281e3fea89a8",
				Metadata:  config.MetadataConfig{Labels: map[string]string{"zkvm": "zisk"}},
				ELFSource: "/guests/guest.elf",
			},
		},
		{
			name: "no zkvm label",
			config: `{"instance": {"extra_args": ["--elf=https://example.com/guest.elf"]},
				"metadata": {"labels": {"stateless_validator": "reth"}}}`,
			wantErr: true,
		},
		{
			name: "no elf argument",
			config: `{"instance": {"extra_args": ["--zkvm=openvm"]},
				"metadata": {"labels": {"zkvm": "openvm"}}}`,
			wantErr: true,
		},
		{
			name: "elf without a value",
			config: `{"instance": {"extra_args": ["--elf"]},
				"metadata": {"labels": {"zkvm": "openvm"}}}`,
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseRunConfig([]byte(tc.config))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("config = %+v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("config = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestStatelessInput(t *testing.T) {
	cases := []struct {
		name    string
		blocks  string
		want    []byte
		wantErr error
	}{
		{
			name: "last block wins",
			blocks: `[{"statelessInputBytes": "0xaabb"},
				{"statelessInputBytes": "0xccdd"}]`,
			want: []byte{0xcc, 0xdd},
		},
		{
			name:    "no blocks",
			blocks:  `[]`,
			wantErr: errNoBlocks,
		},
		{
			name:    "empty input",
			blocks:  `[{"statelessInputBytes": "0x"}]`,
			wantErr: eest.ErrNoStatelessInput,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixtures, err := eest.ParseFixtureFile([]byte(fixtureFile(tc.blocks)))
			if err != nil {
				t.Fatal(err)
			}
			got, err := statelessInput(fixtures[fixtureName])
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("input = %x, want %x", got, tc.want)
			}
		})
	}
}

// fixtureName is the pytest node id of the fixture the tests parse, which the
// test name drops the leading directory of.
const fixtureName = "tests/benchmark/example/test_input.py::test_input[fork_Amsterdam]"

// fixtureFile is one fixture file holding one blockchain-test fixture.
func fixtureFile(blocks string) string {
	return `{"` + fixtureName + `": {"_info": {"fixture-format": "blockchain_test"}, "blocks": ` + blocks + `}}`
}

func TestPrepareSuite(t *testing.T) {
	fixtures := writeFixtures(t)
	cases := []struct {
		name   string
		filter string
		want   []string
	}{
		{name: "no filter", want: suiteTests},
		{name: "substring filter", filter: "gas-value_10M", want: []string{suiteTests[0], suiteTests[2]}},
		{name: "regex filter", filter: "regex:test_input.*30M", want: []string{suiteTests[1]}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			results := t.TempDir()
			cfg := &config.Config{Runner: config.RunnerConfig{Benchmark: config.BenchmarkConfig{
				ResultsDir: results,
				Tests: config.TestsConfig{
					Filter: tc.filter,
					Source: config.SourceConfig{EESTFixtures: &config.EESTFixturesSource{
						LocalFixturesDir: fixtures,
						FixturesSubdir:   "blockchain_tests",
					}},
				},
			}}}
			suite, err := prepareSuite(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(suite.names, tc.want) {
				t.Errorf("names = %v, want %v", suite.names, tc.want)
			}
			if suite.fixtures != filepath.Join(fixtures, "blockchain_tests") {
				t.Errorf("fixtures = %s, want the blockchain_tests directory", suite.fixtures)
			}
			if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(suite.hash) {
				t.Errorf("hash = %q, want 16 hex digits", suite.hash)
			}
			data, err := os.ReadFile(filepath.Join(results, "suites", suite.hash, "summary.json"))
			if err != nil {
				t.Fatal(err)
			}
			var summary executor.SuiteInfo
			if err := json.Unmarshal(data, &summary); err != nil {
				t.Fatal(err)
			}
			summarized := make([]string, len(summary.Tests))
			for i, test := range summary.Tests {
				summarized[i] = test.Name
			}
			if summary.Hash != suite.hash || summary.Filter != tc.filter || !slices.Equal(summarized, tc.want) {
				t.Errorf("summary holds hash %s, filter %q, and tests %v, want %s, %q, and %v",
					summary.Hash, summary.Filter, summarized, suite.hash, tc.filter, tc.want)
			}
		})
	}
}

// suiteTests are the tests writeFixtures writes, in name order.
var suiteTests = []string{
	"benchmark/example/test_input.py::test_input[fork_Amsterdam-blockchain_test-benchmark-gas-value_10M]",
	"benchmark/example/test_input.py::test_input[fork_Amsterdam-blockchain_test-benchmark-gas-value_30M]",
	"benchmark/example/test_other.py::test_other[fork_Amsterdam-blockchain_test-benchmark-gas-value_10M]",
}

// writeFixtures writes a local EEST fixtures directory whose blockchain_tests
// directory holds a stateless fixture for each test of suiteTests.
func writeFixtures(t *testing.T) string {
	t.Helper()
	fixtures := make([]string, len(suiteTests))
	for i, name := range suiteTests {
		fixtures[i] = fmt.Sprintf(`"tests/%s": {"_info": {"fixture-format": "blockchain_test"},
			"blocks": [{"blockHeader": {"number": "0x1", "gasUsed": "0x1", "hash": "0x0%d"},
			"statelessInputBytes": "0xccdd", "statelessOutputBytes": "0x01"}]}`, name, i)
	}
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "blockchain_tests"), 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "blockchain_tests", "example.json")
	if err := os.WriteFile(file, []byte("{"+strings.Join(fixtures, ",")+"}"), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestEESTSourceConfig(t *testing.T) {
	info := &executor.EESTSourceInfo{
		FixturesSubdir:        "blockchain_tests",
		R2BucketURL:           "https://example.r2.dev/devnets/devnet-8",
		R2BucketStartingBlock: 100000,
		R2BucketBlocks:        1000,
	}
	got := eestSourceConfig(info)
	if !got.UseR2Bucket() || got.R2BucketStartingBlock != 100000 || got.R2BucketBlocks != 1000 {
		t.Fatalf("r2 bucket fields dropped: %+v", got)
	}
	if got.FixturesSubdir != "blockchain_tests" {
		t.Fatalf("fixtures subdir dropped: %+v", got)
	}
}
