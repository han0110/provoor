package estimate

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ethpandaops/benchmarkoor/pkg/eest"
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
			config: `{"suite_hash": "f585281e3fea89a8",
				"instance": {"extra_args": ["--zkvm=openvm", "--elf=https://example.com/guest.elf"]},
				"metadata": {"labels": {"zkvm": "openvm"}}}`,
			want: &runConfig{Zkvm: "openvm", ELFSource: "https://example.com/guest.elf", SuiteHash: "f585281e3fea89a8"},
		},
		{
			name: "elf as two arguments",
			config: `{"suite_hash": "f585281e3fea89a8",
				"instance": {"extra_args": ["--elf", "/guests/guest.elf", "--zkvm=zisk"]},
				"metadata": {"labels": {"zkvm": "zisk"}}}`,
			want: &runConfig{Zkvm: "zisk", ELFSource: "/guests/guest.elf", SuiteHash: "f585281e3fea89a8"},
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
