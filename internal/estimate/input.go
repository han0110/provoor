package estimate

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/ethpandaops/benchmarkoor/pkg/config"
	"github.com/ethpandaops/benchmarkoor/pkg/eest"
	"github.com/ethpandaops/benchmarkoor/pkg/executor"
	"github.com/sirupsen/logrus"
)

// runConfig is what a run's config.json says about the guest program it
// benchmarked and the fixtures it ran.
type runConfig struct {
	Zkvm      string
	ELFSource string
	SuiteHash string
}

// readRunConfig reads the run's config.json.
func readRunConfig(runDir string) (*runConfig, error) {
	data, err := os.ReadFile(filepath.Join(runDir, "config.json"))
	if err != nil {
		return nil, err
	}
	return parseRunConfig(data)
}

// parseRunConfig takes the zkVM, the guest ELF source, and the suite hash of a
// run from its config.json.
func parseRunConfig(data []byte) (*runConfig, error) {
	var parsed struct {
		SuiteHash string `json:"suite_hash"`
		Instance  struct {
			ExtraArgs []string `json:"extra_args"`
		} `json:"instance"`
		Metadata struct {
			Labels struct {
				Zkvm string `json:"zkvm"`
			} `json:"labels"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}
	if parsed.Metadata.Labels.Zkvm == "" {
		return nil, errors.New("metadata.labels holds no zkvm label")
	}
	source, err := elfSource(parsed.Instance.ExtraArgs)
	if err != nil {
		return nil, err
	}
	return &runConfig{
		Zkvm:      parsed.Metadata.Labels.Zkvm,
		ELFSource: source,
		SuiteHash: parsed.SuiteHash,
	}, nil
}

// elfSource is the value of the --elf argument the run served, written either
// as one argument or as two.
func elfSource(args []string) (string, error) {
	for i, arg := range args {
		if source, ok := strings.CutPrefix(arg, "--elf="); ok {
			return source, nil
		}
		if arg == "--elf" && i+1 < len(args) {
			return args[i+1], nil
		}
	}
	return "", errors.New("instance.extra_args holds no --elf argument")
}

// readTestNames reads the names of the tests the run measured.
func readTestNames(runDir string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(runDir, "result.json"))
	if err != nil {
		return nil, err
	}
	var result executor.RunResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(result.Tests)), nil
}

// prepareFixtures returns the directory holding the suite's fixtures, after
// downloading them into the benchmarkoor cache when they are not there yet.
func prepareFixtures(ctx context.Context, runDir, suiteHash string) (string, error) {
	data, err := os.ReadFile(filepath.Join(runDir, "..", "..", "suites", suiteHash, "summary.json"))
	if err != nil {
		return "", err
	}
	var summary struct {
		Source struct {
			EEST *executor.EESTSourceInfo `json:"eest"`
		} `json:"source"`
	}
	if err := json.Unmarshal(data, &summary); err != nil {
		return "", err
	}
	if summary.Source.EEST == nil {
		return "", fmt.Errorf("suite %s was not filled from EEST fixtures", suiteHash)
	}
	cacheDir, err := (&config.Config{}).ResolveCacheDir()
	if err != nil {
		return "", err
	}
	// The download reports its progress on its own logger, which this command
	// does not print.
	log := logrus.New()
	log.SetOutput(io.Discard)
	source := executor.NewEESTSource(log, &config.EESTFixturesSource{
		GitHubRepo:     summary.Source.EEST.GitHubRepo,
		GitHubRelease:  summary.Source.EEST.GitHubRelease,
		FixturesURL:    summary.Source.EEST.FixturesURL,
		FixturesSubdir: summary.Source.EEST.FixturesSubdir,
	}, cacheDir, nil, "")
	prepared, err := source.Prepare(ctx)
	if err != nil {
		return "", err
	}
	return prepared.BasePath, nil
}

// walkFixtures parses every fixture file under dir once and sends the input of
// each pending test. One file holds several fixtures, keyed by the pytest node
// id the test name comes from.
func walkFixtures(ctx context.Context, dir string, pending map[string]struct{}, jobs chan<- job) error {
	missing := maps.Clone(pending)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".json") {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fixtures, err := eest.ParseFixtureFile(data)
		if err != nil {
			// The directory also carries files of other shapes, such as the
			// genesis groups of a stateful fill.
			return nil
		}
		for key, fixture := range fixtures {
			name := strings.TrimPrefix(key, "tests/")
			if _, ok := missing[name]; !ok {
				continue
			}
			stdin, err := statelessInput(fixture)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			delete(missing, name)
			select {
			case jobs <- job{name: name, stdin: stdin}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s holds no fixture for %d tests, such as %s",
			dir, len(missing), slices.Sorted(maps.Keys(missing))[0])
	}
	return nil
}

// errNoBlocks reports a fixture that holds no block to take an input from.
var errNoBlocks = errors.New("fixture has no blocks")

// statelessInput is the guest input of a fixture's benchmark block, which is
// its last one. The bytes reach the zkVM as they are, since the framing is the
// prover's own.
func statelessInput(fixture *eest.Fixture) ([]byte, error) {
	if len(fixture.Blocks) == 0 {
		return nil, errNoBlocks
	}
	input := fixture.Blocks[len(fixture.Blocks)-1].StatelessInputBytes
	if input == "" || input == "0x" {
		return nil, eest.ErrNoStatelessInput
	}
	return hex.DecodeString(strings.TrimPrefix(input, "0x"))
}
