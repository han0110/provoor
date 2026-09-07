// Package estimate estimates what proving every test of a benchmark run
// costs, by executing the run's guest program on each test's input in an
// ere-server.
package estimate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/han0110/provoor/internal/cluster"
	"github.com/han0110/provoor/internal/ereserver"
)

const (
	// ereVersion is the ere release whose server estimates the cost. It is the
	// release scripts/fetch-verifier.sh pins the proof verifier to.
	ereVersion = "0.18.0"
	// artifactName is the estimation artifact of one run.
	artifactName = "result.estimate.json"
	// progressInterval is how many estimations pass between two saves of the
	// artifact, so an interrupted command resumes near where it stopped.
	progressInterval = 50
)

// artifact holds the estimations of one run, keyed by the test names of
// result.json.
type artifact struct {
	Zkvm      string                               `json:"zkvm"`
	Image     string                               `json:"image"`
	ELFURL    string                               `json:"elf_url"`
	ELFSHA256 string                               `json:"elf_sha256"`
	Tests     map[string]*ereserver.CostEstimation `json:"tests"`
	Failures  map[string]string                    `json:"failures,omitempty"`
}

// job is one test's input, on its way to a worker.
type job struct {
	name  string
	stdin []byte
}

// outcome is what one test cost, or the guest failure that leaves it without
// a cost.
type outcome struct {
	name  string
	cost  *ereserver.CostEstimation
	guest *ereserver.GuestError
}

// Run estimates the cost of every test of the run that is not estimated yet
// and writes the result next to the run's own results. An empty image selects
// the ere-server of the run's zkVM.
func Run(ctx context.Context, runDir string, image string, concurrency int, output io.Writer) error {
	config, err := readRunConfig(runDir)
	if err != nil {
		return err
	}
	names, err := readTestNames(runDir)
	if err != nil {
		return err
	}
	path := filepath.Join(runDir, artifactName)
	result, err := readArtifact(path)
	if err != nil {
		return err
	}
	if image == "" {
		image = ereserver.Image(config.Zkvm, ereVersion)
	}
	elf, err := cluster.ResolveSource(ctx, config.ELFSource)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(elf)
	elfSHA256 := hex.EncodeToString(digest[:])
	if err := checkResumable(result, image, elfSHA256); err != nil {
		return err
	}
	pending := pendingTests(names, result)
	if len(pending) == 0 {
		fmt.Fprintln(output, "estimate: nothing to do")
		return nil
	}
	result.Zkvm, result.Image = config.Zkvm, image
	result.ELFURL, result.ELFSHA256 = config.ELFSource, elfSHA256

	fixtures, err := prepareFixtures(ctx, runDir, config.SuiteHash)
	if err != nil {
		return err
	}
	hosts, err := cluster.DialHosts(ctx, []string{""})
	if err != nil {
		return err
	}
	defer hosts.Close()
	out := cluster.NewOutput(output)
	if err := hosts.Pull(ctx, image, out); err != nil {
		return err
	}
	server, err := ereserver.Start(ctx, hosts.Client(""), image, elf, out)
	if err != nil {
		return err
	}
	defer func() {
		if err := server.Close(ctx); err != nil {
			out.Printf("removing %s: %v", server.Name(), err)
		}
	}()

	out.Printf("estimating %d tests with %s", len(pending), image)
	return estimateAll(ctx, &server.Client, fixtures, pending, concurrency, result, path, out)
}

// estimateAll estimates every pending test against one server and saves the
// artifact as it goes. A guest failure is recorded and the run continues,
// while every other failure ends it.
func estimateAll(ctx context.Context, client *ereserver.Client, fixtures string, pending map[string]struct{},
	concurrency int, result *artifact, path string, output *cluster.Output,
) error {
	total := len(pending)
	jobs := make(chan job)
	outcomes := make(chan outcome)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer close(jobs)
		return walkFixtures(ctx, fixtures, pending, jobs)
	})
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for range concurrency {
		group.Go(func() error {
			defer workers.Done()
			for input := range jobs {
				cost, err := client.ExecuteEstimatedCost(ctx, input.stdin)
				var guest *ereserver.GuestError
				if err != nil && !errors.As(err, &guest) {
					return fmt.Errorf("estimating %s: %w", input.name, err)
				}
				outcomes <- outcome{name: input.name, cost: cost, guest: guest}
			}
			return nil
		})
	}
	go func() {
		workers.Wait()
		close(outcomes)
	}()

	// The loop drains every outcome, so a worker never blocks on its send.
	estimated, failed := 0, 0
	var saveErr error
	for done := range outcomes {
		record(result, done)
		estimated++
		if done.guest != nil {
			failed++
		}
		if saveErr != nil || estimated%progressInterval != 0 {
			continue
		}
		output.Printf("estimated %d/%d", estimated, total)
		if saveErr = writeArtifact(path, result); saveErr != nil {
			cancel()
		}
	}
	err := group.Wait()
	if saveErr != nil {
		return saveErr
	}
	if writeErr := writeArtifact(path, result); err == nil {
		err = writeErr
	}
	if err != nil {
		return err
	}
	output.Printf("estimated %d tests, %d failed", estimated, failed)
	return nil
}

// record files one outcome, dropping the failure an earlier attempt recorded.
func record(result *artifact, done outcome) {
	if done.guest != nil {
		result.Failures[done.name] = done.guest.Message
		return
	}
	result.Tests[done.name] = done.cost
	delete(result.Failures, done.name)
}

// pendingTests are the tests the artifact holds no cost for. A recorded
// failure is estimated again, since a new server can succeed where an earlier
// one failed.
func pendingTests(names []string, result *artifact) map[string]struct{} {
	pending := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, ok := result.Tests[name]; !ok {
			pending[name] = struct{}{}
		}
	}
	return pending
}

// checkResumable rejects an artifact an earlier command filled under another
// image or another ELF, since its costs belong to that guest program.
func checkResumable(result *artifact, image, elfSHA256 string) error {
	if len(result.Tests) == 0 && len(result.Failures) == 0 {
		return nil
	}
	if result.Image == image && result.ELFSHA256 == elfSHA256 {
		return nil
	}
	return fmt.Errorf("%s holds estimations of image %s and elf %s, not of image %s and elf %s, so remove it to estimate again",
		artifactName, result.Image, result.ELFSHA256, image, elfSHA256)
}

// readArtifact reads the artifact of an earlier estimation, or an empty one.
func readArtifact(path string) (*artifact, error) {
	result := &artifact{
		Tests:    map[string]*ereserver.CostEstimation{},
		Failures: map[string]string{},
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, result); err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	return result, nil
}

// writeArtifact replaces the artifact in one step, so an interrupted write
// leaves the last complete one in place.
func writeArtifact(path string, result *artifact) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	if _, err := temp.Write(append(data, '\n')); err != nil {
		_ = temp.Close()
		_ = os.Remove(temp.Name())
		return err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(temp.Name())
		return err
	}
	// A temporary file is private, while the artifact is read next to the
	// results the run published.
	if err := os.Chmod(temp.Name(), 0o644); err != nil {
		_ = os.Remove(temp.Name())
		return err
	}
	return os.Rename(temp.Name(), path)
}
