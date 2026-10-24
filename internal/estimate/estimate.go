// Package estimate estimates what proving every test of a benchmark run, or of
// each instance of a benchmarkoor configuration, costs. It executes the guest
// program on each test's input in an ere-server. A profile also records what
// each guest call stack costs, in one pprof profile per test.
package estimate

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ethpandaops/benchmarkoor/pkg/config"
	"github.com/ethpandaops/benchmarkoor/pkg/executor"
	"golang.org/x/sync/errgroup"

	"github.com/han0110/provoor/internal/cluster"
	"github.com/han0110/provoor/internal/ereserver"
)

const (
	// artifactName is the estimation artifact of one estimate.
	artifactName = "result.estimate.json"
	// estimatesDir holds one directory per estimate, beside the runs and the
	// suites of a results directory.
	estimatesDir = "estimates"
	// progressInterval is how many estimations pass between two saves of the
	// artifact, so an interrupted command resumes near where it stopped.
	progressInterval = 50
)

// ereServerTags maps the zkvm and zkvm_version labels of an instance to the tag
// of the ere-server image that estimates the cost of its guest program.
var ereServerTags = map[string]map[string]string{
	"openvm": {"v2.1.0-preview": "0.19.4"},
	"zisk": {
		"v1.2.0-alpha": "0.18.0",
		// 77e2aae is an ere revision with no release.
		"v1.3.0-alpha": "77e2aae",
		"v1.3.1-alpha": "0.19.2",
		"v1.3.2-alpha": "0.19.4",
	},
}

// profileEreServerTags maps the labels as ereServerTags does, to the tag of the
// ere-server image that profiles the guest program. ere v0.19.1 is the first
// release with the Profile RPC, so a zkVM version that only earlier releases
// support has no tag.
var profileEreServerTags = map[string]map[string]string{
	"openvm": {"v2.1.0-preview": "0.19.4"},
	"zisk": {
		"v1.3.1-alpha": "0.19.2",
		"v1.3.2-alpha": "0.19.4",
	},
}

// kind is what sets a command of the package apart. A profile is an estimate
// whose tests also keep the pprof profile of their execution, which holds the
// cost per guest call stack.
type kind struct {
	// name is the command, which also prefixes the output lines.
	name string
	// gerund, past, and records are the words of the output lines and the
	// errors, such as estimating, estimated, and estimations.
	gerund, past, records string
	// dir holds one directory per estimate, beside the runs and the suites of
	// a results directory. It is also the plural of name.
	dir string
	// artifactName is the artifact in the directory of an estimate.
	artifactName string
	// tags maps the zkvm and zkvm_version labels of an instance to the tag of
	// the ere-server image.
	tags map[string]map[string]string
	// testFile is the file of each test record in the directory of its test,
	// or empty when the records have no file.
	testFile string
	// measure executes the guest program on the input of one test. It returns
	// the record of the test and the content of its test file.
	measure func(ctx context.Context, client *ereserver.Client, stdin []byte) (*ereserver.CostEstimation, []byte, error)
}

var (
	estimateKind = &kind{
		name: "estimate", gerund: "estimating", past: "estimated", records: "estimations",
		dir: estimatesDir, artifactName: artifactName, tags: ereServerTags, measure: estimateTest,
	}
	profileKind = &kind{
		name: "profile", gerund: "profiling", past: "profiled", records: "profiled tests",
		dir: "profiles", artifactName: "result.profile.json", tags: profileEreServerTags,
		testFile: "profile.pb.gz", measure: profileTest,
	}
)

// estimateTest measures one test of an estimate, which has no test file.
func estimateTest(ctx context.Context, client *ereserver.Client, stdin []byte) (*ereserver.CostEstimation, []byte, error) {
	cost, err := client.ExecuteEstimatedCost(ctx, stdin)
	return cost, nil, err
}

// profileTest measures one test of a profile. Its test file is the gzipped
// pprof profile, which go tool pprof reads.
func profileTest(ctx context.Context, client *ereserver.Client, stdin []byte) (*ereserver.CostEstimation, []byte, error) {
	profiled, err := client.Profile(ctx, stdin)
	if err != nil {
		return nil, nil, err
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	// A bytes.Buffer takes every write, so the compression cannot fail.
	_, _ = writer.Write(profiled.Pprof)
	_ = writer.Close()
	// The record is a copy, so it holds no reference to the pprof profile.
	cost := profiled.CostEstimation
	return &cost, compressed.Bytes(), nil
}

// artifact holds the estimations of one estimate, keyed by test name. The
// header ahead of zkvm comes from the config.json of a run or from an instance
// of a benchmarkoor configuration. It never holds extra_args, since they carry
// rig addresses. image_sha256 is the image digest of the latest ere-server, and
// command is the provoor command of the latest invocation.
type artifact struct {
	Timestamp   int64                                `json:"timestamp"`
	SuiteHash   string                               `json:"suite_hash"`
	Instance    instance                             `json:"instance"`
	Metadata    config.MetadataConfig                `json:"metadata"`
	Zkvm        string                               `json:"zkvm"`
	Image       string                               `json:"image"`
	ImageSHA256 string                               `json:"image_sha256,omitempty"`
	Command     []string                             `json:"command,omitempty"`
	ELFURL      string                               `json:"elf_url"`
	ELFSHA256   string                               `json:"elf_sha256"`
	Tests       map[string]*ereserver.CostEstimation `json:"tests"`
	Failures    map[string]string                    `json:"failures,omitempty"`
}

// instance is the benchmarkoor instance with the guest program of an estimate.
type instance struct {
	ID     string `json:"id"`
	Client string `json:"client"`
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

// Run estimates the cost of every test that is not estimated yet and writes
// the result to estimates/<id>/ of the results directory. A directory target
// is a run directory, whose estimate takes the id of the run. Any other target
// is a benchmarkoor configuration, and each of its instances continues its
// latest estimate or starts a new one. An ereTag replaces the tag of
// ereServerTags for every instance, and an elfURL replaces the ELF source of a
// run directory.
func Run(ctx context.Context, target, ereTag, elfURL string, concurrency int, output io.Writer) error {
	return estimateKind.run(ctx, target, ereTag, elfURL, concurrency, output)
}

// Profile profiles every test as Run estimates it, with the tags of
// profileEreServerTags, and writes the result to profiles/<id>/ of the
// results directory. That directory also holds the gzipped pprof profile of
// each profiled test in <test>/profile.pb.gz. The result also goes to
// estimates/<id>/, unless that directory holds the estimations of another
// image, ELF, or suite.
func Profile(ctx context.Context, target, ereTag, elfURL string, concurrency int, output io.Writer) error {
	return profileKind.run(ctx, target, ereTag, elfURL, concurrency, output)
}

// run measures the tests of the target, as Run describes for an estimate.
func (k *kind) run(ctx context.Context, target, ereTag, elfURL string, concurrency int, output io.Writer) error {
	info, err := os.Stat(target)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return k.estimateRun(ctx, target, ereTag, elfURL, concurrency, output)
	}
	if elfURL != "" {
		return errors.New("--elf applies only to a run directory, since a configuration names the ELF of each instance")
	}
	return k.estimateConfig(ctx, target, ereTag, concurrency, output)
}

// estimateRun estimates the tests of a run. The header of the estimate comes
// from the config.json of the run on every invocation.
func (k *kind) estimateRun(ctx context.Context, runDir, ereTag, elfURL string, concurrency int, output io.Writer) error {
	runDir, err := filepath.Abs(runDir)
	if err != nil {
		return err
	}
	run, err := readRunConfig(runDir)
	if err != nil {
		return err
	}
	names, err := readTestNames(runDir)
	if err != nil {
		return err
	}
	// The results directory is the parent of runs/.
	resultsDir, id := filepath.Dir(filepath.Dir(runDir)), filepath.Base(runDir)
	path := k.artifactPath(resultsDir, id)
	result, err := readArtifact(path)
	if err != nil {
		return err
	}
	source := cmp.Or(elfURL, run.ELFSource)
	tag, elf, elfSHA256, err := k.resolveGuest(ctx, run.Metadata.Labels, ereTag, source)
	if err != nil {
		return err
	}
	image := ereserver.Image(run.Metadata.Labels["zkvm"], tag)
	if err := k.checkResumable(path, result, image, elfSHA256); err != nil {
		return err
	}
	result.Timestamp, result.SuiteHash = run.Timestamp, run.SuiteHash
	result.Instance, result.Metadata = run.Instance, run.Metadata
	result.Zkvm, result.Image = run.Metadata.Labels["zkvm"], image
	result.ELFURL, result.ELFSHA256 = relativeToWorkingDir(source), elfSHA256
	result.Command = append(k.invocation(runDir, tag), "--elf", result.ELFURL)
	return k.complete(ctx, resultsDir, id, result, names, elf, func() (string, error) {
		return prepareFixtures(ctx, runDir, run.SuiteHash)
	}, concurrency, output)
}

// estimateConfig estimates the tests of every instance of a benchmarkoor
// configuration, one after another, under the results directory of the
// configuration.
func (k *kind) estimateConfig(ctx context.Context, path, ereTag string, concurrency int, output io.Writer) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if cfg.Runner.Benchmark.Tests.Source.EESTFixtures == nil {
		return errors.New("runner.benchmark.tests.source.eest_fixtures is not set, and only EEST fixtures carry stateless inputs")
	}
	// Every instance maps to its image and reads its ELF before the fixture
	// work, so a typo fails in seconds.
	estimates := make([]*artifact, len(cfg.Runner.Instances))
	elfs := make([][]byte, len(cfg.Runner.Instances))
	for i := range cfg.Runner.Instances {
		clientInstance := &cfg.Runner.Instances[i]
		if estimates[i], elfs[i], err = k.newEstimate(ctx, path, ereTag, cfg, clientInstance); err != nil {
			return fmt.Errorf("instance %s: %w", clientInstance.ID, err)
		}
	}
	suite, err := prepareSuite(ctx, cfg)
	if err != nil {
		return err
	}
	resultsDir := cfg.Runner.Benchmark.ResultsDir
	for i, started := range estimates {
		started.SuiteHash = suite.hash
		id, result, err := k.continuation(resultsDir, started)
		if err != nil {
			return err
		}
		if result == nil {
			started.Timestamp = time.Now().Unix()
			id, result = newEstimateID(started.Timestamp, started.Instance.ID), started
		}
		fmt.Fprintf(output, "%s: %s\n", k.name, id)
		result.Command = started.Command
		err = k.complete(ctx, resultsDir, id, result, suite.names, elfs[i], func() (string, error) {
			return suite.fixtures, nil
		}, concurrency, output)
		if err != nil {
			return err
		}
	}
	return nil
}

// newEstimate maps the labels of a configuration instance to its image and
// reads the guest ELF of its extra_args. It returns the artifact that a new
// estimate of the instance starts from, and the ELF.
func (k *kind) newEstimate(ctx context.Context, path, ereTag string, cfg *config.Config, clientInstance *config.ClientInstance) (*artifact, []byte, error) {
	labels := cfg.GetMetadataLabels(clientInstance)
	source, err := elfSource(clientInstance.ExtraArgs)
	if err != nil {
		return nil, nil, err
	}
	tag, elf, elfSHA256, err := k.resolveGuest(ctx, labels, ereTag, source)
	if err != nil {
		return nil, nil, err
	}
	return &artifact{
		Instance:  instance{ID: clientInstance.ID, Client: clientInstance.Client},
		Metadata:  config.MetadataConfig{Labels: labels},
		Zkvm:      labels["zkvm"],
		Image:     ereserver.Image(labels["zkvm"], tag),
		Command:   k.invocation(path, tag),
		ELFURL:    relativeToWorkingDir(source),
		ELFSHA256: elfSHA256,
		Tests:     map[string]*ereserver.CostEstimation{},
		Failures:  map[string]string{},
	}, elf, nil
}

// Link renames the estimate in estimateDir to the id of the run in runDir, so
// the UI links them. The estimate must sit in estimates/ beside runs/, and it
// must hold estimations of the suite and the guest ELF of the run.
func Link(ctx context.Context, runDir, estimateDir string, output io.Writer) error {
	return estimateKind.link(ctx, runDir, estimateDir, output)
}

// LinkProfile renames the profile in profileDir to the id of the run in
// runDir, as Link renames an estimate. The profile must sit in profiles/
// beside runs/. It also renames the estimate that the profile wrote at its old
// id, unless the run has an estimate already.
func LinkProfile(ctx context.Context, runDir, profileDir string, output io.Writer) error {
	return profileKind.link(ctx, runDir, profileDir, output)
}

// link renames the estimate in estimateDir to the id of the run in runDir, as
// Link describes for an estimate.
func (k *kind) link(ctx context.Context, runDir, estimateDir string, output io.Writer) error {
	runDir, err := filepath.Abs(runDir)
	if err != nil {
		return err
	}
	estimateDir, err = filepath.Abs(estimateDir)
	if err != nil {
		return err
	}
	run, err := readRunConfig(runDir)
	if err != nil {
		return err
	}
	// readArtifact gives an empty artifact for a missing file.
	path := filepath.Join(estimateDir, k.artifactName)
	if _, err := os.Stat(path); err != nil {
		return err
	}
	result, err := readArtifact(path)
	if err != nil {
		return err
	}
	resultsDir, id := filepath.Dir(filepath.Dir(runDir)), filepath.Base(runDir)
	estimates := filepath.Join(resultsDir, k.dir)
	if filepath.Dir(estimateDir) != estimates {
		return fmt.Errorf("%s is not in %s of the run", estimateDir, estimates)
	}
	if filepath.Base(estimateDir) == id {
		fmt.Fprintf(output, "%s: %s already links to the run\n", k.name, id)
		return nil
	}
	destination := filepath.Join(estimates, id)
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("%s exists already", destination)
	}
	if result.SuiteHash != run.SuiteHash {
		return fmt.Errorf("%s holds %s of suite %s, not of suite %s of the run", estimateDir, k.records, result.SuiteHash, run.SuiteHash)
	}
	_, elfSHA256, err := resolveELF(ctx, run.ELFSource)
	if err != nil {
		return err
	}
	if result.ELFSHA256 != elfSHA256 {
		return fmt.Errorf("%s holds %s of elf %s, not of elf %s of the run", estimateDir, k.records, result.ELFSHA256, elfSHA256)
	}
	if err := os.Rename(estimateDir, destination); err != nil {
		return err
	}
	linkedMirror, err := k.linkMirror(resultsDir, filepath.Base(estimateDir), id, result, output)
	if err != nil {
		return err
	}
	// The rename changes a directory name that the index holds. Only the
	// estimates have an index.
	if _, err := os.Stat(filepath.Join(resultsDir, estimatesDir, "index.json")); err == nil && (k == estimateKind || linkedMirror) {
		index, err := executor.GenerateEstimateIndex(resultsDir)
		if err != nil {
			return err
		}
		if err := executor.WriteEstimateIndex(resultsDir, index, nil); err != nil {
			return err
		}
	}
	fmt.Fprintf(output, "%s: linked %s to run %s\n", k.name, filepath.Base(estimateDir), id)
	return nil
}

// linkMirror renames the mirror of a profile at oldID to the id of the run, and
// reports whether it renamed it. Only an estimate with the image, ELF, and
// suite of the profile is its mirror. It does nothing for an estimate. An
// estimate at the id of the run stays, beside the mirror.
func (k *kind) linkMirror(resultsDir, oldID, id string, profiled *artifact, output io.Writer) (bool, error) {
	if k != profileKind {
		return false, nil
	}
	// readArtifact gives an empty artifact for a missing file, and its empty
	// ELF digest matches no profile of a run.
	mirror, err := readArtifact(estimateKind.artifactPath(resultsDir, oldID))
	if err != nil || !sameEstimation(mirror, profiled) {
		return false, err
	}
	source, destination := filepath.Join(resultsDir, estimatesDir, oldID), filepath.Join(resultsDir, estimatesDir, id)
	if _, err := os.Stat(destination); err == nil {
		fmt.Fprintf(output, "%s: keeps %s, since %s exists already\n", k.name, filepath.Join(estimatesDir, oldID), filepath.Join(estimatesDir, id))
		return false, nil
	}
	err = os.Rename(source, destination)
	return err == nil, err
}

// resolveGuest returns the ere-server image tag, and the guest ELF with its
// SHA-256 digest.
func (k *kind) resolveGuest(ctx context.Context, labels map[string]string, ereTag, source string) (tag string, elf []byte, elfSHA256 string, err error) {
	tag, err = k.ereServerTag(labels["zkvm"], labels["zkvm_version"], ereTag)
	if err != nil {
		return "", nil, "", err
	}
	elf, elfSHA256, err = resolveELF(ctx, source)
	if err != nil {
		return "", nil, "", err
	}
	return tag, elf, elfSHA256, nil
}

// resolveELF returns the guest ELF of the source with its SHA-256 digest.
func resolveELF(ctx context.Context, source string) (elf []byte, elfSHA256 string, err error) {
	elf, err = cluster.ResolveSource(ctx, source)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(elf)
	return elf, hex.EncodeToString(digest[:]), nil
}

// ereServerTag returns ereTag when it is set, and otherwise the ere-server image
// tag that the tags of the kind map the zkvm and zkvm_version labels to. The
// zkvm label names the image, so both cases require it.
func (k *kind) ereServerTag(zkvm, zkvmVersion, ereTag string) (string, error) {
	if zkvm == "" {
		return "", errors.New("metadata.labels holds no zkvm label")
	}
	if ereTag != "" {
		return ereTag, nil
	}
	tag, ok := k.tags[zkvm][zkvmVersion]
	if !ok {
		return "", fmt.Errorf("no ere-server image is mapped to zkvm %q version %q, so pass --ere-tag to select one", zkvm, zkvmVersion)
	}
	return tag, nil
}

// continuation returns the latest estimate with the instance, image, ELF, and
// suite of the started artifact, the estimate of a run included. The artifact
// is nil when no estimate matches.
func (k *kind) continuation(resultsDir string, started *artifact) (string, *artifact, error) {
	var id string
	var latest *artifact
	err := k.scanEstimates(resultsDir, func(otherID string, other *artifact) {
		if other.Instance.ID == started.Instance.ID && sameEstimation(other, started) &&
			(latest == nil || other.Timestamp > latest.Timestamp) {
			id, latest = otherID, other
		}
	})
	return id, latest, err
}

// newEstimateID returns an id in the run id format, from the start time of the
// estimate and 4 random bytes.
func newEstimateID(timestamp int64, instanceID string) string {
	var random [4]byte
	_, _ = rand.Read(random[:])
	return fmt.Sprintf("%d_%s_%s", timestamp, hex.EncodeToString(random[:]), instanceID)
}

// invocation is the provoor command that estimates the target with the
// ere-server image tag.
func (k *kind) invocation(target, tag string) []string {
	return []string{"provoor", k.name, filepath.Clean(relativeToWorkingDir(target)), "--ere-tag", tag}
}

// relativeToWorkingDir returns an absolute path relative to the working
// directory, so the published artifact holds no home path. Any other value,
// such as a URL, stays as it is.
func relativeToWorkingDir(path string) string {
	if workingDir, err := os.Getwd(); err == nil && filepath.IsAbs(path) {
		if relative, err := filepath.Rel(workingDir, path); err == nil {
			return relative
		}
	}
	return path
}

// complete copies the estimations of earlier estimates into the artifact of
// the estimate id. Then it estimates every test that stays pending with an
// ere-server of the image on the guest ELF. prepare returns the fixtures
// directory, and it runs only when a test is pending. The test files of the
// copied estimations land right before the first save of the artifact, so a
// failure of an earlier step leaves none.
func (k *kind) complete(ctx context.Context, resultsDir, id string, result *artifact, names []string, elf []byte,
	prepare func() (string, error), concurrency int, output io.Writer,
) error {
	// A test file must stay in the directory of the estimate.
	if k.testFile != "" {
		for _, name := range names {
			if !filepath.IsLocal(sanitizeResultPath(name)) {
				return fmt.Errorf("test name %q is not a local path", name)
			}
		}
	}
	mirror, err := k.readMirror(resultsDir, id, result, output)
	if err != nil {
		return err
	}
	reused, sources, err := k.reuseEstimates(resultsDir, id, names, result)
	if err != nil {
		return err
	}
	if reused > 0 {
		fmt.Fprintf(output, "%s: reused %d %s of earlier %s\n", k.name, reused, k.records, k.dir)
	}
	pending := pendingTests(names, result)
	if len(pending) == 0 {
		if err := k.linkTestFiles(resultsDir, id, sources); err != nil {
			return err
		}
		if err := k.save(resultsDir, id, result, mirror); err != nil {
			return err
		}
		fmt.Fprintf(output, "%s: nothing to do\n", k.name)
		return nil
	}

	fixtures, err := prepare()
	if err != nil {
		return err
	}
	hosts, err := cluster.DialHosts(ctx, []string{""})
	if err != nil {
		return err
	}
	defer hosts.Close()
	out := cluster.NewOutput(output)
	if err := hosts.Pull(ctx, result.Image, out); err != nil {
		return err
	}
	server, err := ereserver.Start(ctx, hosts.Client(""), result.Image, elf, out)
	if err != nil {
		return err
	}
	defer func() {
		if err := server.Close(ctx); err != nil {
			out.Printf("removing %s: %v", server.Name(), err)
		}
	}()
	result.ImageSHA256 = server.ImageSHA256
	if err := k.linkTestFiles(resultsDir, id, sources); err != nil {
		return err
	}

	out.Printf("%s %d tests with %s", k.gerund, len(pending), result.Image)
	return k.estimateAll(ctx, &server.Client, fixtures, pending, concurrency, resultsDir, id, result, mirror, out)
}

// estimateAll estimates every pending test against one server and saves the
// artifact as it goes. A guest failure is recorded and the estimation
// continues, while every other failure ends it.
func (k *kind) estimateAll(ctx context.Context, client *ereserver.Client, fixtures string, pending map[string]struct{},
	concurrency int, resultsDir, id string, result, mirror *artifact, output *cluster.Output,
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
				cost, content, err := k.measure(ctx, client, input.stdin)
				var guest *ereserver.GuestError
				if err != nil && !errors.As(err, &guest) {
					return fmt.Errorf("%s %s: %w", k.gerund, input.name, err)
				}
				// The test file lands before the record that names it.
				if content != nil {
					if err := writeFile(k.testFilePath(filepath.Join(resultsDir, k.dir, id), input.name), content); err != nil {
						return err
					}
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
		output.Printf("%s %d/%d", k.past, estimated, total)
		if saveErr = k.save(resultsDir, id, result, mirror); saveErr != nil {
			cancel()
		}
	}
	err := group.Wait()
	if saveErr != nil {
		return saveErr
	}
	// Only a failure before the first outcome leaves the artifact without an
	// estimation. Such an estimate saves nothing.
	if len(result.Tests) == 0 && len(result.Failures) == 0 {
		return err
	}
	if writeErr := k.save(resultsDir, id, result, mirror); err == nil {
		err = writeErr
	}
	if err != nil {
		return err
	}
	output.Printf("%s %d tests, %d failed", k.past, estimated, failed)
	return nil
}

// record files one outcome.
func record(result *artifact, done outcome) {
	if done.guest != nil {
		result.Failures[done.name] = done.guest.Message
		return
	}
	result.Tests[done.name] = done.cost
}

// pendingTests are the tests the artifact holds neither a cost nor a failure
// for. A recorded failure is a guest exit on the test's input, so it stays.
func pendingTests(names []string, result *artifact) map[string]struct{} {
	pending := make(map[string]struct{}, len(names))
	for _, name := range names {
		_, estimated := result.Tests[name]
		_, failed := result.Failures[name]
		if !estimated && !failed {
			pending[name] = struct{}{}
		}
	}
	return pending
}

// reuseEstimates copies into the artifact every estimation it lacks from the
// other estimates of the results directory with the same image, ELF, and
// suite. It copies from the estimates of runs and of configurations. A rerun
// of a guest then starts from the estimations of the earlier estimate. It
// returns the number of copied records, and the id of the estimate that each
// copied cost comes from, keyed by test name.
func (k *kind) reuseEstimates(resultsDir, id string, names []string, result *artifact) (int, map[string]string, error) {
	reused := 0
	sources := map[string]string{}
	err := k.scanEstimates(resultsDir, func(otherID string, other *artifact) {
		if otherID == id || !sameEstimation(other, result) {
			return
		}
		for name := range pendingTests(names, result) {
			if cost, ok := other.Tests[name]; ok {
				result.Tests[name] = cost
				sources[name] = otherID
				reused++
			} else if message, ok := other.Failures[name]; ok {
				result.Failures[name] = message
				reused++
			}
		}
	})
	return reused, sources, err
}

// linkTestFiles hard links the test file of each test in sources into the
// estimate id. sources maps a test name to the estimate that holds its file.
// It does nothing when the records of the kind have no file.
func (k *kind) linkTestFiles(resultsDir, id string, sources map[string]string) error {
	if k.testFile == "" {
		return nil
	}
	estimates := filepath.Join(resultsDir, k.dir)
	for name, otherID := range sources {
		destination := k.testFilePath(filepath.Join(estimates, id), name)
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return err
		}
		// An interrupted earlier invocation can leave a test file without its
		// record.
		if err := os.Remove(destination); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := os.Link(k.testFilePath(filepath.Join(estimates, otherID), name), destination); err != nil {
			return err
		}
	}
	return nil
}

// testFilePath is the test file of the test name in the directory of an
// estimate. Its path is the one that benchmarkoor gives the results of the test.
func (k *kind) testFilePath(estimateDir, name string) string {
	return filepath.Join(estimateDir, sanitizeResultPath(name), k.testFile)
}

// scanEstimates passes the id and the artifact of every estimate of the
// results directory to visit, in id order.
func (k *kind) scanEstimates(resultsDir string, visit func(id string, estimated *artifact)) error {
	entries, err := os.ReadDir(filepath.Join(resultsDir, k.dir))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		estimated, err := readArtifact(k.artifactPath(resultsDir, entry.Name()))
		if err != nil {
			return err
		}
		visit(entry.Name(), estimated)
	}
	return nil
}

// sameEstimation reports whether two artifacts estimate the same suite with the
// same image and ELF, so that their estimations are interchangeable.
func sameEstimation(a, b *artifact) bool {
	return a.Image == b.Image && a.ELFSHA256 == b.ELFSHA256 && a.SuiteHash == b.SuiteHash
}

// artifactPath is the artifact of the estimate id in the results directory.
func (k *kind) artifactPath(resultsDir, id string) string {
	return filepath.Join(resultsDir, k.dir, id, k.artifactName)
}

// checkResumable rejects an artifact an earlier command filled under another
// image or another ELF, since its costs belong to that guest program.
func (k *kind) checkResumable(path string, result *artifact, image, elfSHA256 string) error {
	if len(result.Tests) == 0 && len(result.Failures) == 0 {
		return nil
	}
	if result.Image == image && result.ELFSHA256 == elfSHA256 {
		return nil
	}
	return fmt.Errorf("%s holds %s of image %s and elf %s, not of image %s and elf %s, so remove it to %s again",
		path, k.records, result.Image, result.ELFSHA256, image, elfSHA256, k.name)
}

// readMirror reads the mirror of a profile, the estimate at its id that the
// profile also writes. The mirror is nil for an estimate. It is also nil when
// the estimate at the id holds the estimations of another image, ELF, or
// suite, since that estimate stays as it is.
func (k *kind) readMirror(resultsDir, id string, result *artifact, output io.Writer) (*artifact, error) {
	if k != profileKind {
		return nil, nil
	}
	path := estimateKind.artifactPath(resultsDir, id)
	mirror, err := readArtifact(path)
	if err != nil {
		return nil, err
	}
	// readArtifact gives an empty artifact for a missing file.
	if _, err := os.Stat(path); err == nil && !sameEstimation(mirror, result) {
		fmt.Fprintf(output, "%s: keeps %s, which holds another image, ELF, or suite\n", k.name, filepath.Join(estimatesDir, id))
		return nil, nil
	}
	return mirror, nil
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

// save writes the artifact of the estimate id. A profile with a mirror also
// writes the artifact as the estimate of the same id, with the records of the
// mirror that the artifact does not replace.
func (k *kind) save(resultsDir, id string, result, mirror *artifact) error {
	if err := writeArtifact(k.artifactPath(resultsDir, id), result); err != nil || mirror == nil {
		return err
	}
	merged := *result
	merged.Tests, merged.Failures = maps.Clone(mirror.Tests), maps.Clone(mirror.Failures)
	for name := range result.Tests {
		delete(merged.Failures, name)
	}
	for name := range result.Failures {
		delete(merged.Tests, name)
	}
	maps.Copy(merged.Tests, result.Tests)
	maps.Copy(merged.Failures, result.Failures)
	return writeArtifact(estimateKind.artifactPath(resultsDir, id), &merged)
}

// writeArtifact replaces the artifact in one step, so an interrupted write
// leaves the last complete one in place.
func writeArtifact(path string, result *artifact) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(path, append(data, '\n'))
}

// writeFile replaces the file at path with data in one step.
func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		_ = os.Remove(temp.Name())
		return err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(temp.Name())
		return err
	}
	// A temporary file is private, while the file is published with the
	// results.
	if err := os.Chmod(temp.Name(), 0o644); err != nil {
		_ = os.Remove(temp.Name())
		return err
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		_ = os.Remove(temp.Name())
		return err
	}
	return nil
}
