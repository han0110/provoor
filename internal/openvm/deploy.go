package openvm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"golang.org/x/sync/errgroup"

	"github.com/han0110/provoor/internal/cluster"
)

// The coordinator only has to open its API port. A worker binds its socket
// only after it compiled every loadout program to a native library, minutes
// per program, so its budget scales with the loadout.
const (
	coordinatorReadyTimeout = 120 * time.Second
	workerReadyTimeout      = 900 * time.Second
)

// workerContainerPattern matches the deployment's worker container names, so
// teardown covers containers of an earlier configuration with more GPUs.
var workerContainerPattern = regexp.MustCompile(`^/?` + workerContainerPrefix + `[0-9]+$`)

// deployment carries the dialed hosts and resolved loadout of one Up.
type deployment struct {
	cfg      *Config
	hosts    *cluster.Hosts
	out      *cluster.Output
	programs []program
	loadout  string
}

// hostWorkers is one host's worker containers, each carrying its position in
// the configuration, which is its cluster-wide prover id.
type hostWorkers struct {
	ssh     string
	workers []indexedWorker
}

type indexedWorker struct {
	Worker
	proverID int
}

// Up deploys the cluster and blocks until the coordinator accepts connections
// and every worker accepts work. A container already running is left in
// place and a keyset already derived into the artifacts volume is reused.
func (cfg *Config) Up(ctx context.Context, w io.Writer) error {
	out := cluster.NewOutput(w)
	hosts, err := cluster.DialHosts(ctx, cfg.destinations())
	if err != nil {
		return err
	}
	defer hosts.Close()

	programs, err := resolvePrograms(ctx, cfg.Guests)
	if err != nil {
		return err
	}
	for _, entry := range programs {
		out.Printf("guest %s resolves to %s", entry.source, entry.name)
	}
	d := &deployment{cfg: cfg, hosts: hosts, out: out, programs: programs, loadout: edgePrograms(programs)}

	if err := hosts.Pull(ctx, cfg.ImageRef(), out); err != nil {
		return err
	}

	// The coordinator host needs the artifacts too, since it serves each
	// program's verification baseline.
	g, gctx := errgroup.WithContext(ctx)
	for _, destination := range hosts.Destinations() {
		g.Go(func() error {
			return d.ensureArtifacts(gctx, destination)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	if err := d.startCoordinator(ctx); err != nil {
		return err
	}

	g, gctx = errgroup.WithContext(ctx)
	for _, group := range cfg.workerHosts() {
		g.Go(func() error {
			return d.startWorkers(gctx, group)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// A cluster without GPU metrics still proves.
	if err := cluster.StartSidecars(ctx, cfg.Telemetry, hosts, out); err != nil {
		out.Printf("telemetry unavailable: %v", err)
	}

	out.Printf("cluster ready, coordinator api on %s port %d, %d worker containers accepting work",
		cfg.CoordinatorHost(), apiPort, len(cfg.Workers))
	return nil
}

// Down stops and removes every cluster container, workers first and then the
// coordinator, collecting stop failures so one degraded host cannot leave the
// coordinator running. Journald logs, volumes, and persisted final proofs stay
// in place.
func (cfg *Config) Down(ctx context.Context, w io.Writer) error {
	out := cluster.NewOutput(w)
	hosts, err := cluster.DialHosts(ctx, cfg.destinations())
	if err != nil {
		return err
	}
	defer hosts.Close()

	cluster.StopSidecars(ctx, hosts)

	groups := cfg.workerHosts()
	errs := make([]error, len(groups))
	var wg sync.WaitGroup
	for i, group := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = stopWorkers(ctx, hosts.Client(group.ssh), cluster.HostName(group.ssh), out)
		}()
	}
	wg.Wait()
	errs = append(errs, cluster.StopAndRemove(ctx, hosts.Client(cfg.Coordinator.SSH), coordinatorContainer, cluster.CoordinatorName, out))
	if err := errors.Join(errs...); err != nil {
		return err
	}

	out.Printf("logs remain available via journalctl CONTAINER_NAME=%s<n> or CONTAINER_NAME=%s on each host",
		workerContainerPrefix, coordinatorContainer)
	return nil
}

// workerHosts groups the flat worker list by host in configuration order.
func (cfg *Config) workerHosts() []hostWorkers {
	groups := []hostWorkers{}
	index := map[string]int{}
	for i, worker := range cfg.Workers {
		hostIndex, ok := index[worker.SSH]
		if !ok {
			hostIndex = len(groups)
			index[worker.SSH] = hostIndex
			groups = append(groups, hostWorkers{ssh: worker.SSH})
		}
		groups[hostIndex].workers = append(groups[hostIndex].workers, indexedWorker{Worker: worker, proverID: i})
	}
	return groups
}

// resolvePrograms reads every guest's ELF and verifying key and derives its
// loadout entry, rejecting duplicates whose keysets would collide.
func resolvePrograms(ctx context.Context, guests []cluster.Guest) ([]program, error) {
	programs := make([]program, 0, len(guests))
	seen := map[string]string{}
	for _, guest := range guests {
		elf, verifyingKey, err := cluster.ResolveGuest(ctx, guest)
		if err != nil {
			return nil, err
		}
		name := programName(elf)
		if prior, ok := seen[name]; ok {
			return nil, fmt.Errorf("guest %s duplicates %s, both hash to %s", guest.ELF, prior, name)
		}
		seen[name] = guest.ELF
		programs = append(programs, program{name: name, source: guest.ELF, elf: elf, verifyingKey: verifyingKey})
	}
	return programs, nil
}

// stopWorkers stops every worker container on one host by name pattern, so a
// teardown after removing workers from the configuration still covers them.
func stopWorkers(ctx context.Context, cli *client.Client, node string, out *cluster.Output) error {
	containers, err := cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.Arg("name", workerContainerPrefix)),
	})
	if err != nil {
		return fmt.Errorf("listing workers on %s: %w", node, err)
	}
	found := false
	for _, summary := range containers {
		for _, name := range summary.Names {
			if !workerContainerPattern.MatchString(name) {
				continue
			}
			found = true
			if err := cluster.StopAndRemove(ctx, cli, strings.TrimPrefix(name, "/"), node, out); err != nil {
				return err
			}
			break
		}
	}
	if !found {
		out.Printf("[%s] no %s* containers", node, workerContainerPrefix)
	}
	return nil
}

// ensureArtifacts derives the keyset of every program the host's artifacts
// volume does not hold yet, then checks every keyset against its configured
// verifying key. Keygen is deterministic under one VM configuration, so every
// host derives identical artifacts and no bytes cross hosts.
func (d *deployment) ensureArtifacts(ctx context.Context, destination string) error {
	cli := d.hosts.Client(destination)
	node := cluster.HostName(destination)
	missing, err := d.missingPrograms(ctx, cli, node)
	if err != nil {
		return err
	}
	for _, entry := range missing {
		if err := d.runKeygen(ctx, cli, node, entry); err != nil {
			return err
		}
	}
	if len(missing) == 0 {
		d.out.Printf("[%s] all keysets already in volume %s", node, artifactsVolume(d.cfg.ZkvmVersion))
	}
	return d.verifyBaselines(ctx, cli, node)
}

// verifyBaselines checks every derived verification baseline against the
// configured verifying key, keysets carried over from an earlier Up included.
// A mismatch means the configured key does not belong to the configured ELF,
// or the image derives a different keyset.
func (d *deployment) verifyBaselines(ctx context.Context, cli *client.Client, node string) error {
	probe := fmt.Sprintf(
		`for name in %s; do echo "${name} $(sha256sum < "%s/programs/${name}/%d/baseline.bin" | cut -d" " -f1)"; done`,
		strings.Join(d.programNames(), " "), artifactsDir, programVersion)
	output, err := d.probeArtifacts(ctx, cli, probe)
	if err != nil {
		return fmt.Errorf("digesting baselines on %s: %w", node, err)
	}

	digests := map[string]string{}
	for _, line := range strings.Split(output, "\n") {
		if fields := strings.Fields(line); len(fields) == 2 {
			digests[fields[0]] = fields[1]
		}
	}
	for _, entry := range d.programs {
		configured := sha256.Sum256(entry.verifyingKey)
		if want := hex.EncodeToString(configured[:]); digests[entry.name] != want {
			return fmt.Errorf("program %s on %s derived a baseline of sha256 %q, the vk configured for %s hashes to %s",
				entry.name, node, digests[entry.name], entry.source, want)
		}
	}
	d.out.Printf("[%s] every keyset matches its configured vk", node)
	return nil
}

// missingPrograms probes which programs lack a derived keyset, everything
// when the artifacts volume does not exist yet and otherwise by each
// program's verification baseline, the last file keygen moves into place.
func (d *deployment) missingPrograms(ctx context.Context, cli *client.Client, node string) ([]program, error) {
	volumeName := artifactsVolume(d.cfg.ZkvmVersion)
	if _, err := cli.VolumeInspect(ctx, volumeName); errdefs.IsNotFound(err) {
		return d.programs, nil
	} else if err != nil {
		return nil, fmt.Errorf("inspecting volume %s on %s: %w", volumeName, node, err)
	}

	probe := fmt.Sprintf(
		`for name in %s; do [ -e "%s/programs/${name}/%d/baseline.bin" ] && echo "${name}"; done; true`,
		strings.Join(d.programNames(), " "), artifactsDir, programVersion)
	output, err := d.probeArtifacts(ctx, cli, probe)
	if err != nil {
		return nil, fmt.Errorf("probing keysets on %s: %w", node, err)
	}

	present := map[string]bool{}
	for _, name := range strings.Fields(output) {
		present[name] = true
	}
	missing := []program{}
	for _, entry := range d.programs {
		if !present[entry.name] {
			missing = append(missing, entry)
		}
	}
	return missing, nil
}

func (d *deployment) programNames() []string {
	names := make([]string, len(d.programs))
	for i, entry := range d.programs {
		names[i] = entry.name
	}
	return names
}

// probeArtifacts runs one shell snippet over the host's artifacts volume,
// mounted read-only, and returns what it wrote.
func (d *deployment) probeArtifacts(ctx context.Context, cli *client.Client, script string) (string, error) {
	var output bytes.Buffer
	err := artifactsShellSpec(d.cfg, keygenContainer+"-probe", script, true).Run(ctx, cli, &output)
	return output.String(), err
}

// runKeygen derives one program's keyset. A failed run removes the partial
// program directory, so the next Up retries it, while the shared proving keys
// stay untouched because keygen writes them only after succeeding.
func (d *deployment) runKeygen(ctx context.Context, cli *client.Client, node string, entry program) error {
	d.out.Printf("[%s] deriving keyset for %s, minutes on GPU...", node, entry.name)
	output := d.out.Prefixed(node)
	err := keygenSpec(d.cfg, entry).Run(ctx, cli, output)
	output.Flush()
	if err != nil {
		cleanup := fmt.Sprintf(`rm -rf "%s/programs/%s"`, artifactsDir, entry.name)
		_ = artifactsShellSpec(d.cfg, keygenContainer, cleanup, false).Run(context.WithoutCancel(ctx), cli, io.Discard)
		return fmt.Errorf("keygen for %s on %s: %w", entry.name, node, err)
	}
	d.out.Printf("[%s] keyset ready for %s", node, entry.name)
	return nil
}

func (d *deployment) startCoordinator(ctx context.Context) error {
	cli := d.hosts.Client(d.cfg.Coordinator.SSH)
	node := d.cfg.CoordinatorHost()

	running, err := cluster.Running(ctx, cli, coordinatorContainer)
	if err != nil {
		return fmt.Errorf("coordinator on %s: %w", node, err)
	}
	if running {
		d.out.Printf("[%s] %s already running, run provoor down first to apply config changes", cluster.CoordinatorName, coordinatorContainer)
	} else {
		if err := coordinatorSpec(d.cfg, d.loadout).Start(ctx, cli); err != nil {
			return fmt.Errorf("starting coordinator on %s: %w", node, err)
		}
		d.out.Printf("[%s] starting coordinator (api %d)...", cluster.CoordinatorName, apiPort)
	}

	if err := cluster.WaitListening(ctx, cli, coordinatorContainer, node, coordinatorReadyTimeout, apiPort); err != nil {
		return err
	}
	d.out.Printf("[%s] coordinator ready (api %d)", cluster.CoordinatorName, apiPort)
	return nil
}

// startWorkers starts one host's worker containers and blocks until each one
// logs its ready line. A worker registers milliseconds after launch but binds
// its socket only once every loadout program is built, so registration gates
// nothing.
func (d *deployment) startWorkers(ctx context.Context, group hostWorkers) error {
	cli := d.hosts.Client(group.ssh)
	host := cluster.HostName(group.ssh)
	info, err := cli.Info(ctx)
	if err != nil {
		return fmt.Errorf("reading docker info on %s: %w", host, err)
	}
	if info.NCPU < len(group.workers) {
		return fmt.Errorf("%d CPUs on %s cannot be split across %d workers", info.NCPU, host, len(group.workers))
	}

	// The container is named after the GPU it owns, which makes it unique on
	// its host, while the label names the configuration entry it came from.
	names := make([]string, len(group.workers))
	labels := make([]string, len(group.workers))
	for position, worker := range group.workers {
		names[position] = workerContainer(worker.deviceID())
		labels[position] = workerName(worker.proverID, worker.Worker)
		running, err := cluster.Running(ctx, cli, names[position])
		if err != nil {
			return fmt.Errorf("%s: %w", labels[position], err)
		}
		if running {
			d.out.Printf("[%s] %s already running, run provoor down first to apply config changes", labels[position], names[position])
			continue
		}
		cpuset := cpusetCPUs(position, len(group.workers), info.NCPU)
		if err := workerSpec(d.cfg, worker.Worker, worker.proverID, cpuset, d.loadout).Start(ctx, cli); err != nil {
			return fmt.Errorf("starting %s: %w", labels[position], err)
		}
	}
	d.out.Printf("[%s] waiting for %d workers to accept work...", host, len(group.workers))

	g, gctx := errgroup.WithContext(ctx)
	for position := range group.workers {
		g.Go(func() error {
			if err := cluster.WaitLogLine(gctx, cli, names[position], labels[position], workerReadyLine, workerReadyTimeout); err != nil {
				return err
			}
			d.out.Printf("[%s] ready", labels[position])
			return nil
		})
	}
	return g.Wait()
}
