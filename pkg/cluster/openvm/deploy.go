package openvm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"golang.org/x/sync/errgroup"

	"github.com/ethpandaops/provoor/pkg/cluster"
)

// Readiness budgets. The coordinator only has to open its API port, while a
// worker registers milliseconds after launch but binds its socket only after
// compiling every loadout program to a native library, minutes per program,
// so worker readiness gets a generous budget that scales with the loadout.
// Keygen holds a GPU for minutes per program and is bounded by the
// invocation context alone.
const (
	coordinatorReadyTimeout = 120 * time.Second
	workerReadyTimeout      = 900 * time.Second
)

// workerContainerPattern matches the deployment's worker container names, so
// teardown covers containers of an earlier configuration with more GPUs.
var workerContainerPattern = regexp.MustCompile(`^/?` + workerContainerPrefix + `[0-9]+$`)

// Up deploys the cluster and blocks until the coordinator accepts
// connections and every worker container has registered. It is idempotent,
// a container already running is left in place and a keyset already derived
// into the artifacts volume is reused per program.
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
	for _, prog := range programs {
		out.Printf("guest %s resolves to %s", prog.source, prog.name)
	}
	d := &deployment{cfg: cfg, hosts: hosts, out: out, programs: programs, loadout: edgePrograms(programs)}

	// Pull the image on every host first, so every container of this
	// deployment runs the current tag.
	g, gctx := errgroup.WithContext(ctx)
	for _, destination := range hosts.Destinations() {
		g.Go(func() error {
			out.Printf("[%s] pulling %s", cluster.HostName(destination), cfg.imageRef())
			return cluster.PullImage(gctx, hosts.Client(destination), cfg.imageRef())
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Derive missing keysets on every host. The coordinator host needs the
	// artifacts too, it serves each program's verification baseline.
	g, gctx = errgroup.WithContext(ctx)
	for _, n := range d.nodes() {
		g.Go(func() error {
			return d.ensureArtifacts(gctx, n)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	if err := d.startCoordinator(ctx); err != nil {
		return err
	}

	g, gctx = errgroup.WithContext(ctx)
	for _, group := range d.workerHosts() {
		g.Go(func() error {
			return d.startWorkers(gctx, group)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	out.Printf("cluster ready, coordinator api on %s port %d, %d worker containers accepting work",
		cfg.Coordinator.Name, apiPort, numProvers(cfg))
	return nil
}

// Down stops and removes every cluster container, workers first and then the
// coordinator, reversing the start order. Stop failures are collected rather
// than aborting, so one degraded host cannot leave the coordinator running.
// Journald logs, the volumes, and the persisted final proofs stay in place.
func (cfg *Config) Down(ctx context.Context, w io.Writer) error {
	out := cluster.NewOutput(w)
	hosts, err := cluster.DialHosts(ctx, cfg.destinations())
	if err != nil {
		return err
	}
	defer hosts.Close()

	var (
		mu   sync.Mutex
		errs []error
	)
	var wg sync.WaitGroup
	seen := map[string]bool{}
	for _, worker := range cfg.Workers {
		if seen[worker.SSH] {
			continue
		}
		seen[worker.SSH] = true
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := stopWorkers(ctx, hosts.Client(worker.SSH), worker.Name, out); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if err := cluster.StopAndRemove(ctx, hosts.Client(cfg.Coordinator.SSH), coordinatorContainer, cfg.Coordinator.Name, out); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}

	out.Printf("logs remain available via journalctl CONTAINER_NAME=%s<n> or CONTAINER_NAME=%s on each host",
		workerContainerPrefix, coordinatorContainer)
	return nil
}

// stopWorkers stops every worker container on one host by name pattern, so
// a teardown after removing workers from the configuration still covers them.
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

func (cfg *Config) destinations() []string {
	destinations := []string{cfg.Coordinator.SSH}
	for _, worker := range cfg.Workers {
		destinations = append(destinations, worker.SSH)
	}
	return destinations
}

// resolvePrograms reads every guest source and derives its loadout entry,
// rejecting duplicates whose keysets would collide.
func resolvePrograms(ctx context.Context, sources []string) ([]program, error) {
	programs := make([]program, 0, len(sources))
	seen := map[string]string{}
	for _, source := range sources {
		elf, err := cluster.ResolveGuestELF(ctx, source)
		if err != nil {
			return nil, err
		}
		name := programName(elf)
		if prior, ok := seen[name]; ok {
			return nil, fmt.Errorf("guest %s duplicates %s, both hash to %s", source, prior, name)
		}
		seen[name] = source
		programs = append(programs, program{name: name, source: source, elf: elf})
	}
	return programs, nil
}

// deployment carries the dialed hosts and resolved loadout of one Up
// invocation.
type deployment struct {
	cfg      *Config
	hosts    *cluster.Hosts
	out      *cluster.Output
	programs []program
	loadout  string
}

// node is one deployment host in its artifact-provisioning role, the
// coordinator and workers deduplicated by destination.
type node struct {
	ssh  string
	name string
}

func (d *deployment) nodes() []node {
	nodes := []node{{ssh: d.cfg.Coordinator.SSH, name: d.cfg.Coordinator.Name}}
	seen := map[string]bool{d.cfg.Coordinator.SSH: true}
	for _, worker := range d.cfg.Workers {
		if seen[worker.SSH] {
			continue
		}
		seen[worker.SSH] = true
		nodes = append(nodes, node{ssh: worker.SSH, name: worker.Name})
	}
	return nodes
}

// ensureArtifacts derives the keyset of every program the host's artifacts
// volume does not hold yet. Keygen is deterministic under one VM
// configuration, so every host derives identical artifacts independently and
// no bytes cross hosts.
func (d *deployment) ensureArtifacts(ctx context.Context, n node) error {
	cli := d.hosts.Client(n.ssh)
	missing, err := d.missingPrograms(ctx, cli, n)
	if err != nil {
		return err
	}
	for _, prog := range missing {
		if err := d.runKeygen(ctx, cli, n, prog); err != nil {
			return err
		}
	}
	if len(missing) == 0 {
		d.out.Printf("[%s] all keysets already in volume %s", n.name, artifactsVolume(d.cfg.ImageTag))
	}
	return nil
}

// missingPrograms probes which programs lack a derived keyset, everything
// when the artifacts volume does not exist yet and otherwise by each
// program's verification baseline, the last file keygen moves into place.
func (d *deployment) missingPrograms(ctx context.Context, cli *client.Client, n node) ([]program, error) {
	volumeName := artifactsVolume(d.cfg.ImageTag)
	if _, err := cli.VolumeInspect(ctx, volumeName); client.IsErrNotFound(err) {
		return d.programs, nil
	} else if err != nil {
		return nil, fmt.Errorf("inspecting volume %s on %s: %w", volumeName, n.name, err)
	}

	names := make([]string, len(d.programs))
	for i, prog := range d.programs {
		names[i] = prog.name
	}
	probe := fmt.Sprintf(
		`for name in %s; do [ -e "%s/programs/${name}/%d/baseline.bin" ] && echo "${name}"; done; true`,
		strings.Join(names, " "), artifactsDir, programVersion)
	containerCfg := &container.Config{Image: d.cfg.imageRef(), Cmd: []string{"bash", "-c", probe}}
	hostCfg := &container.HostConfig{Mounts: []mount.Mount{{
		Type:     mount.TypeVolume,
		Source:   volumeName,
		Target:   artifactsDir,
		ReadOnly: true,
	}}}
	var output bytes.Buffer
	if err := cluster.RunToCompletion(ctx, cli, keygenContainer+"-probe", containerCfg, hostCfg, nil, &output); err != nil {
		return nil, fmt.Errorf("probing keysets on %s: %w", n.name, err)
	}

	present := map[string]bool{}
	for _, name := range strings.Fields(output.String()) {
		present[name] = true
	}
	missing := []program{}
	for _, prog := range d.programs {
		if !present[prog.name] {
			missing = append(missing, prog)
		}
	}
	return missing, nil
}

// runKeygen derives one program's keyset, injecting the guest ELF into the
// container so no file touches the remote host filesystem. A failed run
// removes the partial program directory, so the next Up retries it, while
// the shared proving keys stay untouched, keygen only writes them after
// succeeding.
func (d *deployment) runKeygen(ctx context.Context, cli *client.Client, n node, prog program) error {
	d.out.Printf("[%s] deriving keyset for %s, minutes on GPU...", n.name, prog.name)
	containerCfg, hostCfg := keygenSpec(d.cfg, prog)
	output := d.out.Prefixed(n.name)
	err := cluster.RunToCompletion(ctx, cli, keygenContainer, containerCfg, hostCfg,
		map[string][]byte{guestELFPath: prog.elf}, output)
	output.Flush()
	if err != nil {
		cleanup := fmt.Sprintf(`rm -rf "%s/programs/%s"`, artifactsDir, prog.name)
		cleanupCfg := &container.Config{Image: d.cfg.imageRef(), Cmd: []string{"bash", "-c", cleanup}}
		_ = cluster.RunToCompletion(context.WithoutCancel(ctx), cli, keygenContainer,
			cleanupCfg, &container.HostConfig{Mounts: hostCfg.Mounts}, nil, io.Discard)
		return fmt.Errorf("keygen for %s on %s: %w", prog.name, n.name, err)
	}
	d.out.Printf("[%s] keyset ready for %s", n.name, prog.name)
	return nil
}

func (d *deployment) startCoordinator(ctx context.Context) error {
	cli := d.hosts.Client(d.cfg.Coordinator.SSH)
	n := d.cfg.Coordinator.Name

	running, err := cluster.EnsureAbsentUnlessRunning(ctx, cli, coordinatorContainer)
	if err != nil {
		return fmt.Errorf("coordinator on %s: %w", n, err)
	}
	if running {
		d.out.Printf("[%s] %s already running, run provoor down first to apply config changes", n, coordinatorContainer)
	} else {
		containerCfg, hostCfg := coordinatorSpec(d.cfg, d.loadout)
		created, err := cli.ContainerCreate(ctx, containerCfg, hostCfg, nil, nil, coordinatorContainer)
		if err != nil {
			return fmt.Errorf("creating coordinator on %s: %w", n, err)
		}
		if err := cluster.CopyFileToContainer(ctx, cli, created.ID, managerConfig, []byte(managerTOML(d.cfg))); err != nil {
			return fmt.Errorf("writing coordinator config on %s: %w", n, err)
		}
		if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
			return fmt.Errorf("starting coordinator on %s: %w", n, err)
		}
		d.out.Printf("[%s] starting coordinator (api %d)...", n, apiPort)
	}

	if err := d.waitCoordinatorReady(ctx, cli, n); err != nil {
		return err
	}
	d.out.Printf("[%s] coordinator ready (api %d)", n, apiPort)
	return nil
}

func (d *deployment) waitCoordinatorReady(ctx context.Context, cli *client.Client, n string) error {
	probe := []string{"bash", "-c", fmt.Sprintf("echo > /dev/tcp/127.0.0.1/%d", apiPort)}
	deadline := time.Now().Add(coordinatorReadyTimeout)
	for {
		inspect, err := cli.ContainerInspect(ctx, coordinatorContainer)
		if err != nil {
			return fmt.Errorf("inspecting coordinator on %s: %w", n, err)
		}
		if inspect.State == nil || !inspect.State.Running {
			return fmt.Errorf("coordinator on %s exited before becoming ready, journalctl CONTAINER_NAME=%s has the log",
				n, coordinatorContainer)
		}
		ready, err := cluster.ExecSucceeds(ctx, cli, coordinatorContainer, probe)
		if err != nil {
			return fmt.Errorf("probing coordinator on %s: %w", n, err)
		}
		if ready {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("coordinator on %s not ready after %s", n, coordinatorReadyTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

// hostWorkers is one host's worker containers, each carrying its position
// in the configuration, which is its cluster-wide prover id.
type hostWorkers struct {
	node    cluster.Node
	workers []indexedWorker
}

type indexedWorker struct {
	Worker
	proverID int
}

// workerHosts groups the flat worker list by host in configuration order.
func (d *deployment) workerHosts() []hostWorkers {
	groups := []hostWorkers{}
	index := map[string]int{}
	for i, worker := range d.cfg.Workers {
		hostIndex, ok := index[worker.SSH]
		if !ok {
			hostIndex = len(groups)
			index[worker.SSH] = hostIndex
			groups = append(groups, hostWorkers{node: worker.Node})
		}
		groups[hostIndex].workers = append(groups[hostIndex].workers, indexedWorker{Worker: worker, proverID: i})
	}
	return groups
}

// startWorkers starts one host's worker containers and blocks until each one
// accepts work.
func (d *deployment) startWorkers(ctx context.Context, group hostWorkers) error {
	cli := d.hosts.Client(group.node.SSH)
	info, err := cli.Info(ctx)
	if err != nil {
		return fmt.Errorf("reading docker info on %s: %w", group.node.Name, err)
	}
	if info.NCPU < len(group.workers) {
		return fmt.Errorf("%d CPUs on %s cannot be split across %d workers", info.NCPU, group.node.Name, len(group.workers))
	}

	for position, worker := range group.workers {
		name := workerContainer(worker.GPU)
		running, err := cluster.EnsureAbsentUnlessRunning(ctx, cli, name)
		if err != nil {
			return fmt.Errorf("worker %s on %s: %w", name, group.node.Name, err)
		}
		if running {
			d.out.Printf("[%s] %s already running, run provoor down first to apply config changes", group.node.Name, name)
			continue
		}
		cpuset := cpusetCPUs(position, len(group.workers), info.NCPU)
		containerCfg, hostCfg := workerSpec(d.cfg, worker.GPU, cpuset, d.loadout)
		created, err := cli.ContainerCreate(ctx, containerCfg, hostCfg, nil, nil, name)
		if err != nil {
			return fmt.Errorf("creating worker %s on %s: %w", name, group.node.Name, err)
		}
		toml := workerTOML(d.cfg, worker.Worker, worker.proverID)
		if err := cluster.CopyFileToContainer(ctx, cli, created.ID, workerConfigPath(worker.GPU), []byte(toml)); err != nil {
			return fmt.Errorf("writing worker config %s on %s: %w", name, group.node.Name, err)
		}
		if err := cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
			return fmt.Errorf("starting worker %s on %s: %w", name, group.node.Name, err)
		}
	}
	d.out.Printf("[%s] waiting for %d workers to accept work...", group.node.Name, len(group.workers))

	// The ready line, not the registration line, gates completion. A worker
	// registers milliseconds after launch but only binds its socket once
	// every loadout program is built, so proceeding on registration would
	// hand proofs to workers that cannot answer yet.
	pending := map[string]bool{}
	for _, worker := range group.workers {
		pending[workerContainer(worker.GPU)] = true
	}
	deadline := time.Now().Add(workerReadyTimeout)
	for len(pending) > 0 {
		for name := range pending {
			inspect, err := cli.ContainerInspect(ctx, name)
			if err != nil {
				return fmt.Errorf("inspecting worker %s on %s: %w", name, group.node.Name, err)
			}
			if inspect.State == nil || !inspect.State.Running {
				return fmt.Errorf("worker %s on %s exited before it was ready, journalctl CONTAINER_NAME=%s has the log",
					name, group.node.Name, name)
			}
			logs, err := cluster.ContainerLogsText(ctx, cli, name)
			if err != nil {
				return fmt.Errorf("reading worker %s log on %s: %w", name, group.node.Name, err)
			}
			if strings.Contains(logs, workerReadyLine) {
				d.out.Printf("[%s] %s ready", group.node.Name, name)
				delete(pending, name)
			}
		}
		if len(pending) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d workers on %s not ready after %s", len(pending), group.node.Name, workerReadyTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	return nil
}
