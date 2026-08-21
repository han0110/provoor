// Package cluster provides the backend-neutral machinery for deploying
// proving clusters, the host topology configuration, Docker daemons reached
// over SSH, and container lifecycle helpers. Each zkVM backend, such as the
// zisk subpackage, builds its deployment on top and owns everything specific
// to its prover.
package cluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Node identifies one deployment host.
type Node struct {
	// SSH is the destination the deployer dials, resolved by the local ssh
	// binary against the user's own SSH configuration. Empty targets the
	// local Docker daemon. It identifies the host, so entries sharing it are
	// co-located and dial each other over loopback.
	SSH string `yaml:"ssh"`
	// IP is the node's data-network address, which other cluster nodes
	// dial, distinct from SSH for bastion deployments where the SSH proxy
	// routes no cluster traffic. A co-located peer dials loopback instead.
	IP string `yaml:"ip"`
}

// GPU selects the GPUs one container is given, either a count the daemon
// chooses or the device ids to expose. These are the two forms of docker's
// --gpus that name an amount, and one of them is required, since a deployment
// that never says how many GPUs it proves on cannot advertise its own
// capacity.
type GPU struct {
	Count     int   `yaml:"count"`
	DeviceIDs []int `yaml:"device_ids"`
}

// Len is the number of GPUs the selection exposes.
func (g GPU) Len() int {
	if g.Count > 0 {
		return g.Count
	}
	return len(g.DeviceIDs)
}

// Label names the selection for a worker id and the log lines carrying it, a
// count as x4 and device ids as 0_1.
func (g GPU) Label() string {
	if g.Count > 0 {
		return "x" + strconv.Itoa(g.Count)
	}
	ids := make([]string, len(g.DeviceIDs))
	for i, id := range g.DeviceIDs {
		ids[i] = strconv.Itoa(id)
	}
	return strings.Join(ids, "_")
}

// Validate rejects a selection naming no amount, both forms at once, or a
// device id no daemon could expose.
func (g GPU) Validate() error {
	switch {
	case g.Count > 0 && len(g.DeviceIDs) > 0:
		return fmt.Errorf("gpu takes count or device_ids, not both")
	case g.Count < 0:
		return fmt.Errorf("gpu count expects a positive integer, got %d", g.Count)
	case g.Count == 0 && len(g.DeviceIDs) == 0:
		return fmt.Errorf("gpu expects a count or device_ids")
	}
	seen := map[int]bool{}
	for _, id := range g.DeviceIDs {
		if id < 0 {
			return fmt.Errorf("gpu device_ids expects non-negative ids, got %d", id)
		}
		if seen[id] {
			return fmt.Errorf("gpu device_ids repeats %d", id)
		}
		seen[id] = true
	}
	return nil
}

// WorkerName identifies one worker by its position in the configuration and
// the GPUs it owns. It is the id the worker registers under and the prefix its
// progress lines carry, so a reader matches a line to a configuration entry
// without the deployment naming its hosts.
func WorkerName(index int, gpu GPU) string {
	return fmt.Sprintf("worker_%d-gpu_%s", index, gpu.Label())
}

// Zkvm reads the zkvm field of a cluster configuration file, which selects
// the backend that parses the rest.
func Zkvm(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var peek struct {
		Zkvm string `yaml:"zkvm"`
	}
	if err := yaml.Unmarshal(raw, &peek); err != nil {
		return "", fmt.Errorf("parsing %s: %w", path, err)
	}
	return peek.Zkvm, nil
}

// ValidateColocation requires the coordinator's dialable address once any
// worker lives on another host. Hosts are identified by SSH destination, so
// co-location is exact rather than inferred.
func ValidateColocation(coordinator Node, worker Node) error {
	if worker.SSH != coordinator.SSH && coordinator.IP == "" {
		return fmt.Errorf("worker on %s is not co-located with the coordinator, coordinator ip is required", HostName(worker.SSH))
	}
	return nil
}
