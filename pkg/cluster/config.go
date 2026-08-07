// Package cluster provides the backend-neutral machinery for deploying
// proving clusters, the host topology configuration, Docker daemons reached
// over SSH, and container lifecycle helpers. Each zkVM backend, such as the
// zisk subpackage, builds its deployment on top and owns everything specific
// to its prover.
package cluster

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Node identifies one deployment host.
type Node struct {
	// Name identifies the node's host. Nodes sharing a name are co-located
	// and dial each other over loopback. Defaults to the SSH host part.
	Name string `yaml:"name"`
	// SSH is the destination the deployer dials, resolved by the local ssh
	// binary against the user's own SSH configuration. Empty targets the
	// local Docker daemon.
	SSH string `yaml:"ssh"`
	// IP is the node's data-network address, which other cluster nodes
	// dial, distinct from SSH for bastion deployments where the SSH proxy
	// routes no cluster traffic. A co-located peer dials loopback instead.
	IP string `yaml:"ip"`
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

// ApplyNodeDefaults fills in node names from the SSH destinations.
func ApplyNodeDefaults(nodes ...*Node) {
	for _, node := range nodes {
		if node.Name == "" {
			node.Name = HostName(node.SSH)
		}
	}
}

// ValidateColocation rejects a worker whose co-location with the coordinator
// cannot be decided, and requires the coordinator's dialable address once
// any worker lives on another host.
func ValidateColocation(coordinator Node, worker Node) error {
	if worker.Name == coordinator.Name && worker.SSH != coordinator.SSH {
		return fmt.Errorf("worker %q shares the coordinator name but not its ssh destination, so co-location cannot be decided", worker.Name)
	}
	if worker.Name != coordinator.Name && coordinator.IP == "" {
		return fmt.Errorf("worker %q is not co-located with the coordinator, coordinator ip is required", worker.Name)
	}
	return nil
}
