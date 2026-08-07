package main

import (
	"context"
	"fmt"
	"io"

	"github.com/ethpandaops/provoor/pkg/cluster"
	"github.com/ethpandaops/provoor/pkg/cluster/openvm"
	"github.com/ethpandaops/provoor/pkg/cluster/zisk"
)

// clusterBackend is the deploy lifecycle every zkVM backend package provides,
// selected by the configuration's zkvm field.
type clusterBackend interface {
	Up(ctx context.Context, w io.Writer) error
	Down(ctx context.Context, w io.Writer) error
}

func loadCluster(path string) (clusterBackend, error) {
	zkvm, err := cluster.Zkvm(path)
	if err != nil {
		return nil, err
	}
	switch zkvm {
	case "zisk":
		return zisk.Load(path)
	case "openvm":
		return openvm.Load(path)
	default:
		return nil, fmt.Errorf("zkvm %q is not supported, only zisk and openvm", zkvm)
	}
}
