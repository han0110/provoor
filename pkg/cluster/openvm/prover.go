package openvm

import (
	"context"
	"fmt"

	"github.com/ethpandaops/provoor/pkg/cluster"
	"github.com/ethpandaops/provoor/pkg/serve"
)

// Prover proves stateless payloads for one provisioned guest program.
type Prover struct {
	client      *Client
	programName string
	version     string
	sdkVersion  string
}

// NewProver derives the guest ELF's program name, confirms the deployment's
// loadout carries the program, and waits for every worker's registration.
// Programs are provisioned at deploy time, so a mismatched ELF fails before
// any server port opens instead of being refused on the first proof.
func NewProver(ctx context.Context, endpoint string, elf []byte, elfSource, versionPrefix string) (*Prover, error) {
	name := programName(elf)
	client := DialClient(endpoint)
	if err := client.CheckProgram(ctx, name); err != nil {
		return nil, err
	}
	if err := client.WaitReady(ctx); err != nil {
		return nil, err
	}

	guestName := cluster.GuestELFName(elfSource)
	sdkVersion := cluster.SdkVersionFromELFName(elfSource, "openvm")
	version := fmt.Sprintf("%s/openvm/%s", versionPrefix, guestName)
	if sdkVersion != "" {
		version = fmt.Sprintf("%s/openvm-%s/%s", versionPrefix, sdkVersion, guestName)
	}
	return &Prover{client: client, programName: name, version: version, sdkVersion: sdkVersion}, nil
}

// SdkVersion is the zkVM SDK version the guest ELF name carries, empty when
// unnamed.
func (p *Prover) SdkVersion() string {
	return p.sdkVersion
}

// ProgramName is the guest program's loadout name, its content digest.
func (p *Prover) ProgramName() string {
	return p.programName
}

// ClientVersion identifies the prover and guest for run records.
func (p *Prover) ClientVersion() string {
	return p.version
}

// Prove proves one stateless payload, bounded by the context deadline.
func (p *Prover) Prove(ctx context.Context, input []byte, onPhase func(phase string)) (*serve.ProveOutcome, error) {
	result, err := p.client.Prove(ctx, p.programName, input, onPhase)
	if err != nil {
		return nil, err
	}
	return &serve.ProveOutcome{
		PublicValues:       result.PublicValues,
		ProofBytes:         result.ProofBytes,
		ClusterProvingTime: result.ClusterProvingTime,
	}, nil
}

// Close releases the coordinator connection.
func (p *Prover) Close() error {
	return p.client.Close()
}
