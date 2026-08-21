package openvm

import (
	"context"

	"github.com/han0110/provoor/pkg/cluster"
	"github.com/han0110/provoor/pkg/serve"
)

// Prover proves stateless payloads for one provisioned guest program.
type Prover struct {
	client      *Client
	programName string
	version     string
	sdkVersion  string
}

// NewProver derives the guest ELF's program name, binds a verifier to
// programVK, and waits for every worker's registration. Programs are
// provisioned at deploy time, so an ELF or a verifying key the deployment does
// not carry fails before any server port opens instead of being refused on the
// first proof.
func NewProver(ctx context.Context, endpoint string, elf, programVK []byte, elfSource string) (*Prover, error) {
	name := programName(elf)
	client, err := DialClient(ctx, endpoint, name, programVK)
	if err != nil {
		return nil, err
	}
	if err := client.WaitReady(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}

	guestName := cluster.GuestELFName(elfSource)
	sdkVersion := cluster.SdkVersionFromELFName(elfSource, "openvm")
	return &Prover{client: client, programName: name, version: guestName, sdkVersion: sdkVersion}, nil
}

// WaitReady blocks until the manager reports every expected worker registered
// and each one answers its own readiness probe.
func (p *Prover) WaitReady(ctx context.Context) error {
	return p.client.WaitReady(ctx)
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

// ClientVersion is the guest ELF name, identifying the guest and its
// zkVM SDK version for run records.
func (p *Prover) ClientVersion() string {
	return p.version
}

// Warmup proves the shared warmup input once and discards the result.
func (p *Prover) Warmup(ctx context.Context) error {
	_, err := p.client.Prove(ctx, cluster.WarmupInput, nil)
	return err
}

// Prove proves one stateless payload, bounded by the context deadline.
func (p *Prover) Prove(ctx context.Context, input []byte, onPhase func(phase string)) (*serve.ProveOutcome, error) {
	return p.client.Prove(ctx, input, onPhase)
}

// Close releases the verifier and the coordinator connection.
func (p *Prover) Close() error {
	return p.client.Close()
}
