package zisk

import (
	"context"
	"time"

	"github.com/han0110/provoor/pkg/cluster"
	"github.com/han0110/provoor/pkg/serve"
)

// registerTimeout bounds the startup registration RPC, so an unreachable
// coordinator fails startup instead of hanging.
const registerTimeout = 60 * time.Second

// Prover proves stateless payloads for one registered guest program.
type Prover struct {
	client     *Client
	hashID     string
	version    string
	sdkVersion string
}

// NewProver connects to the coordinator, registers the guest ELF, and runs
// the program setup. Registration is content addressed, so the registered
// bytes are what identify the guest and the ELF source only names it for run
// records.
func NewProver(ctx context.Context, endpoint string, elf []byte, elfSource string) (*Prover, error) {
	client, err := DialClient(endpoint)
	if err != nil {
		return nil, err
	}
	registerCtx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()
	hashID, err := client.RegisterGuestProgram(registerCtx, elf)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	if err := client.Setup(ctx, hashID); err != nil {
		_ = client.Close()
		return nil, err
	}

	guestName := cluster.GuestELFName(elfSource)
	sdkVersion := cluster.SdkVersionFromELFName(elfSource, "zisk")
	return &Prover{client: client, hashID: hashID, version: guestName, sdkVersion: sdkVersion}, nil
}

// SdkVersion is the zkVM SDK version the guest ELF name carries, empty when
// unnamed.
func (p *Prover) SdkVersion() string {
	return p.sdkVersion
}

// HashID is the registered guest program's content hash.
func (p *Prover) HashID() string {
	return p.hashID
}

// ClientVersion is the guest ELF name, identifying the guest and its
// zkVM SDK version for run records.
func (p *Prover) ClientVersion() string {
	return p.version
}

// Warmup proves the shared warmup input once and discards the result.
func (p *Prover) Warmup(ctx context.Context) error {
	_, err := p.client.Prove(ctx, p.hashID, cluster.WarmupInput, nil)
	return err
}

// Prove proves one stateless payload, bounded by the context deadline.
func (p *Prover) Prove(ctx context.Context, input []byte, onPhase func(phase string)) (*serve.ProveOutcome, error) {
	return p.client.Prove(ctx, p.hashID, input, onPhase)
}

// Close releases the coordinator connection.
func (p *Prover) Close() error {
	return p.client.Close()
}
