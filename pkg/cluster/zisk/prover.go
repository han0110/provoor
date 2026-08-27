package zisk

import (
	"context"
	"errors"
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

// setupRetryBudget bounds the wait for a cluster that is not ready to take a
// setup. It covers every worker reconnecting after the coordinator restart
// this prover asks for, which is the only reason the refusal is expected.
const setupRetryBudget = 300 * time.Second

// NewProver restarts the coordinator, registers the guest ELF, and runs the
// program setup, binding a verifier to programVK and failing when the cluster
// derives another key from the same ELF. Registration is content addressed, so
// the registered bytes are what identify the guest and the ELF source only
// names it for run records.
//
// The restart comes first because the coordinator never forgets a guest it has
// set up and replays every one of them to each worker that registers, which
// corrupts all but the last. Starting from an empty record leaves this guest
// the only one the workers are ever asked to set up.
func NewProver(ctx context.Context, endpoint string, elf, programVK []byte, elfSource string) (*Prover, error) {
	if err := RestartCoordinator(ctx, endpoint); err != nil {
		return nil, err
	}

	client, err := DialClient(endpoint)
	if err != nil {
		return nil, err
	}
	hashID, err := registerWhenReady(ctx, client, elf)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	if err := setupWhenReady(ctx, client, hashID, programVK); err != nil {
		_ = client.Close()
		return nil, err
	}

	guestName := cluster.GuestELFName(elfSource)
	sdkVersion := cluster.SdkVersionFromELFName(elfSource, "zisk")
	return &Prover{client: client, hashID: hashID, version: guestName, sdkVersion: sdkVersion}, nil
}

// registerWhenReady registers the guest ELF, waiting out the coordinator this
// prover just ended while its replacement starts. Registration is idempotent
// and content addressed, so repeating it costs nothing.
func registerWhenReady(ctx context.Context, client *Client, elf []byte) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, setupRetryBudget)
	defer cancel()
	for {
		registerCtx, cancelRegister := context.WithTimeout(ctx, registerTimeout)
		hashID, err := client.RegisterGuestProgram(registerCtx, elf)
		cancelRegister()
		if err == nil {
			return hashID, nil
		}
		select {
		case <-ctx.Done():
			return "", err
		case <-time.After(submitRetryInterval):
		}
	}
}

// setupWhenReady runs the guest setup, waiting out a cluster still short of
// workers. The restart this prover asks for drops every worker connection, so
// the coordinator refuses a setup it accepts once they are back. Every other
// failure is returned at once, since a mismatched key never becomes right.
func setupWhenReady(ctx context.Context, client *Client, hashID string, programVK []byte) error {
	ctx, cancel := context.WithTimeout(ctx, setupRetryBudget)
	defer cancel()
	for {
		err := client.Setup(ctx, hashID, programVK)
		if err == nil || !errors.Is(err, errClusterUnavailable) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(submitRetryInterval):
		}
	}
}

// WaitReady returns at once. A ZisK coordinator exposes no readiness a client
// can trust, its one cluster metric over-reporting both while a lost worker is
// still held and after it is dropped, so submitting is the only faithful way
// to learn whether the cluster can take a job. Prove waits one out.
func (p *Prover) WaitReady(context.Context) error {
	return nil
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
