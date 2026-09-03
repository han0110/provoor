package zisk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/han0110/provoor/internal/cluster"
	"github.com/han0110/provoor/internal/ereverifier"
	"github.com/han0110/provoor/internal/zisk/api"
)

const (
	// maxMessageBytes bounds one gRPC message in both directions, the cap the
	// coordinator enforces on inline input.
	maxMessageBytes = 128 << 20
	// setupTimeout bounds the one-time keygen job for a guest program.
	setupTimeout = 600 * time.Second
	// submitRetryInterval paces submissions while the cluster is unavailable.
	submitRetryInterval = 5 * time.Second
	// waitHoldSeconds is the server-side hold per WaitJobResult poll.
	waitHoldSeconds = 5
	// restartTimeout bounds the request that ends the coordinator. The wait
	// for its replacement is not part of it.
	restartTimeout = 30 * time.Second
	// registerTimeout bounds one registration RPC.
	registerTimeout = 60 * time.Second
	// clusterReadyBudget bounds the wait for a cluster not yet ready to take a
	// registration or a setup. It covers every worker reconnecting after the
	// coordinator restart Dial asks for.
	clusterReadyBudget = 300 * time.Second
	// restartVerb is the line cmd/zisk-supervisor acts on.
	restartVerb = "restart"
)

// errSetupNotDone reports a prove submission refused because the guest
// program has no completed setup, recoverable by a setup.
var errSetupNotDone = errors.New("setup not done")

// errClusterUnavailable reports a submission failure worth retrying.
var errClusterUnavailable = errors.New("cluster unavailable")

// Client proves one registered guest program against a coordinator's client
// API.
type Client struct {
	// HashID is the registered guest program's content hash.
	HashID string

	conn *grpc.ClientConn
	api  api.ZiskCoordinatorApiClient
	// verifierMu is held for reading across a verification, since Close
	// frees a native handle.
	verifierMu sync.RWMutex
	verifier   *ereverifier.Verifier
	programVK  []byte
}

// Dial restarts the coordinator, registers the guest ELF, and runs the
// program setup, checking the key the cluster derives against programVK. The
// restart clears the guests the coordinator would otherwise replay to every
// registering worker, so the registration and the setup wait out the cluster
// coming back.
func Dial(ctx context.Context, endpoint string, elf, programVK []byte) (*Client, error) {
	if err := restartCoordinator(ctx, endpoint); err != nil {
		return nil, err
	}
	c, err := newClient(endpoint, programVK)
	if err != nil {
		return nil, err
	}
	if err := c.register(ctx, elf); err != nil {
		_ = c.Close()
		return nil, err
	}
	readyCtx, cancel := context.WithTimeout(ctx, clusterReadyBudget)
	defer cancel()
	for {
		err := c.setup(readyCtx)
		if err == nil {
			return c, nil
		}
		// A mismatched key never becomes right, so only an unavailable
		// cluster is waited out.
		if !errors.Is(err, errClusterUnavailable) || cluster.Sleep(readyCtx, submitRetryInterval) != nil {
			_ = c.Close()
			return nil, err
		}
	}
}

// newClient connects lazily to a coordinator API endpoint such as
// http://10.0.0.1:7000 and binds the verifier to programVK.
func newClient(endpoint string, programVK []byte) (*Client, error) {
	target := strings.TrimPrefix(endpoint, "http://")
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("dialing coordinator %s: %w", endpoint, err)
	}
	verifier, err := ereverifier.New(ereverifier.Zisk, programVK)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("binding verifier to program vk %x: %w", programVK, err)
	}
	return &Client{conn: conn, api: api.NewZiskCoordinatorApiClient(conn), verifier: verifier, programVK: programVK}, nil
}

// WaitReady returns at once. A ZisK coordinator exposes no readiness a client
// can trust, so Prove waits out a refused submission instead.
func (c *Client) WaitReady(context.Context) error {
	return nil
}

// Prove submits an input, waits for the proof, and cancels the job when the
// context expires first.
func (c *Client) Prove(ctx context.Context, input []byte, onPhase func(phase string)) (*cluster.ProveOutcome, error) {
	jobID, submitWait, err := c.submitProveJob(ctx, input, onPhase)
	if err != nil {
		return nil, err
	}

	outcome, err := c.waitProveJob(ctx, jobID, onPhase)
	if err != nil && ctx.Err() != nil {
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = c.cancelProveJob(cancelCtx, jobID)
		return nil, fmt.Errorf("prove job %s timed out: %w", jobID, err)
	}
	if err != nil {
		return nil, err
	}
	outcome.SubmitWait = submitWait
	return outcome, nil
}

// Close releases the verifier and the coordinator connection. The verifier
// is freed under the write guard, so a proof still being checked keeps its
// handle alive until it is done with it.
func (c *Client) Close() error {
	c.verifierMu.Lock()
	closed := c.verifier == nil
	c.verifier.Close()
	c.verifier = nil
	c.verifierMu.Unlock()

	if closed {
		return nil
	}
	return c.conn.Close()
}

// restartCoordinator ends the running coordinator and returns once it has
// exited, leaving the restart policy to start its replacement. The replacement
// is not waited for, since a published port answers whether or not the
// container behind it is up.
func restartCoordinator(ctx context.Context, endpoint string) error {
	address, err := restartAddress(endpoint)
	if err != nil {
		return err
	}
	return askRestart(ctx, address)
}

// askRestart sends the verb the supervisor acts on. The supervisor writes
// back only to refuse and holds the connection until the coordinator has
// exited, so a clean end of stream is the restart landing and any other read
// failure leaves the coordinator's state unknown.
func askRestart(ctx context.Context, address string) error {
	ctx, cancel := context.WithTimeout(ctx, restartTimeout)
	defer cancel()

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("reaching the coordinator supervisor on %s: %w", address, err)
	}
	defer func() { _ = conn.Close() }()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if _, err := io.WriteString(conn, restartVerb+"\n"); err != nil {
		return fmt.Errorf("asking %s to restart the coordinator: %w", address, err)
	}
	refusal, err := bufio.NewReader(conn).ReadString('\n')
	if answer := strings.TrimSpace(refusal); answer != "" {
		return fmt.Errorf("coordinator supervisor on %s answered %q", address, answer)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("waiting for the coordinator on %s to end: %w", address, err)
	}
	return nil
}

// restartAddress is the supervisor's address on the host the coordinator API
// endpoint names, so a caller configures one endpoint rather than two.
func restartAddress(endpoint string) (string, error) {
	host, _, err := net.SplitHostPort(strings.TrimPrefix(endpoint, "http://"))
	if err != nil {
		return "", fmt.Errorf("coordinator endpoint %q: %w", endpoint, err)
	}
	return net.JoinHostPort(host, strconv.Itoa(coordinatorRestartPort)), nil
}

// register uploads the guest ELF and records its content hash, waiting out
// the coordinator restart. Registration is idempotent and content addressed.
func (c *Client) register(ctx context.Context, elf []byte) error {
	ctx, cancel := context.WithTimeout(ctx, clusterReadyBudget)
	defer cancel()
	for {
		registerCtx, cancelRegister := context.WithTimeout(ctx, registerTimeout)
		resp, err := c.api.RegisterGuestProgram(registerCtx, &api.RegisterGuestProgramRequest{ZiskElf: elf})
		cancelRegister()
		if err == nil {
			c.HashID = resp.HashId
			return nil
		}
		if cluster.Sleep(ctx, submitRetryInterval) != nil {
			return fmt.Errorf("registering guest program: %w", err)
		}
	}
}

// setup runs the cached keygen job for the registered guest program and
// blocks until it completes. A cluster deriving another key from the ELF
// proves a different program, so the reported key has to match the bound one.
func (c *Client) setup(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()

	job := &api.JobRequestMessage{JobKind: &api.JobKind{Kind: &api.JobKind_Setup{Setup: &api.SetupRequest{
		HashId: c.HashID,
	}}}}
	submitted, err := c.api.JobRequest(ctx, job)
	if err != nil {
		// Classified so a caller can tell a cluster that is not ready yet from
		// a setup that never succeeds.
		return fmt.Errorf("submitting setup job: %w", classifySubmitError(err))
	}

	result, err := c.waitJob(ctx, submitted.JobId, func(string) {})
	if err != nil {
		return err
	}
	setup, ok := result.Kind.(*api.JobKindResponse_Setup)
	if !ok {
		return fmt.Errorf("setup job %s answered a non-setup result", submitted.JobId)
	}
	if hashMode := setup.Setup.HashMode; hashMode != "" && hashMode != vadcopFinalHashFamily {
		return fmt.Errorf("cluster hash family %q, expected %s", hashMode, vadcopFinalHashFamily)
	}
	if !bytes.Equal(setup.Setup.Vk, c.programVK) {
		return fmt.Errorf("cluster reports program vk %x for guest program %s, expected %x",
			setup.Setup.Vk, c.HashID, c.programVK)
	}
	return nil
}

// submitProveJob submits one prove job, pacing retries while the cluster is
// short of workers or recovering a missing setup, since a coordinator answers
// a cached setup at once. It returns the admitted job's identifier and how
// long the refused attempts took.
func (c *Client) submitProveJob(ctx context.Context, input []byte, onPhase func(phase string)) (string, time.Duration, error) {
	started := time.Now()
	for {
		// The wait runs to the attempt that lands, so the submission carrying
		// the input stays in the caller's measurement.
		submitWait := time.Since(started)
		jobID, err := c.createProveJob(ctx, input)
		if err == nil {
			return jobID, submitWait, nil
		}
		switch {
		case errors.Is(err, errSetupNotDone):
			if err := c.setup(ctx); err != nil {
				return "", 0, err
			}
		case errors.Is(err, errClusterUnavailable):
		default:
			return "", 0, err
		}
		onPhase("waiting for the cluster")
		if cluster.Sleep(ctx, submitRetryInterval) != nil {
			return "", 0, fmt.Errorf("prove job submission timed out: %w", err)
		}
	}
}

// createProveJob submits one prove job and returns its job id without
// waiting for completion.
func (c *Client) createProveJob(ctx context.Context, input []byte) (string, error) {
	framed := framedStdin(input)
	if len(framed) > maxMessageBytes {
		return "", fmt.Errorf("framed input of %d bytes exceeds the %d byte inline cap", len(framed), maxMessageBytes)
	}

	job := &api.JobRequestMessage{JobKind: &api.JobKind{Kind: &api.JobKind_Prove{Prove: &api.ProveRequest{
		HashId:    c.HashID,
		Input:     &api.InputKind{Kind: &api.InputKind_Inline{Inline: &api.InputChunk{Data: framed}}},
		ProofDest: api.ProofKind_PROOF_KIND_STARK_MINIMAL,
	}}}}
	submitted, err := c.api.JobRequest(ctx, job)
	if err != nil {
		return "", classifySubmitError(err)
	}
	return submitted.JobId, nil
}

// classifySubmitError maps a submission failure onto the recoverable
// conditions the prove loop handles.
func classifySubmitError(err error) error {
	grpcStatus, ok := status.FromError(err)
	if !ok {
		return err
	}
	if strings.Contains(grpcStatus.Message(), "setup not done") {
		return fmt.Errorf("%w: %s", errSetupNotDone, grpcStatus.Message())
	}
	if grpcStatus.Code() == codes.Unavailable || grpcStatus.Code() == codes.Internal {
		return fmt.Errorf("%w: %s", errClusterUnavailable, grpcStatus.Message())
	}
	return err
}

// waitProveJob blocks until a prove job terminates and assembles the verified
// outcome of a completed one.
func (c *Client) waitProveJob(ctx context.Context, jobID string, onPhase func(phase string)) (*cluster.ProveOutcome, error) {
	result, err := c.waitJob(ctx, jobID, onPhase)
	if err != nil {
		return nil, err
	}
	prove, ok := result.Kind.(*api.JobKindResponse_Prove)
	if !ok {
		return nil, fmt.Errorf("prove job %s answered a non-prove result", jobID)
	}
	if prove.Prove.Proof == nil || prove.Prove.Stats == nil {
		return nil, fmt.Errorf("prove job %s answered without proof or stats", jobID)
	}
	publicValues, err := c.verify(prove.Prove.Proof.Data)
	if err != nil {
		return nil, fmt.Errorf("prove job %s: %w", jobID, err)
	}
	return &cluster.ProveOutcome{
		PublicValues:       publicValues,
		ProofBytes:         len(prove.Prove.Proof.Data),
		ClusterProvingTime: time.Duration(prove.Prove.Stats.DurationNanos),
	}, nil
}

// waitJob polls a job until it terminates and returns its result, reporting
// every observed status through onPhase.
func (c *Client) waitJob(ctx context.Context, jobID string, onPhase func(phase string)) (*api.JobKindResponse, error) {
	holdSeconds := uint32(waitHoldSeconds)
	request := &api.WaitJobResultRequest{JobId: jobID, TimeoutSeconds: &holdSeconds}
	for {
		resp, err := c.api.WaitJobResult(ctx, request)
		if err != nil {
			return nil, fmt.Errorf("waiting for job %s: %w", jobID, err)
		}
		if resp.JobStatus == nil || resp.JobStatus.Status == nil {
			return nil, fmt.Errorf("job %s answered without a status", jobID)
		}

		switch jobStatus := resp.JobStatus.Status.(type) {
		case *api.JobStatus_Completed:
			if resp.Result == nil {
				return nil, fmt.Errorf("job %s completed without a result", jobID)
			}
			return resp.Result, nil
		case *api.JobStatus_Failed:
			return nil, fmt.Errorf("job %s failed: %s", jobID, failureReason(jobStatus.Failed.Failure))
		case *api.JobStatus_Cancelled:
			return nil, fmt.Errorf("job %s was cancelled", jobID)
		case *api.JobStatus_Queued:
			onPhase("queued")
		case *api.JobStatus_Running:
			onPhase(runningPhase(jobStatus.Running.GetPhase()))
		case *api.JobStatus_WaitingForInput:
			onPhase("waiting for input")
		}
	}
}

func runningPhase(phase api.JobPhase) string {
	switch phase {
	case api.JobPhase_JOB_PHASE_CONTRIBUTIONS:
		return "contributions"
	case api.JobPhase_JOB_PHASE_PROVE:
		return "prove"
	case api.JobPhase_JOB_PHASE_RECURSE:
		return "recurse"
	default:
		return "running"
	}
}

func failureReason(failure *api.JobFailure) string {
	switch kind := failure.GetKind().(type) {
	case *api.JobFailure_Timeout:
		return fmt.Sprintf("timeout in phase %s", kind.Timeout.GetPhase())
	case *api.JobFailure_Input:
		return "input: " + kind.Input.Reason
	case *api.JobFailure_Execution:
		return "execution: " + kind.Execution.Reason
	case *api.JobFailure_Internal:
		return "internal error, trace " + kind.Internal.TraceId
	case *api.JobFailure_Cancelled:
		return "cancelled"
	default:
		return "unknown failure"
	}
}

func (c *Client) cancelProveJob(ctx context.Context, jobID string) error {
	_, err := c.api.CancelJob(ctx, &api.CancelJobRequest{JobId: jobID})
	return err
}

// framedStdin returns data with a little-endian u64 length prefix, padded
// with zeros to a multiple of eight bytes, the layout the guest stdin reader
// expects.
func framedStdin(data []byte) []byte {
	framed := make([]byte, (8+len(data)+7)/8*8)
	binary.LittleEndian.PutUint64(framed, uint64(len(data)))
	copy(framed[8:], data)
	return framed
}

// verify checks a proof envelope against the bound verifier, holding the
// binding for as long as the verifier is in use.
func (c *Client) verify(envelope []byte) ([]byte, error) {
	c.verifierMu.RLock()
	defer c.verifierMu.RUnlock()

	if c.verifier == nil {
		return nil, errors.New("the client is closed")
	}
	return verifyProof(c.verifier, envelope)
}

// verifyProof transcodes a proof envelope into the encoding ere's verifier
// accepts and verifies it against the bound program verifying key. The
// transcode rejects a malformed envelope on its own, so a decode failure here
// means the transcode emitted something the verifier cannot read.
func verifyProof(verifier *ereverifier.Verifier, envelope []byte) ([]byte, error) {
	proof, err := transcodeProof(envelope)
	if err != nil {
		return nil, err
	}
	publicValues, err := verifier.Verify(proof)
	switch {
	case errors.Is(err, ereverifier.ErrDecodeProof):
		return nil, fmt.Errorf("decoding the transcoded proof: %w", err)
	case errors.Is(err, ereverifier.ErrVerify):
		return nil, fmt.Errorf("verifying the proof: %w", err)
	case err != nil:
		return nil, fmt.Errorf("running the verifier: %w", err)
	}
	return publicValues, nil
}
