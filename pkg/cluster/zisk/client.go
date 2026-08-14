package zisk

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/han0110/provoor/pkg/cluster"
	"github.com/han0110/provoor/pkg/cluster/zisk/api"
	"github.com/han0110/provoor/pkg/ereverifier"
	"github.com/han0110/provoor/pkg/serve"
)

const (
	// maxMessageBytes bounds one gRPC message in both directions, the same
	// cap the coordinator enforces on inline input. An input past it is
	// rejected before submission with a legible error.
	maxMessageBytes = 128 << 20
	// setupTimeout bounds the one-time keygen job for a guest program.
	setupTimeout = 600 * time.Second
	// submitRetryInterval paces prove submission retries while the cluster
	// is unavailable.
	submitRetryInterval = 5 * time.Second
	// waitHoldSeconds is the server-side hold per WaitJobResult poll.
	waitHoldSeconds = 5
)

// errSetupNotDone reports a prove submission refused because the guest
// program has no completed setup, recoverable by running Setup.
var errSetupNotDone = errors.New("setup not done")

// errClusterUnavailable reports a submission failure worth retrying, the
// cluster being unreachable or momentarily failing internally.
var errClusterUnavailable = errors.New("cluster unavailable")

// InputTooLargeError reports an input whose framed form exceeds the
// coordinator's inline message cap.
type InputTooLargeError struct {
	FramedBytes int
}

func (e *InputTooLargeError) Error() string {
	return fmt.Sprintf("framed input of %d bytes exceeds the %d byte inline cap", e.FramedBytes, maxMessageBytes)
}

// ProveTimeoutError reports a prove job cancelled on deadline.
type ProveTimeoutError struct {
	JobID string
}

func (e *ProveTimeoutError) Error() string {
	return fmt.Sprintf("prove job %s timed out", e.JobID)
}

// Client drives guest program registration and prove jobs against a
// coordinator's client-facing gRPC API.
type Client struct {
	conn *grpc.ClientConn
	api  api.ZiskCoordinatorApiClient

	// verifierMu guards the verifier binding. It is held for reading across a
	// verification, since the binding frees a native handle that must not be
	// released while a proof is still being checked against it.
	verifierMu sync.RWMutex
	programVK  []byte
	verifier   *ereverifier.Verifier
}

// DialClient connects to a coordinator API endpoint such as
// http://10.0.0.1:7000. The connection is lazy, an unreachable coordinator
// surfaces on the first call.
func DialClient(endpoint string) (*Client, error) {
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
	return &Client{conn: conn, api: api.NewZiskCoordinatorApiClient(conn)}, nil
}

// Close releases the verifier and the coordinator connection. The verifier is
// freed under the write guard, so a proof still being checked keeps its handle
// alive until it is done with it.
func (c *Client) Close() error {
	c.verifierMu.Lock()
	c.verifier.Close()
	c.verifier = nil
	c.verifierMu.Unlock()

	return c.conn.Close()
}

// RegisterGuestProgram uploads a guest ELF and returns its content hash.
// Registration is idempotent, the same ELF always returns the same hash.
func (c *Client) RegisterGuestProgram(ctx context.Context, elf []byte) (string, error) {
	resp, err := c.api.RegisterGuestProgram(ctx, &api.RegisterGuestProgramRequest{ZiskElf: elf})
	if err != nil {
		return "", fmt.Errorf("registering guest program: %w", err)
	}
	return resp.HashId, nil
}

// Setup runs the keygen job for a registered guest program and blocks until
// it completes, binding the client to expectedProgramVK. The coordinator
// caches the result, so repeating it is cheap.
func (c *Client) Setup(ctx context.Context, hashID string, expectedProgramVK []byte) error {
	if len(expectedProgramVK) == 0 {
		return fmt.Errorf("setting up guest program %s without a program verifying key", hashID)
	}

	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()

	job := &api.JobRequestMessage{JobKind: &api.JobKind{Kind: &api.JobKind_Setup{Setup: &api.SetupRequest{
		HashId: hashID,
	}}}}
	submitted, err := c.api.JobRequest(ctx, job)
	if err != nil {
		return fmt.Errorf("submitting setup job: %w", err)
	}

	result, err := c.waitJob(ctx, submitted.JobId, nil)
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
	// The key the client verifies against is the configured one, so a cluster
	// deriving another key from the ELF it registered is proving a different
	// program and every proof it returns would be rejected anyway.
	if !bytes.Equal(setup.Setup.Vk, expectedProgramVK) {
		return fmt.Errorf("cluster reports program vk %x for guest program %s, expected %x",
			setup.Setup.Vk, hashID, expectedProgramVK)
	}
	return c.bindVerifier(expectedProgramVK)
}

// bindVerifier binds the client to a program verifying key, the Merkle root
// of the guest program's ROM trace. A client proves one program, so a later
// binding to another key means it was handed a second program and is refused
// rather than replaced.
func (c *Client) bindVerifier(programVK []byte) error {
	c.verifierMu.Lock()
	defer c.verifierMu.Unlock()

	if c.verifier != nil {
		if !bytes.Equal(c.programVK, programVK) {
			return fmt.Errorf("binding to program vk %x, already bound to %x", programVK, c.programVK)
		}
		return nil
	}
	verifier, err := ereverifier.New(ereverifier.Zisk, programVK)
	if err != nil {
		return fmt.Errorf("binding verifier to program vk %x: %w", programVK, err)
	}
	c.programVK, c.verifier = programVK, verifier
	return nil
}

// boundProgramVK returns the key the client is bound to, nil before the
// first setup.
func (c *Client) boundProgramVK() []byte {
	c.verifierMu.RLock()
	defer c.verifierMu.RUnlock()

	return c.programVK
}

// verify checks a proof envelope against the bound verifier, holding the
// binding for as long as the verifier is in use.
func (c *Client) verify(envelope []byte) ([]byte, error) {
	c.verifierMu.RLock()
	defer c.verifierMu.RUnlock()

	if c.verifier == nil {
		return nil, errors.New("no verifier bound, setup has not completed")
	}
	return verifyProof(c.verifier, envelope)
}

// createProveJob submits one prove job for an input and returns its job id
// without waiting for completion.
func (c *Client) createProveJob(ctx context.Context, hashID string, input []byte) (string, error) {
	framed := framedStdin(input)
	if len(framed) > maxMessageBytes {
		return "", &InputTooLargeError{FramedBytes: len(framed)}
	}

	job := &api.JobRequestMessage{JobKind: &api.JobKind{Kind: &api.JobKind_Prove{Prove: &api.ProveRequest{
		HashId:    hashID,
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

// WaitProveJob blocks until a prove job terminates, reporting phase
// transitions through onPhase when non-nil.
func (c *Client) WaitProveJob(ctx context.Context, jobID string, onPhase func(phase string)) (*serve.ProveOutcome, error) {
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
	return &serve.ProveOutcome{
		PublicValues:       publicValues,
		ProofBytes:         len(prove.Prove.Proof.Data),
		ClusterProvingTime: time.Duration(prove.Prove.Stats.DurationNanos), //nolint:gosec // nanoseconds fit
	}, nil
}

// CancelProveJob cancels a job, returning false when it already terminated.
func (c *Client) CancelProveJob(ctx context.Context, jobID string) (bool, error) {
	resp, err := c.api.CancelJob(ctx, &api.CancelJobRequest{JobId: jobID})
	if err != nil {
		return false, err
	}
	return resp.Cancelled, nil
}

// Prove submits an input, waits for the proof, and cancels the job when the
// context expires first. Submission retries while the cluster is unavailable
// and recovers a missing setup, both bounded by the same context.
func (c *Client) Prove(ctx context.Context, hashID string, input []byte, onPhase func(phase string)) (*serve.ProveOutcome, error) {
	var jobID string
	for {
		var err error
		jobID, err = c.createProveJob(ctx, hashID, input)
		if err == nil {
			break
		}
		switch {
		case errors.Is(err, errSetupNotDone):
			// A coordinator restart drops its setups, so the key to recover
			// against is the one the client already binds.
			if err := c.Setup(ctx, hashID, c.boundProgramVK()); err != nil {
				return nil, err
			}
		case errors.Is(err, errClusterUnavailable):
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("prove job submission timed out: %w", err)
			case <-time.After(submitRetryInterval):
			}
		default:
			return nil, err
		}
	}

	result, err := c.WaitProveJob(ctx, jobID, onPhase)
	if err != nil && ctx.Err() != nil {
		cancelCtx, cancelCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancelCancel()
		_, _ = c.CancelProveJob(cancelCtx, jobID)
		return nil, &ProveTimeoutError{JobID: jobID}
	}
	return result, err
}

// waitJob polls a job until it terminates and returns its result, reporting
// status transitions through onPhase when non-nil.
func (c *Client) waitJob(ctx context.Context, jobID string, onPhase func(phase string)) (*api.JobKindResponse, error) {
	holdSeconds := uint32(waitHoldSeconds)
	request := &api.WaitJobResultRequest{JobId: jobID, TimeoutSeconds: &holdSeconds}
	lastPhase := ""
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
			cluster.ReportPhase(onPhase, &lastPhase, "queued")
		case *api.JobStatus_Running:
			cluster.ReportPhase(onPhase, &lastPhase, runningPhase(jobStatus.Running.GetPhase()))
		case *api.JobStatus_WaitingForInput:
			cluster.ReportPhase(onPhase, &lastPhase, "waiting for input")
		}
	}
}

func runningPhase(phase api.JobPhase) string {
	switch phase {
	case api.JobPhase_JOB_PHASE_CONTRIBUTIONS:
		return "contributions"
	case api.JobPhase_JOB_PHASE_PROVE:
		return "prove"
	case api.JobPhase_JOB_PHASE_AGGREGATE:
		return "aggregate"
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

// framedStdin returns data with a little-endian u64 length prefix, padded
// with zeros to a multiple of eight bytes, the layout the guest stdin reader
// expects.
func framedStdin(data []byte) []byte {
	framed := make([]byte, (8+len(data)+7)/8*8)
	binary.LittleEndian.PutUint64(framed, uint64(len(data)))
	copy(framed[8:], data)
	return framed
}
