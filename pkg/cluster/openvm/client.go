package openvm

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/han0110/provoor/pkg/cluster"
	"github.com/han0110/provoor/pkg/ereverifier"
	"github.com/han0110/provoor/pkg/serve"
)

const (
	// requestTimeout caps one plain HTTP request in total, sized for
	// multi-megabyte input uploads and proof downloads.
	requestTimeout = 300 * time.Second
	// connectTimeout bounds dialing the coordinator.
	connectTimeout = 10 * time.Second
	// submitRetryInterval paces prove submission retries while the cluster is
	// busy, not ready, or off the network.
	submitRetryInterval = 5 * time.Second
	// statusStreamReadTimeout drops a silent status stream, so a dead
	// connection reconnects instead of hanging.
	statusStreamReadTimeout = 60 * time.Second
	// statusStreamReconnectDelay paces reconnects of a dropped status
	// stream.
	statusStreamReconnectDelay = time.Second
	// readyPollInterval paces the wait for every worker's registration.
	readyPollInterval = 3 * time.Second
)

// errClusterBusy reports a submission refused because the manager already
// runs a proof, it accepts one at a time, recoverable by retrying.
var errClusterBusy = errors.New("cluster busy")

// errClusterNotReady reports a submission refused while workers are still
// registering or compiling, recoverable by retrying.
var errClusterNotReady = errors.New("cluster not ready")

// errClusterUnreachable reports a submission that never reached the
// coordinator, recoverable by retrying since a restart cures it.
var errClusterUnreachable = errors.New("cluster unreachable")

// ProveTimeoutError reports a proof cancelled on deadline. Cause carries the
// last stream failure when one kept the proof from settling.
type ProveTimeoutError struct {
	ProofUUID string
	Cause     error
}

func (e *ProveTimeoutError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("proof %s timed out: %v", e.ProofUUID, e.Cause)
	}
	return fmt.Sprintf("proof %s timed out", e.ProofUUID)
}

func (e *ProveTimeoutError) Unwrap() error { return e.Cause }

// statusError reports a status stream refused with an HTTP error, which a
// reconnect cannot cure.
type statusError struct {
	code int
	body string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("status %d: %s", e.code, e.body)
}

// Client drives prove jobs for one program against an edge-manager
// coordinator's client HTTP API.
type Client struct {
	endpoint    string
	programName string
	http        *http.Client
	// stream carries the status event streams, unbounded in total duration
	// with silence policed per read instead.
	stream *http.Client
	// verifier is bound to the caller's program verifying key for the
	// client's whole life, so no proof can be accepted unverified. Its guard
	// is held for reading across a verification, since the handle it frees
	// must not be released while a proof is still being checked against it.
	verifierMu sync.RWMutex
	verifier   *ereverifier.Verifier
}

// DialClient targets a coordinator API endpoint such as http://10.0.0.1:3000
// and binds a verifier to programVK, so an unreachable coordinator surfaces
// here instead of on the first proof. The verification baseline the
// coordinator serves for the program is a cross-check of programVK rather than
// its source, so a cluster proving anything else is refused before it proves
// it. The caller releases the client with Close.
func DialClient(ctx context.Context, endpoint, programName string, programVK []byte) (*Client, error) {
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: connectTimeout}).DialContext}
	client := &Client{
		endpoint:    strings.TrimRight(endpoint, "/"),
		programName: programName,
		http:        &http.Client{Timeout: requestTimeout, Transport: transport},
		stream:      &http.Client{Transport: transport},
	}
	verifier, err := ereverifier.New(ereverifier.OpenVM, programVK)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("binding a verifier to program %s: %w", programName, err)
	}
	client.verifier = verifier
	clusterVerifyingKey, err := client.fetchProgramVerifyingKey(ctx)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	if !bytes.Equal(clusterVerifyingKey, programVK) {
		_ = client.Close()
		// A baseline runs to hundreds of bytes and two of them differ in a
		// handful, so the digests are what a reader can compare, against
		// sha256sum of the configured key as deployment reports it.
		return nil, fmt.Errorf("cluster serves program %s under a verifying key of sha256 %x, the configured key hashes to %x",
			programName, sha256.Sum256(clusterVerifyingKey), sha256.Sum256(programVK))
	}
	return client, nil
}

// Close releases the verifier and pooled coordinator connections. The
// verifier is freed under the write guard, so a proof still being checked
// keeps its handle alive until it is done with it.
func (c *Client) Close() error {
	c.verifierMu.Lock()
	c.verifier.Close()
	c.verifier = nil
	c.verifierMu.Unlock()

	c.http.CloseIdleConnections()
	c.stream.CloseIdleConnections()
	return nil
}

// verify checks a proof against the bound verifier, holding the binding for
// as long as the verifier is in use.
func (c *Client) verify(proof []byte) ([]byte, error) {
	c.verifierMu.RLock()
	defer c.verifierMu.RUnlock()

	if c.verifier == nil {
		return nil, errors.New("the client is closed")
	}
	return verifyProof(c.verifier, proof)
}

// fetchProgramVerifyingKey reads the program's verification baseline, which
// the coordinator serves per name, and so also confirms the deployment's
// loadout carries the program.
func (c *Client) fetchProgramVerifyingKey(ctx context.Context) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/vk/"+c.programName, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching the verifying key of program %s: %w", c.programName, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK {
		programVerifyingKey, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("fetching the verifying key of program %s: %w", c.programName, err)
		}
		return programVerifyingKey, nil
	}
	body := readBody(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("program %s is not in the cluster loadout, deploy a cluster carrying this guest ELF: %s", c.programName, body)
	}
	return nil, fmt.Errorf("fetching the verifying key of program %s: status %d: %s", c.programName, resp.StatusCode, body)
}

// WaitReady polls the coordinator until every expected worker is registered,
// so the first proof does not queue behind cluster bring-up. The wait carries
// no budget of its own and ends only with the caller's context, since a wait
// cut short leaves the rest of the cluster's recovery to land inside the next
// proof's measurement.
func (c *Client) WaitReady(ctx context.Context) error {
	for {
		detail, err := c.ready(ctx)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cluster not ready: %s", detail)
		case <-time.After(readyPollInterval):
		}
	}
}

func (c *Client) ready(ctx context.Context) (detail string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/readyz", nil)
	if err != nil {
		return "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err.Error(), err
	}
	defer func() { _ = resp.Body.Close() }()
	body := readBody(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return body, fmt.Errorf("cluster not ready: %s", body)
	}
	return "", nil
}

// Prove submits an input, waits for the proof, and cancels it when the
// context expires first. Submission retries while the manager is busy with
// another proof or its workers are not ready, bounded by the same context.
func (c *Client) Prove(ctx context.Context, input []byte, onPhase func(phase string)) (*serve.ProveOutcome, error) {
	proofUUID, submitWait, err := c.submitProof(ctx, input)
	if err != nil {
		return nil, err
	}

	result, err := c.waitProof(ctx, proofUUID, onPhase)
	if err != nil && ctx.Err() != nil {
		cancelCtx, cancelCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancelCancel()
		_ = c.CancelProof(cancelCtx, proofUUID)
		return nil, &ProveTimeoutError{ProofUUID: proofUUID, Cause: err}
	}
	if err == nil {
		result.SubmitWait = submitWait
	}
	return result, err
}

// submitProof submits one proof, waiting out a manager busy with another, a
// coordinator whose workers are not ready, and one a restart has taken off
// the network. Every one of them comes back on its own, so only the caller's
// context ends the wait, and the retries are paced since an unpaced branch
// would resubmit as fast as the manager could refuse. It returns the admitted
// proof's identifier and how long the refused attempts took, which is not
// time the cluster spent proving the input.
func (c *Client) submitProof(ctx context.Context, input []byte) (string, time.Duration, error) {
	started := time.Now()
	var submitWait time.Duration
	for {
		// The wait runs to the attempt that lands, so the submission carrying
		// the input stays in the caller's measurement.
		submitWait = time.Since(started)
		proofUUID := newProofUUID()
		err := c.submit(ctx, proofUUID, input)
		if err == nil {
			return proofUUID, submitWait, nil
		}
		switch {
		case errors.Is(err, errClusterUnreachable),
			errors.Is(err, errClusterBusy),
			errors.Is(err, errClusterNotReady):
		default:
			return "", 0, err
		}
		select {
		case <-ctx.Done():
			return "", 0, fmt.Errorf("prove submission timed out: %w", err)
		case <-time.After(submitRetryInterval):
		}
	}
}

// newProofUUID generates the client-side proof identifier, a random 128-bit
// value in hex, within the charset the manager accepts as a directory name.
func newProofUUID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}

// submit stages the input and starts the proof, one fresh identifier per
// attempt so a lost admission response cannot collide with itself.
func (c *Client) submit(ctx context.Context, proofUUID string, input []byte) error {
	if err := c.uploadInput(ctx, proofUUID, input); err != nil {
		return err
	}
	return c.startProof(ctx, proofUUID)
}

func (c *Client) uploadInput(ctx context.Context, proofUUID string, input []byte) error {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("input", "input.bin")
	if err != nil {
		return err
	}
	if _, err := part.Write(framedStdin(input)); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/upload_input/"+proofUUID, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: uploading input for proof %s: %w", errClusterUnreachable, proofUUID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusServiceUnavailable {
		return fmt.Errorf("%w: uploading input for proof %s: %s", errClusterNotReady, proofUUID, readBody(resp.Body))
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("uploading input for proof %s: status %d: %s", proofUUID, resp.StatusCode, readBody(resp.Body))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *Client) startProof(ctx context.Context, proofUUID string) error {
	payload, err := json.Marshal(struct {
		ProofUUID string `json:"proof_uuid"`
		Program   struct {
			Name    string `json:"name"`
			Version int    `json:"version"`
		} `json:"program"`
		InputAlreadyUploaded bool `json:"input_already_uploaded"`
	}{
		ProofUUID: proofUUID,
		Program: struct {
			Name    string `json:"name"`
			Version int    `json:"version"`
		}{Name: c.programName, Version: programVersion},
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/start_proof", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: starting proof %s: %w", errClusterUnreachable, proofUUID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return classifyStartError(resp.StatusCode, readBody(resp.Body))
}

// classifyStartError maps an admission failure onto the recoverable
// conditions the prove loop handles. A rejected loadout membership is
// permanent, while a busy manager, absent workers, and a proof the manager
// took and then could not place all recover on their own.
func classifyStartError(code int, body string) error {
	switch {
	case code == http.StatusConflict && strings.Contains(body, "program_not_in_loadout"):
		return fmt.Errorf("program is not in the cluster loadout: %s", body)
	case code == http.StatusConflict:
		return fmt.Errorf("%w: %s", errClusterBusy, body)
	case code == http.StatusServiceUnavailable:
		return fmt.Errorf("%w: %s", errClusterNotReady, body)
	// A manager aborts a proof it already took either because a worker
	// refused the dispatch or because the input fan-out failed. The first
	// holds the cluster until the aborted proof drains and the second
	// releases it before answering, so both clear without intervention.
	case code == http.StatusInternalServerError &&
		(strings.Contains(body, "failed to accept work") || strings.Contains(body, "upload input to workers")):
		return fmt.Errorf("%w: %s", errClusterNotReady, body)
	default:
		return fmt.Errorf("starting proof: status %d: %s", code, body)
	}
}

// waitProof follows the proof to a settled status and assembles the result
// of a completed one.
func (c *Client) waitProof(ctx context.Context, proofUUID string, onPhase func(phase string)) (*serve.ProveOutcome, error) {
	status, reason, err := c.awaitSettled(ctx, proofUUID, onPhase)
	if err != nil {
		return nil, err
	}
	switch status {
	case "completed":
	case "canceled":
		return nil, fmt.Errorf("proof %s was cancelled", proofUUID)
	default:
		return nil, fmt.Errorf("proof %s failed: %s", proofUUID, reason)
	}

	latency, err := c.proofLatency(ctx, proofUUID)
	if err != nil {
		return nil, err
	}
	proof, err := c.downloadProof(ctx, proofUUID)
	if err != nil {
		return nil, err
	}
	publicValues, err := c.verify(proof)
	if err != nil {
		return nil, fmt.Errorf("proof %s: %w", proofUUID, err)
	}
	return &serve.ProveOutcome{
		PublicValues:       publicValues,
		ProofBytes:         len(proof),
		ClusterProvingTime: latency,
	}, nil
}

// awaitSettled follows the proof's status event stream until it settles,
// reporting transitions through onPhase when non-nil. The server replays the
// current status on subscribe, so a dropped stream reconnects without loss.
func (c *Client) awaitSettled(ctx context.Context, proofUUID string, onPhase func(phase string)) (status, reason string, err error) {
	lastPhase := ""
	// The last stream failure is kept so an expiring context reports why the
	// proof never settled rather than a bare deadline.
	var lastErr error
	expired := func() (string, string, error) {
		if lastErr != nil {
			return "", "", fmt.Errorf("streaming proof %s status: %w", proofUUID, lastErr)
		}
		return "", "", ctx.Err()
	}
	for {
		status, reason, err := c.streamStatus(ctx, proofUUID, onPhase, &lastPhase)
		switch {
		case err == nil && status != "":
			return status, reason, nil
		case err != nil && errors.As(err, new(*statusError)):
			return "", "", fmt.Errorf("streaming proof %s status: %w", proofUUID, err)
		case ctx.Err() != nil:
			return expired()
		}
		if err != nil {
			lastErr = err
		}
		// The stream dropped or ended unsettled, resubscribe.
		select {
		case <-ctx.Done():
			return expired()
		case <-time.After(statusStreamReconnectDelay):
		}
	}
}

// streamStatus reads one status event stream until the proof settles,
// returning an empty status when the stream ends or drops first. Silence
// past the read timeout cancels the request, surfacing as a dropped stream.
func (c *Client) streamStatus(ctx context.Context, proofUUID string, onPhase func(phase string), lastPhase *string) (string, string, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(streamCtx, http.MethodGet, c.endpoint+"/proof_events/"+proofUUID, nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := c.stream.Do(req)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", "", &statusError{code: resp.StatusCode, body: readBody(resp.Body)}
	}

	watchdog := time.AfterFunc(statusStreamReadTimeout, cancel)
	defer watchdog.Stop()

	scanner := bufio.NewScanner(resp.Body)
	event, data := "", ""
	for scanner.Scan() {
		watchdog.Reset(statusStreamReadTimeout)
		line := scanner.Text()
		switch {
		case line == "":
			if event == "status" && data != "" {
				status, reason, err := parseProofStatus(data)
				if err != nil {
					return "", "", err
				}
				if !settled(status) {
					cluster.ReportPhase(onPhase, lastPhase, status)
				}
				if settled(status) {
					return status, reason, nil
				}
			}
			event, data = "", ""
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
	}
	return "", "", scanner.Err()
}

// parseProofStatus decodes one externally tagged status value, a bare JSON
// string for the reasonless variants and a single-key object otherwise.
func parseProofStatus(data string) (status, reason string, err error) {
	var plain string
	if json.Unmarshal([]byte(data), &plain) == nil {
		return plain, "", nil
	}
	var tagged map[string]string
	if err := json.Unmarshal([]byte(data), &tagged); err != nil {
		return "", "", fmt.Errorf("parsing proof status %q: %w", data, err)
	}
	for status, reason := range tagged {
		return status, reason, nil
	}
	return "", "", fmt.Errorf("empty proof status %q", data)
}

// settled reports whether a status is terminal. The transient failing state
// always becomes failed, so it stays a phase.
func settled(status string) bool {
	return status == "completed" || status == "failed" || status == "canceled"
}

// proofLatency reads the settled proof's admission-to-completion latency.
// The manager evicts settled proofs from memory after a few minutes, which
// this read directly after settling stays well inside.
func (c *Client) proofLatency(ctx context.Context, proofUUID string) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/proof_state/"+proofUUID, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("reading proof %s state: %w", proofUUID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("reading proof %s state: status %d: %s", proofUUID, resp.StatusCode, readBody(resp.Body))
	}
	var state struct {
		E2ELatencyMs *float64 `json:"e2e_latency_ms"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return 0, fmt.Errorf("reading proof %s state: %w", proofUUID, err)
	}
	if state.E2ELatencyMs == nil {
		return 0, fmt.Errorf("proof %s reported no e2e latency", proofUUID)
	}
	return time.Duration(*state.E2ELatencyMs * float64(time.Millisecond)), nil
}

// downloadProof fetches the persisted proof envelope, served from disk so it
// survives the manager's in-memory eviction.
func (c *Client) downloadProof(ctx context.Context, proofUUID string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/proof/"+proofUUID, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("downloading proof %s: %w", proofUUID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("downloading proof %s: status %d: %s", proofUUID, resp.StatusCode, readBody(resp.Body))
	}
	proof, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("downloading proof %s: %w", proofUUID, err)
	}
	return proof, nil
}

// CancelProof cancels a proof, a no-op on one already settled.
func (c *Client) CancelProof(ctx context.Context, proofUUID string) error {
	payload, err := json.Marshal(struct {
		ProofUUID string `json:"proof_uuid"`
	}{ProofUUID: proofUUID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/cancel_proof", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cancelling proof %s: %w", proofUUID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("cancelling proof %s: status %d: %s", proofUUID, resp.StatusCode, readBody(resp.Body))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// framedStdin wraps a payload as the serialized guest stdin, one buffered
// byte string and no deferrals, the layout bincode's legacy configuration
// gives the SDK's StdIn, fixed-width little-endian integers with u64 length
// prefixes.
func framedStdin(data []byte) []byte {
	framed := make([]byte, 24+len(data))
	binary.LittleEndian.PutUint64(framed, 1)
	binary.LittleEndian.PutUint64(framed[8:], uint64(len(data)))
	copy(framed[16:], data)
	return framed
}

func readBody(body io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(body, 4096))
	return strings.TrimSpace(string(raw))
}
