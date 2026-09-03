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

	"github.com/han0110/provoor/internal/cluster"
	"github.com/han0110/provoor/internal/ereverifier"
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
	// statusStreamReconnectDelay paces reconnects of a dropped status stream.
	statusStreamReconnectDelay = time.Second
	// readyPollInterval paces the wait for every worker's registration.
	readyPollInterval = 3 * time.Second
)

var (
	// errClusterBusy reports a submission refused because the manager already
	// runs a proof, recoverable by retrying.
	errClusterBusy = errors.New("cluster busy")
	// errClusterNotReady reports a submission refused while workers are still
	// registering or compiling, recoverable by retrying.
	errClusterNotReady = errors.New("cluster not ready")
	// errClusterUnreachable reports a submission that never reached the
	// coordinator, recoverable by retrying since a restart cures it.
	errClusterUnreachable = errors.New("cluster unreachable")
	// errStreamRefused reports a status stream refused with an HTTP error,
	// which a reconnect cannot cure.
	errStreamRefused = errors.New("status stream refused")
)

// Client drives prove jobs for one program against an edge-manager
// coordinator's client HTTP API.
type Client struct {
	// ProgramName is the guest program's loadout name, its content digest.
	ProgramName string
	endpoint    string
	requests    *http.Client
	// stream carries the status event streams, unbounded in total duration
	// with silence policed per read instead.
	stream *http.Client
	// verifier is bound to the program verifying key for the client's whole
	// life. Its guard is held for reading across a verification, so the
	// handle is not freed while a proof is still checked against it.
	verifierMu sync.RWMutex
	verifier   *ereverifier.Verifier
}

// Dial targets a coordinator API endpoint such as http://10.0.0.1:3000 and
// binds a verifier to programVK, released with Close. The baseline the
// coordinator serves for the program is a cross check of programVK, so a
// cluster proving anything else is refused before it proves it.
func Dial(ctx context.Context, endpoint string, elf, programVK []byte) (*Client, error) {
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: connectTimeout}).DialContext}
	c := &Client{
		ProgramName: programName(elf),
		endpoint:    strings.TrimRight(endpoint, "/"),
		requests:    &http.Client{Timeout: requestTimeout, Transport: transport},
		stream:      &http.Client{Transport: transport},
	}
	verifier, err := ereverifier.New(ereverifier.OpenVM, programVK)
	if err != nil {
		return nil, fmt.Errorf("binding a verifier to program %s: %w", c.ProgramName, err)
	}
	c.verifier = verifier
	clusterVerifyingKey, err := c.fetchProgramVerifyingKey(ctx)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	if !bytes.Equal(clusterVerifyingKey, programVK) {
		_ = c.Close()
		// The digests are what a reader can compare against sha256sum of the
		// configured key, as the deployment reports it.
		return nil, fmt.Errorf("cluster serves program %s under a verifying key of sha256 %x, the configured key hashes to %x",
			c.ProgramName, sha256.Sum256(clusterVerifyingKey), sha256.Sum256(programVK))
	}
	return c, nil
}

// WaitReady polls the coordinator until every expected worker is registered,
// so the first proof does not queue behind cluster bring-up. The wait ends
// only with the caller's context, since a wait cut short leaves the rest of
// the recovery inside the next measurement.
func (c *Client) WaitReady(ctx context.Context) error {
	for {
		err := c.ready(ctx)
		if err == nil {
			return nil
		}
		if cluster.Sleep(ctx, readyPollInterval) != nil {
			return err
		}
	}
}

// Prove submits an input, waits for the proof, and cancels it when the
// context expires first. Submission retries while the manager is busy with
// another proof or its workers are not ready, bounded by the same context.
func (c *Client) Prove(ctx context.Context, input []byte, onPhase func(phase string)) (*cluster.ProveOutcome, error) {
	proofUUID, submitWait, err := c.submitProof(ctx, input)
	if err != nil {
		return nil, err
	}

	outcome, err := c.waitProof(ctx, proofUUID, onPhase)
	if err != nil && ctx.Err() != nil {
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = c.cancelProof(cancelCtx, proofUUID)
		return nil, fmt.Errorf("proof %s timed out: %w", proofUUID, err)
	}
	if err != nil {
		return nil, err
	}
	outcome.SubmitWait = submitWait
	return outcome, nil
}

// Close releases the verifier and pooled coordinator connections. The
// verifier is freed under the write guard, so a proof still being checked
// keeps its handle alive until it is done with it.
func (c *Client) Close() error {
	c.verifierMu.Lock()
	c.verifier.Close()
	c.verifier = nil
	c.verifierMu.Unlock()

	c.requests.CloseIdleConnections()
	c.stream.CloseIdleConnections()
	return nil
}

// fetchProgramVerifyingKey reads the program's verification baseline, which
// the coordinator serves per name, and so also confirms the deployment's
// loadout carries the program.
func (c *Client) fetchProgramVerifyingKey(ctx context.Context) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/vk/"+c.ProgramName, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.requests.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching the verifying key of program %s: %w", c.ProgramName, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK {
		programVerifyingKey, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("fetching the verifying key of program %s: %w", c.ProgramName, err)
		}
		return programVerifyingKey, nil
	}
	body := readBody(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("program %s is not in the cluster loadout, deploy a cluster carrying this guest ELF: %s", c.ProgramName, body)
	}
	return nil, fmt.Errorf("fetching the verifying key of program %s: status %d: %s", c.ProgramName, resp.StatusCode, body)
}

func (c *Client) ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/readyz", nil)
	if err != nil {
		return err
	}
	resp, err := c.requests.Do(req)
	if err != nil {
		return fmt.Errorf("cluster not ready: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cluster not ready: %s", readBody(resp.Body))
	}
	return nil
}

// submitProof submits one proof, waiting out a busy manager, unready workers,
// and a coordinator a restart took off the network until the caller's context
// ends. It returns the admitted proof's identifier and how long the refused
// attempts took.
func (c *Client) submitProof(ctx context.Context, input []byte) (string, time.Duration, error) {
	started := time.Now()
	for {
		// The wait runs to the attempt that lands, so the submission carrying
		// the input stays in the caller's measurement.
		submitWait := time.Since(started)
		// A fresh identifier per attempt keeps a lost admission response from
		// colliding with its retry.
		proofUUID := newProofUUID()
		err := c.uploadInput(ctx, proofUUID, input)
		if err == nil {
			err = c.startProof(ctx, proofUUID)
		}
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
		if cluster.Sleep(ctx, submitRetryInterval) != nil {
			return "", 0, fmt.Errorf("prove submission timed out: %w", err)
		}
	}
}

// newProofUUID is a random 128-bit identifier in hex, within the charset the
// manager accepts as a directory name.
func newProofUUID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
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
	resp, err := c.requests.Do(req)
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
	payload, err := json.Marshal(map[string]any{
		"proof_uuid":             proofUUID,
		"program":                map[string]any{"name": c.ProgramName, "version": programVersion},
		"input_already_uploaded": false,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/start_proof", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.requests.Do(req)
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
	// A worker that refused the dispatch holds the cluster until the aborted
	// proof drains and a failed input fan-out releases it before answering,
	// so both clear without intervention.
	case code == http.StatusInternalServerError &&
		(strings.Contains(body, "failed to accept work") || strings.Contains(body, "upload input to workers")):
		return fmt.Errorf("%w: %s", errClusterNotReady, body)
	default:
		return fmt.Errorf("starting proof: status %d: %s", code, body)
	}
}

// waitProof follows the proof to a settled status and assembles the outcome
// of a completed one.
func (c *Client) waitProof(ctx context.Context, proofUUID string, onPhase func(phase string)) (*cluster.ProveOutcome, error) {
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
	return &cluster.ProveOutcome{
		PublicValues:       publicValues,
		ProofBytes:         len(proof),
		ClusterProvingTime: latency,
	}, nil
}

// awaitSettled follows the proof's status event stream until it settles. The
// server replays the current status on subscribe, so a dropped stream
// reconnects without loss.
func (c *Client) awaitSettled(ctx context.Context, proofUUID string, onPhase func(phase string)) (string, string, error) {
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
		status, reason, err := c.streamStatus(ctx, proofUUID, onPhase)
		switch {
		case err == nil && status != "":
			return status, reason, nil
		case errors.Is(err, errStreamRefused):
			return "", "", fmt.Errorf("streaming proof %s status: %w", proofUUID, err)
		case ctx.Err() != nil:
			return expired()
		}
		if err != nil {
			lastErr = err
		}
		if cluster.Sleep(ctx, statusStreamReconnectDelay) != nil {
			return expired()
		}
	}
}

// streamStatus reads one status event stream until the proof settles,
// returning an empty status when the stream ends or drops first. Silence past
// the read timeout cancels the request, which surfaces as a dropped stream.
func (c *Client) streamStatus(ctx context.Context, proofUUID string, onPhase func(phase string)) (string, string, error) {
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
		return "", "", fmt.Errorf("%w: status %d: %s", errStreamRefused, resp.StatusCode, readBody(resp.Body))
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
				if settled(status) {
					return status, reason, nil
				}
				onPhase(status)
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
func parseProofStatus(data string) (string, string, error) {
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

// proofLatency reads the settled proof's admission-to-completion latency. The
// manager evicts settled proofs from memory after a few minutes, which this
// read directly after settling stays well inside.
func (c *Client) proofLatency(ctx context.Context, proofUUID string) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/proof_state/"+proofUUID, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.requests.Do(req)
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
	resp, err := c.requests.Do(req)
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

// cancelProof cancels a proof, a no-op on one already settled.
func (c *Client) cancelProof(ctx context.Context, proofUUID string) error {
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
	resp, err := c.requests.Do(req)
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
// byte string and no deferrals. The layout is bincode's legacy configuration
// of the SDK's StdIn, fixed-width little-endian integers with u64 length
// prefixes.
func framedStdin(data []byte) []byte {
	framed := make([]byte, 24+len(data))
	binary.LittleEndian.PutUint64(framed, 1)
	binary.LittleEndian.PutUint64(framed[8:], uint64(len(data)))
	copy(framed[16:], data)
	return framed
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

// verifyProof verifies a proof envelope against the program's verification
// baseline and returns the public values it proves. A malformed envelope and
// a well-formed one that fails verification are reported apart.
func verifyProof(verifier *ereverifier.Verifier, proof []byte) ([]byte, error) {
	publicValues, err := verifier.Verify(proof)
	switch {
	case errors.Is(err, ereverifier.ErrDecodeProof):
		return nil, fmt.Errorf("decoding the proof envelope: %w", err)
	case errors.Is(err, ereverifier.ErrVerify):
		return nil, fmt.Errorf("verifying the proof: %w", err)
	case err != nil:
		return nil, fmt.Errorf("running the verifier: %w", err)
	}
	return publicValues, nil
}

func readBody(body io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(body, 4096))
	return strings.TrimSpace(string(raw))
}
