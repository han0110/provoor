package openvm

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
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
	"time"
)

const (
	// requestTimeout caps one plain HTTP request in total, sized for
	// multi-megabyte input uploads and proof downloads.
	requestTimeout = 300 * time.Second
	// connectTimeout bounds dialing the coordinator.
	connectTimeout = 10 * time.Second
	// submitRetryInterval paces prove submission retries while the cluster
	// is busy or not ready.
	submitRetryInterval = 5 * time.Second
	// statusStreamReadTimeout drops a silent status stream, so a dead
	// connection reconnects instead of hanging.
	statusStreamReadTimeout = 60 * time.Second
	// statusStreamReconnectDelay paces reconnects of a dropped status
	// stream.
	statusStreamReconnectDelay = time.Second
	// readyTimeout bounds the startup wait for every worker's registration.
	readyTimeout = 120 * time.Second
)

// errClusterBusy reports a submission refused because the manager already
// runs a proof, it accepts one at a time, recoverable by retrying.
var errClusterBusy = errors.New("cluster busy")

// errClusterNotReady reports a submission refused while workers are still
// registering or compiling, recoverable by retrying.
var errClusterNotReady = errors.New("cluster not ready")

// ProveTimeoutError reports a proof cancelled on deadline.
type ProveTimeoutError struct {
	ProofUUID string
}

func (e *ProveTimeoutError) Error() string {
	return fmt.Sprintf("proof %s timed out", e.ProofUUID)
}

// ProveResult carries what one completed proof reports.
type ProveResult struct {
	// PublicValues is the fixed-size commitment the guest produced.
	PublicValues []byte
	// ProofBytes is the size of the returned proof envelope.
	ProofBytes int
	// ClusterProvingTime is the manager's admission-to-completion latency,
	// the same boundary the ZisK backend reports.
	ClusterProvingTime time.Duration
}

// statusError reports a status stream refused with an HTTP error, which a
// reconnect cannot cure.
type statusError struct {
	code int
	body string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("status %d: %s", e.code, e.body)
}

// Client drives prove jobs against an edge-manager coordinator's client
// HTTP API.
type Client struct {
	endpoint string
	http     *http.Client
	// stream carries the status event streams, unbounded in total duration
	// with silence policed per read instead.
	stream *http.Client
}

// DialClient targets a coordinator API endpoint such as http://10.0.0.1:3000.
// Connections are lazy, an unreachable coordinator surfaces on the first
// call.
func DialClient(endpoint string) *Client {
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: connectTimeout}).DialContext}
	return &Client{
		endpoint: strings.TrimRight(endpoint, "/"),
		http:     &http.Client{Timeout: requestTimeout, Transport: transport},
		stream:   &http.Client{Transport: transport},
	}
}

// Close releases pooled coordinator connections.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	c.stream.CloseIdleConnections()
	return nil
}

// CheckProgram confirms the deployment's loadout carries the program, whose
// verification baseline the coordinator serves per name.
func (c *Client) CheckProgram(ctx context.Context, name string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/vk/"+name, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("checking program %s: %w", name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	body := readBody(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("program %s is not in the cluster loadout, deploy a cluster carrying this guest ELF: %s", name, body)
	}
	return fmt.Errorf("checking program %s: status %d: %s", name, resp.StatusCode, body)
}

// WaitReady polls the coordinator until every expected worker is registered,
// so the first proof does not queue behind cluster bring-up.
func (c *Client) WaitReady(ctx context.Context) error {
	deadline := time.Now().Add(readyTimeout)
	for {
		detail, err := c.ready(ctx)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster not ready after %s: %s", readyTimeout, detail)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
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
func (c *Client) Prove(ctx context.Context, programName string, input []byte, onPhase func(phase string)) (*ProveResult, error) {
	var proofUUID string
	for {
		proofUUID = newProofUUID()
		err := c.submit(ctx, proofUUID, programName, input)
		if err == nil {
			break
		}
		if !errors.Is(err, errClusterBusy) && !errors.Is(err, errClusterNotReady) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("prove submission timed out: %w", err)
		case <-time.After(submitRetryInterval):
		}
	}

	result, err := c.waitProof(ctx, proofUUID, onPhase)
	if err != nil && ctx.Err() != nil {
		cancelCtx, cancelCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancelCancel()
		_ = c.CancelProof(cancelCtx, proofUUID)
		return nil, &ProveTimeoutError{ProofUUID: proofUUID}
	}
	return result, err
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
func (c *Client) submit(ctx context.Context, proofUUID, programName string, input []byte) error {
	if err := c.uploadInput(ctx, proofUUID, input); err != nil {
		return err
	}
	return c.startProof(ctx, proofUUID, programName)
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
		return fmt.Errorf("uploading input for proof %s: %w", proofUUID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("uploading input for proof %s: status %d: %s", proofUUID, resp.StatusCode, readBody(resp.Body))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (c *Client) startProof(ctx context.Context, proofUUID, programName string) error {
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
		}{Name: programName, Version: programVersion},
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
		return fmt.Errorf("starting proof %s: %w", proofUUID, err)
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
// permanent, while a busy manager, absent workers, and a partially failed
// work dispatch all recover on their own.
func classifyStartError(code int, body string) error {
	switch {
	case code == http.StatusConflict && strings.Contains(body, "program_not_in_loadout"):
		return fmt.Errorf("program is not in the cluster loadout: %s", body)
	case code == http.StatusConflict:
		return fmt.Errorf("%w: %s", errClusterBusy, body)
	case code == http.StatusServiceUnavailable:
		return fmt.Errorf("%w: %s", errClusterNotReady, body)
	case code == http.StatusInternalServerError && strings.Contains(body, "failed to accept work"):
		return fmt.Errorf("%w: %s", errClusterNotReady, body)
	default:
		return fmt.Errorf("starting proof: status %d: %s", code, body)
	}
}

// waitProof follows the proof to a settled status and assembles the result
// of a completed one.
func (c *Client) waitProof(ctx context.Context, proofUUID string, onPhase func(phase string)) (*ProveResult, error) {
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
	if err := verifyProof(proof); err != nil {
		return nil, fmt.Errorf("proof %s: %w", proofUUID, err)
	}
	publicValues, err := decodeProofPublicValues(proof)
	if err != nil {
		return nil, fmt.Errorf("proof %s: %w", proofUUID, err)
	}
	return &ProveResult{
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
	for {
		status, reason, err := c.streamStatus(ctx, proofUUID, onPhase, &lastPhase)
		switch {
		case err == nil && status != "":
			return status, reason, nil
		case err != nil && errors.As(err, new(*statusError)):
			return "", "", fmt.Errorf("streaming proof %s status: %w", proofUUID, err)
		case ctx.Err() != nil:
			return "", "", ctx.Err()
		}
		// The stream dropped or ended unsettled, resubscribe.
		select {
		case <-ctx.Done():
			return "", "", ctx.Err()
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
					reportPhase(onPhase, lastPhase, status)
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

func reportPhase(onPhase func(phase string), lastPhase *string, phase string) {
	if onPhase != nil && phase != *lastPhase {
		*lastPhase = phase
		onPhase(phase)
	}
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
