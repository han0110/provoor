package serve

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeProver answers with fixed public values or a fixed error.
type fakeProver struct {
	publicValues []byte
	err          error
	phases       []string
}

func (p *fakeProver) ClientVersion() string {
	return "provoor/0.0.0/fake/guest"
}

func (p *fakeProver) Warmup(context.Context) error {
	return nil
}

func (p *fakeProver) Prove(_ context.Context, _ []byte, onPhase func(string)) (*ProveOutcome, error) {
	if p.err != nil {
		return nil, p.err
	}
	for _, phase := range p.phases {
		onPhase(phase)
	}
	return &ProveOutcome{
		PublicValues:       p.publicValues,
		ProofBytes:         316119,
		ClusterProvingTime: 4011 * time.Millisecond,
	}, nil
}

// commitment returns fixed-size public values carrying output in the prefix.
func commitment(output []byte) []byte {
	publicValues := make([]byte, 256)
	copy(publicValues, output)
	return publicValues
}

func newServer(prover Prover, output *bytes.Buffer) *Server {
	return &Server{
		Prover: prover,
		Output: output,
		Exit:   func(int) { panic("unexpected exit") },
	}
}

func post(t *testing.T, server *Server, body string) map[string]any {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/", strings.NewReader(body))
	server.Handler().ServeHTTP(recorder, request)
	var decoded map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("response %q: %v", recorder.Body.String(), err)
	}
	return decoded
}

func proveRequest(output []byte) string {
	payload := map[string]string{
		"blockHash":               "0xabc123",
		"blockNumber":             "0x1",
		"gasUsed":                 "0x3938700",
		"statelessInput":          "0x150102",
		"expectedStatelessOutput": "0x" + hex.EncodeToString(output),
	}
	encoded, _ := json.Marshal(payload)
	return `{"jsonrpc":"2.0","method":"engine_proveStatelessValidator","params":[` + string(encoded) + `],"id":7}`
}

func TestClientVersion(t *testing.T) {
	server := newServer(&fakeProver{}, &bytes.Buffer{})
	resp := post(t, server, `{"jsonrpc":"2.0","method":"web3_clientVersion","params":[],"id":1}`)
	if resp["result"] != "provoor/0.0.0/fake/guest" {
		t.Errorf("result = %v", resp["result"])
	}
}

func TestProveValid(t *testing.T) {
	expected := []byte{1, 2, 3, 4}
	output := &bytes.Buffer{}
	server := newServer(&fakeProver{publicValues: commitment(expected), phases: []string{"queued", "prove"}}, output)

	resp := post(t, server, proveRequest(expected))
	result := resp["result"].(map[string]any)
	if result["status"] != "VALID" {
		t.Errorf("status = %v", result["status"])
	}
	// A passing proof answers status alone, leaving measurements to the metric
	// line and echoing no output to diff.
	if _, ok := result["statelessOutput"]; ok {
		t.Errorf("VALID response should not echo the output, got %v", result)
	}
	if len(result) != 1 {
		t.Errorf("VALID response should carry status alone, got %v", result)
	}

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	var metric metricLine
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &metric); err != nil {
		t.Fatalf("metric line %q: %v", lines[len(lines)-1], err)
	}
	if metric.Block.Hash != "0xabc123" || !metric.OutputMatched || metric.StatelessInputSize != 3 {
		t.Errorf("metric = %+v", metric)
	}
	if metric.ProofSize != 316119 || metric.ClusterReportedProvingTimeMs != 4011 {
		t.Errorf("proving metric = %v/%v", metric.ProofSize, metric.ClusterReportedProvingTimeMs)
	}
	if metric.Block.Number != 1 || metric.Block.GasUsed != 60000000 {
		t.Errorf("block metric = %+v", metric.Block)
	}
	if metric.Throughput.MGasPerSec <= 0 || metric.Timing.TotalMs != metric.ProvingTimeMs {
		t.Errorf("throughput metric = %+v timing = %+v", metric.Throughput, metric.Timing)
	}
	if !strings.Contains(output.String(), "phase prove") {
		t.Errorf("expected phase lines in output, got %q", output.String())
	}
}

func TestProveMismatch(t *testing.T) {
	output := &bytes.Buffer{}
	server := newServer(&fakeProver{publicValues: commitment([]byte{9, 9, 9})}, output)

	resp := post(t, server, proveRequest([]byte{1, 2, 3}))
	result := resp["result"].(map[string]any)
	if result["status"] != "INVALID" || result["validationError"] != "stateless output mismatch" {
		t.Errorf("result = %v", result)
	}
	if !strings.Contains(output.String(), `"outputMatched":false`) {
		t.Errorf("metric should record the mismatch, got %q", output.String())
	}
}

func TestProveClusterError(t *testing.T) {
	server := newServer(&fakeProver{err: errors.New("cluster unavailable: down")}, &bytes.Buffer{})
	resp := post(t, server, proveRequest([]byte{1}))
	if resp["result"] != nil {
		t.Errorf("result = %v", resp["result"])
	}
	rpcErr := resp["error"].(map[string]any)
	if !strings.Contains(rpcErr["message"].(string), "cluster unavailable") {
		t.Errorf("error = %v", rpcErr)
	}
}

func TestProveClusterErrorFailsRun(t *testing.T) {
	exited := 0
	server := newServer(&fakeProver{err: errors.New("down")}, &bytes.Buffer{})
	server.FailRunOnClusterError = true
	server.Exit = func(code int) { exited = code; panic("exit") }

	defer func() {
		if recover() == nil || exited != 1 {
			t.Errorf("expected exit 1, got %d", exited)
		}
	}()
	post(t, server, proveRequest([]byte{1}))
}

// TestProveClientDisconnectDoesNotFailRun pins that a client going away is
// never attributed to the cluster. The prover error carries no context error,
// the shape a backend returns when it reports its own timeout type, so the
// decision has to come from the request rather than the error chain.
func TestProveClientDisconnectDoesNotFailRun(t *testing.T) {
	server := newServer(&fakeProver{err: errors.New("prove job 7 timed out")}, &bytes.Buffer{})
	server.FailRunOnClusterError = true
	server.Exit = func(int) { t.Error("a disconnected client must not fail the run") }

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest("POST", "/", strings.NewReader(proveRequest([]byte{1}))).WithContext(ctx)
	server.Handler().ServeHTTP(httptest.NewRecorder(), request)
}

func TestMethodNotFound(t *testing.T) {
	server := newServer(&fakeProver{}, &bytes.Buffer{})
	for _, method := range []string{"eth_getBlockByNumber", "engine_newPayloadV5"} {
		resp := post(t, server, `{"jsonrpc":"2.0","method":"`+method+`","params":[],"id":1}`)
		rpcErr, ok := resp["error"].(map[string]any)
		if !ok || rpcErr["code"] != float64(-32601) {
			t.Errorf("%s: error = %v", method, resp["error"])
		}
	}
}

func TestProveRejectsMalformedParams(t *testing.T) {
	server := newServer(&fakeProver{}, &bytes.Buffer{})
	cases := []string{
		`{"jsonrpc":"2.0","method":"engine_proveStatelessValidator","params":[],"id":1}`,
		`{"jsonrpc":"2.0","method":"engine_proveStatelessValidator","params":[{"statelessInput":"noprefix"}],"id":1}`,
		`{"jsonrpc":"2.0","method":"engine_proveStatelessValidator","params":[{"statelessInput":"0xzz"}],"id":1}`,
	}
	for _, body := range cases {
		resp := post(t, server, body)
		rpcErr, ok := resp["error"].(map[string]any)
		if !ok || rpcErr["code"] != float64(-32602) {
			t.Errorf("body %s: error = %v", body, resp["error"])
		}
	}
}

func TestOutputMatched(t *testing.T) {
	publicValues := commitment([]byte{1, 2, 3})
	if !outputMatched(publicValues, []byte{1, 2, 3}) {
		t.Error("expected prefix with zero tail to match")
	}
	if !outputMatched(publicValues, publicValues) {
		t.Error("expected full-length equality to match")
	}
	if outputMatched(publicValues, []byte{1, 2}) {
		t.Error("expected mismatch when the tail starts with a nonzero byte")
	}
	if outputMatched([]byte{1}, []byte{1, 2}) {
		t.Error("expected mismatch when expected is longer than the commitment")
	}
	dirty := commitment([]byte{1, 2, 3})
	dirty[200] = 1
	if outputMatched(dirty, []byte{1, 2, 3}) {
		t.Error("expected mismatch on a nonzero trailing byte")
	}
}
