package zisk

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/han0110/provoor/internal/ereverifier"
	"github.com/han0110/provoor/internal/zisk/api"
)

// setupCoordinator answers every job with a completed setup reporting one
// verifying key, the only cluster behaviour the setup path reads.
type setupCoordinator struct {
	api.UnimplementedZiskCoordinatorApiServer
	reportedVK []byte
}

// refusingCoordinator completes every setup and refuses every prove
// submission for want of a setup. It counts submissions, since the recovery
// that answers such a refusal is served from the coordinator's cache and
// returns at once.
type refusingCoordinator struct {
	setupCoordinator
	mu      sync.Mutex
	submits int
}

func (s *setupCoordinator) JobRequest(context.Context, *api.JobRequestMessage) (*api.JobResponse, error) {
	return &api.JobResponse{JobId: "job"}, nil
}

func (s *setupCoordinator) WaitJobResult(context.Context, *api.WaitJobResultRequest) (*api.WaitJobResultResponse, error) {
	return &api.WaitJobResultResponse{
		JobId:     "job",
		JobStatus: &api.JobStatus{Status: &api.JobStatus_Completed{Completed: &api.JobStatusCompleted{}}},
		Result: &api.JobKindResponse{Kind: &api.JobKindResponse_Setup{
			Setup: &api.SetupResponse{Vk: s.reportedVK},
		}},
	}, nil
}

func (r *refusingCoordinator) JobRequest(ctx context.Context, request *api.JobRequestMessage) (*api.JobResponse, error) {
	if request.GetJobKind().GetSetup() != nil {
		return r.setupCoordinator.JobRequest(ctx, request)
	}
	r.mu.Lock()
	r.submits++
	r.mu.Unlock()
	return nil, status.Error(codes.Unavailable, "Cluster unavailable: workers connected but setup not done; call setup() first")
}

func (r *refusingCoordinator) submissions() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.submits
}

// dialFakeCoordinator serves coordinator on loopback and returns a client
// bound to programVK with a registered hash.
func dialFakeCoordinator(t *testing.T, coordinator api.ZiskCoordinatorApiServer, programVK []byte) *Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	api.RegisterZiskCoordinatorApiServer(server, coordinator)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	client, err := newClient("http://"+listener.Addr().String(), programVK)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	client.HashID = "hash"
	return client
}

// stubSupervisor speaks the supervisor's half, answering only when it refuses
// and otherwise closing, which is how a landed request reads on the wire.
func stubSupervisor(t *testing.T, refusal string) (address string, verb <-chan string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	// A channel rather than a shared variable, since the socket closing is
	// not an ordering the race detector recognises.
	seen := make(chan string, 1)
	go func() {
		defer close(seen)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		line, _ := bufio.NewReader(conn).ReadString('\n')
		if refusal != "" {
			_, _ = io.WriteString(conn, refusal+"\n")
		}
		seen <- strings.TrimSpace(line)
	}()
	return listener.Addr().String(), seen
}

func boundToFixture(t *testing.T, programVK []byte) *ereverifier.Verifier {
	t.Helper()
	verifier, err := ereverifier.New(ereverifier.Zisk, programVK)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(verifier.Close)
	return verifier
}

func TestFramedStdin(t *testing.T) {
	cases := []struct {
		data []byte
		want []byte
	}{
		{nil, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{[]byte{0xaa}, []byte{1, 0, 0, 0, 0, 0, 0, 0, 0xaa, 0, 0, 0, 0, 0, 0, 0}},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{8, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}},
	}
	for _, tc := range cases {
		if got := framedStdin(tc.data); !bytes.Equal(got, tc.want) {
			t.Errorf("framedStdin(%x) = %x, want %x", tc.data, got, tc.want)
		}
	}
	if got := framedStdin(make([]byte, 9)); len(got) != 24 {
		t.Errorf("nine data bytes frame to %d bytes, want 24", len(got))
	}
}

func TestClassifySubmitError(t *testing.T) {
	setupNotDone := status.Error(codes.FailedPrecondition, "setup not done for program")
	if !errors.Is(classifySubmitError(setupNotDone), errSetupNotDone) {
		t.Error("expected a setup-not-done classification")
	}
	for _, code := range []codes.Code{codes.Unavailable, codes.Internal} {
		if !errors.Is(classifySubmitError(status.Error(code, "down")), errClusterUnavailable) {
			t.Errorf("expected %v to classify as cluster unavailable", code)
		}
	}
	invalid := status.Error(codes.InvalidArgument, "bad input")
	classified := classifySubmitError(invalid)
	if errors.Is(classified, errSetupNotDone) || errors.Is(classified, errClusterUnavailable) {
		t.Error("expected an invalid argument to stay unclassified")
	}
}

func TestSetupBindsTheConfiguredProgramVK(t *testing.T) {
	programVK := readFixture(t, "program_vk.bin")
	client := dialFakeCoordinator(t, &setupCoordinator{reportedVK: programVK}, programVK)

	if err := client.setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.verify([]byte("not-an-envelope")); err == nil {
		t.Error("verify() = nil, want a malformed envelope rejected")
	}
	publicValues, err := client.verify(fixtureEnvelope(t).encode())
	if err != nil {
		t.Fatal(err)
	}
	if want := readFixture(t, "public_values.bin"); !bytes.Equal(publicValues, want) {
		t.Errorf("public values = %x, want %x", publicValues, want)
	}
}

func TestSetupRejectsAnotherClusterProgramVK(t *testing.T) {
	configured := readFixture(t, "program_vk.bin")
	reported := readFixture(t, "cluster-program-vk.bin")
	client := dialFakeCoordinator(t, &setupCoordinator{reportedVK: reported}, configured)

	err := client.setup(t.Context())
	if err == nil {
		t.Fatal("setup() = nil, want a program vk mismatch")
	}
	for _, key := range [][]byte{configured, reported} {
		if !strings.Contains(err.Error(), fmt.Sprintf("%x", key)) {
			t.Errorf("err = %v, want it to print %x", err, key)
		}
	}
}

func TestSubmitProveJobPacesSetupRecovery(t *testing.T) {
	programVK := readFixture(t, "program_vk.bin")
	coordinator := &refusingCoordinator{setupCoordinator: setupCoordinator{reportedVK: programVK}}
	client := dialFakeCoordinator(t, coordinator, programVK)
	if err := client.setup(t.Context()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if _, _, err := client.submitProveJob(ctx, nil, func(string) {}); err == nil {
		t.Fatal("submitProveJob() = nil, want the deadline to end the wait")
	}
	// submitRetryInterval outlasts the deadline, so a paced loop submits once.
	if got := coordinator.submissions(); got > 1 {
		t.Errorf("submissions = %d, want the loop paced by submitRetryInterval", got)
	}
}

func TestInputTooLarge(t *testing.T) {
	oversized := make([]byte, maxMessageBytes)
	client := &Client{}
	_, err := client.createProveJob(t.Context(), oversized)
	if err == nil || !strings.Contains(err.Error(), "exceeds the") {
		t.Fatalf("err = %v, want the inline cap to reject the input", err)
	}
}

func TestRestartAddress(t *testing.T) {
	for _, tc := range []struct{ endpoint, want string }{
		{"http://10.0.0.1:7000", "10.0.0.1:7002"},
		{"10.0.0.1:7000", "10.0.0.1:7002"},
	} {
		got, err := restartAddress(tc.endpoint)
		if err != nil || got != tc.want {
			t.Errorf("restartAddress(%q) = %q, %v", tc.endpoint, got, err)
		}
	}
	if _, err := restartAddress("http://10.0.0.1"); err == nil {
		t.Error("an endpoint without a port has no supervisor address to derive")
	}
}

func TestAskRestartSendsTheVerb(t *testing.T) {
	address, seen := stubSupervisor(t, "")
	if err := askRestart(t.Context(), address); err != nil {
		t.Fatalf("askRestart: %v", err)
	}
	if verb := <-seen; verb != "restart" {
		t.Errorf("supervisor was sent %q, want %q", verb, "restart")
	}
}

func TestAskRestartReportsARefusal(t *testing.T) {
	address, _ := stubSupervisor(t, "expected the verb restart")
	err := askRestart(t.Context(), address)
	if err == nil || !strings.Contains(err.Error(), "expected the verb restart") {
		t.Errorf("err = %v, want the supervisor's answer reported", err)
	}
}

func TestAskRestartFailsWhenNoSupervisorAnswers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	if err := askRestart(t.Context(), address); err == nil {
		t.Error("an unreachable supervisor has to fail the forwarder")
	}
}

func TestAskRestartFailsWhenTheSupervisorGoesSilent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	silent := t.Context()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		<-silent.Done()
		_ = conn.Close()
	}()

	ctx, cancel := context.WithTimeout(silent, time.Second)
	defer cancel()
	if err := askRestart(ctx, listener.Addr().String()); err == nil {
		t.Error("a supervisor that never answers has to fail the forwarder")
	}
}

// TestVerifyProof runs the fixture envelope through the verifier ere ships,
// the only authority on whether the transcode produces what it accepts.
func TestVerifyProof(t *testing.T) {
	verifier := boundToFixture(t, readFixture(t, "program_vk.bin"))

	publicValues, err := verifyProof(verifier, fixtureEnvelope(t).encode())
	if err != nil {
		t.Fatal(err)
	}
	if want := readFixture(t, "public_values.bin"); !bytes.Equal(publicValues, want) {
		t.Errorf("public values = %x, want %x", publicValues, want)
	}
}

// TestVerifyProofClusterEnvelope verifies an envelope a coordinator really
// returned against the verifying key that cluster derived, so the transcode
// is pinned against the encoding a live cluster emits.
func TestVerifyProofClusterEnvelope(t *testing.T) {
	verifier := boundToFixture(t, readFixture(t, "cluster-program-vk.bin"))

	publicValues, err := verifyProof(verifier, readFixture(t, "cluster-envelope.bin"))
	if err != nil {
		t.Fatalf("verifyProof() = %v, want a verified proof", err)
	}
	if want := readFixture(t, "cluster-public-values.bin"); !bytes.Equal(publicValues, want) {
		t.Errorf("public values = %x, want %x", publicValues, want)
	}
}

func TestVerifyProofClusterEnvelopeRejectsAnotherProgram(t *testing.T) {
	verifier := boundToFixture(t, readFixture(t, "program_vk.bin"))

	_, err := verifyProof(verifier, readFixture(t, "cluster-envelope.bin"))
	if !errors.Is(err, ereverifier.ErrVerify) {
		t.Errorf("err = %v, want a verification failure", err)
	}
}

func TestVerifyProofRejects(t *testing.T) {
	cases := []struct {
		name      string
		programVK func([]byte) []byte
		mutate    func(*envelope)
	}{
		{
			name:      "wrong program vk",
			programVK: func(vk []byte) []byte { return append([]byte{vk[0] ^ 0xff}, vk[1:]...) },
		},
		{
			name:   "corrupted proof",
			mutate: func(e *envelope) { e.proofWords[len(e.proofWords)/2] ^= 1 },
		},
		{
			name:   "corrupted public values",
			mutate: func(e *envelope) { e.publicValues[programVKWords] ^= 0xff },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			programVK := readFixture(t, "program_vk.bin")
			if tc.programVK != nil {
				programVK = tc.programVK(programVK)
			}
			e := fixtureEnvelope(t)
			if tc.mutate != nil {
				tc.mutate(&e)
			}
			_, err := verifyProof(boundToFixture(t, programVK), e.encode())
			if !errors.Is(err, ereverifier.ErrVerify) {
				t.Errorf("err = %v, want %v", err, ereverifier.ErrVerify)
			}
		})
	}
}
