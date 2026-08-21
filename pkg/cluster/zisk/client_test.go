package zisk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/han0110/provoor/pkg/cluster/zisk/api"
)

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

// setupCoordinator answers every job with a completed setup reporting one
// verifying key, the only cluster behaviour the setup path reads.
type setupCoordinator struct {
	api.UnimplementedZiskCoordinatorApiServer
	reportedVK []byte
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

func dialFakeCoordinator(t *testing.T, coordinator api.ZiskCoordinatorApiServer) *Client {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	api.RegisterZiskCoordinatorApiServer(server, coordinator)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	client, err := DialClient("http://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func dialSetupCoordinator(t *testing.T, reportedVK []byte) *Client {
	t.Helper()
	return dialFakeCoordinator(t, &setupCoordinator{reportedVK: reportedVK})
}

func TestSetupBindsTheConfiguredProgramVK(t *testing.T) {
	programVK := readFixture(t, "program_vk.bin")
	client := dialSetupCoordinator(t, programVK)

	if err := client.Setup(t.Context(), "hash", programVK); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(client.boundProgramVK(), programVK) {
		t.Errorf("bound program vk = %x, want %x", client.boundProgramVK(), programVK)
	}
	// The bound verifier is reachable only through a verification, so a
	// malformed envelope standing in for a proof shows it is in place.
	if _, err := client.verify([]byte("not-an-envelope")); err == nil {
		t.Error("verify() = nil, want the bound verifier to reject a malformed envelope")
	}
}

// TestSetupRejectsAnotherClusterProgramVK covers the cross-check, since a
// cluster deriving another key from the registered ELF proves a program the
// configured key does not describe.
func TestSetupRejectsAnotherClusterProgramVK(t *testing.T) {
	configured := readFixture(t, "program_vk.bin")
	reported := readFixture(t, "cluster-program-vk.bin")
	client := dialSetupCoordinator(t, reported)

	err := client.Setup(t.Context(), "hash", configured)
	if err == nil {
		t.Fatal("Setup() = nil, want a program vk mismatch")
	}
	for _, key := range [][]byte{configured, reported} {
		if !strings.Contains(err.Error(), fmt.Sprintf("%x", key)) {
			t.Errorf("err = %v, want it to print %x", err, key)
		}
	}
	if client.boundProgramVK() != nil {
		t.Error("a mismatch must leave no verifier bound")
	}
}

func TestSetupRequiresAProgramVK(t *testing.T) {
	client := &Client{}
	if err := client.Setup(t.Context(), "hash", nil); err == nil {
		t.Error("Setup() = nil, want a refusal to set up without a program vk")
	}
}

// refusingCoordinator completes every setup and refuses every prove
// submission for want of a setup, the answer a cluster gives when one worker
// parked itself idle while the coordinator still records the program. It
// counts submissions, since the recovery that answers such a refusal is served
// from the coordinator's cache and returns at once.
type refusingCoordinator struct {
	setupCoordinator
	mu      sync.Mutex
	submits int
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

// TestSubmitProveJobPacesSetupRecovery pins the pacing of the recovery branch.
// Recovering a setup costs one cached round trip, so a branch that retried
// without waiting would submit for as long as the cluster kept refusing, at
// whatever rate the coordinator could answer.
func TestSubmitProveJobPacesSetupRecovery(t *testing.T) {
	programVK := readFixture(t, "program_vk.bin")
	coordinator := &refusingCoordinator{setupCoordinator: setupCoordinator{reportedVK: programVK}}
	client := dialFakeCoordinator(t, coordinator)
	if err := client.Setup(t.Context(), "hash", programVK); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	if _, err := client.submitProveJob(ctx, "hash", nil, nil); err == nil {
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
	_, err := client.createProveJob(t.Context(), "hash", oversized)
	var tooLarge *InputTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("err = %v, want InputTooLargeError", err)
	}
}
