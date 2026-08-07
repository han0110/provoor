package zisk

import (
	"bytes"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestInputTooLarge(t *testing.T) {
	oversized := make([]byte, maxMessageBytes)
	client := &Client{}
	_, err := client.createProveJob(t.Context(), "hash", oversized)
	var tooLarge *InputTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("err = %v, want InputTooLargeError", err)
	}
}
