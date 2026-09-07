package ereserver

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/han0110/provoor/internal/ereserver/api"
)

func TestExecuteEstimatedCost(t *testing.T) {
	stdin := []byte("stateless input")
	peakHeapBytes := uint64(4096)
	cost := map[string]uint64{"main": 7, "memory": 3}
	cases := []struct {
		name       string
		status     int
		body       []byte
		want       *CostEstimation
		wantGuest  string
		wantErrors []string
	}{
		{
			name:   "ok",
			status: http.StatusOK,
			body:   okBody(t, &api.ExecuteEstimatedCostOk{Cost: cost, PeakHeapBytes: &peakHeapBytes}),
			want:   &CostEstimation{Cost: cost, PeakHeapBytes: &peakHeapBytes},
		},
		{
			name:   "ok without peak heap",
			status: http.StatusOK,
			body:   okBody(t, &api.ExecuteEstimatedCostOk{Cost: cost}),
			want:   &CostEstimation{Cost: cost},
		},
		{
			name:       "ok without cost",
			status:     http.StatusOK,
			body:       okBody(t, &api.ExecuteEstimatedCostOk{PeakHeapBytes: &peakHeapBytes}),
			wantErrors: []string{"no cost"},
		},
		{
			name:      "guest failure",
			status:    http.StatusOK,
			body:      errBody(t, "guest panicked at block 1"),
			wantGuest: "guest panicked at block 1",
		},
		{
			name:       "transport failure",
			status:     http.StatusInternalServerError,
			body:       []byte(`{"code":"internal","msg":"prover died"}`),
			wantErrors: []string{"500", "prover died"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := serve(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/twirp/api.ZkvmService/ExecuteEstimatedCost" {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
				}
				if got := r.Header.Get("Content-Type"); got != "application/protobuf" {
					t.Errorf("Content-Type = %q, want application/protobuf", got)
				}
				raw, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				var request api.ExecuteEstimatedCostRequest
				if err := proto.Unmarshal(raw, &request); err != nil {
					t.Error(err)
					return
				}
				if !bytes.Equal(request.InputStdin, stdin) || request.InputProofs != nil {
					t.Errorf("request = %+v, want stdin %q and no proofs", &request, stdin)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write(tc.body)
			})

			got, err := client.ExecuteEstimatedCost(t.Context(), stdin)
			var guest *GuestError
			switch {
			case tc.want != nil:
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Errorf("cost estimation = %+v, want %+v", got, tc.want)
				}
			case tc.wantGuest != "":
				if !errors.As(err, &guest) {
					t.Fatalf("err = %v, want a *GuestError", err)
				}
				if guest.Error() != tc.wantGuest {
					t.Errorf("guest error = %q, want %q", guest, tc.wantGuest)
				}
			default:
				if errors.As(err, &guest) {
					t.Fatalf("err = %v, want a transport error", err)
				}
				for _, want := range tc.wantErrors {
					if err == nil || !strings.Contains(err.Error(), want) {
						t.Errorf("err = %v, want mention of %q", err, want)
					}
				}
			}
		})
	}
}

func TestHealth(t *testing.T) {
	cases := []struct {
		name   string
		status int
		wantOK bool
	}{
		{"serving", http.StatusOK, true},
		{"busy", http.StatusServiceUnavailable, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := serve(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/health" {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
				}
				w.WriteHeader(tc.status)
			})
			if err := client.Health(t.Context()); (err == nil) != tc.wantOK {
				t.Errorf("Health = %v, want ok %t", err, tc.wantOK)
			}
		})
	}
}

// serve runs a handler for the duration of one test and returns a client of
// it.
func serve(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &Client{BaseURL: server.URL, HTTP: server.Client()}
}

func okBody(t *testing.T, ok *api.ExecuteEstimatedCostOk) []byte {
	t.Helper()
	return encodeResponse(t, &api.ExecuteEstimatedCostResponse{Result: &api.ExecuteEstimatedCostResponse_Ok{Ok: ok}})
}

func errBody(t *testing.T, message string) []byte {
	t.Helper()
	return encodeResponse(t, &api.ExecuteEstimatedCostResponse{Result: &api.ExecuteEstimatedCostResponse_Err{Err: message}})
}

func encodeResponse(t *testing.T, response *api.ExecuteEstimatedCostResponse) []byte {
	t.Helper()
	body, err := proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
