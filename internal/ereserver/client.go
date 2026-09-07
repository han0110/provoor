// Package ereserver estimates the proving cost of a guest program through an
// ere-server, either one already reachable or a local container it starts.
package ereserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/han0110/provoor/internal/ereserver/api"
)

const (
	executeEstimatedCostPath = "/twirp/api.ZkvmService/ExecuteEstimatedCost"
	healthPath               = "/health"
	// protobufContentType selects the protobuf codec, since twirp answers any
	// other content type in a JSON encoding of its own.
	protobufContentType = "application/protobuf"
	// errorBodyLimit caps how much of a failed response an error quotes.
	errorBodyLimit = 4096
)

// Image returns the ere-server image reference for a zkVM and a version tag
// without the leading v.
func Image(zkvm, version string) string {
	return "ghcr.io/eth-act/ere/ere-server-" + zkvm + ":" + version
}

// CostEstimation is one execution's cost per component and peak heap use.
type CostEstimation struct {
	// Cost is the cost per component. Each zkVM defines the unit.
	Cost map[string]uint64 `json:"cost"`
	// PeakHeapBytes is absent when the estimator cannot read the guest heap.
	PeakHeapBytes *uint64 `json:"peak_heap_bytes,omitempty"`
}

// GuestError is a deterministic failure reported by the zkVM for one input.
// The server keeps serving, so another input can still succeed.
type GuestError struct{ Message string }

func (e *GuestError) Error() string { return e.Message }

// Client calls one ere-server over Twirp.
type Client struct {
	// BaseURL is the server origin, such as http://127.0.0.1:4174.
	BaseURL string
	// HTTP runs the requests. An estimation takes as long as the guest needs,
	// so its timeout comes from the context instead.
	HTTP *http.Client
}

// Health reports whether the server accepts work.
func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url(healthPath), nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health status %d: %s", resp.StatusCode, readBody(resp.Body))
	}
	return nil
}

// ExecuteEstimatedCost executes the guest program on stdin and returns what
// the execution costs. A failure of the guest itself comes back as a
// *GuestError, every other failure as a transport error.
func (c *Client) ExecuteEstimatedCost(ctx context.Context, stdin []byte) (*CostEstimation, error) {
	// input_proofs stays unset, since every backend refuses proof composition
	// for an estimation.
	payload, err := proto.Marshal(&api.ExecuteEstimatedCostRequest{InputStdin: stdin})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(executeEstimatedCostPath), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", protobufContentType)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("execute_estimated_cost status %d: %s", resp.StatusCode, readBody(resp.Body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var response api.ExecuteEstimatedCostResponse
	if err := proto.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("decoding the execute_estimated_cost response: %w", err)
	}
	switch result := response.Result.(type) {
	case *api.ExecuteEstimatedCostResponse_Ok:
		// An estimation without a cost kind has nothing to record.
		if len(result.Ok.Cost) == 0 {
			return nil, errors.New("execute_estimated_cost returned no cost")
		}
		return &CostEstimation{Cost: result.Ok.Cost, PeakHeapBytes: result.Ok.PeakHeapBytes}, nil
	case *api.ExecuteEstimatedCostResponse_Err:
		return nil, &GuestError{Message: result.Err}
	default:
		return nil, errors.New("execute_estimated_cost returned no result")
	}
}

func (c *Client) url(path string) string {
	return strings.TrimRight(c.BaseURL, "/") + path
}

func readBody(body io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(body, errorBodyLimit))
	return strings.TrimSpace(string(raw))
}
