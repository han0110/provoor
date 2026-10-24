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

	"github.com/google/pprof/profile"
	"google.golang.org/protobuf/proto"

	"github.com/han0110/provoor/internal/ereserver/api"
)

const (
	executeEstimatedCostPath = "/twirp/api.ZkvmService/ExecuteEstimatedCost"
	profilePath              = "/twirp/api.ZkvmService/Profile"
	healthPath               = "/health"
	// protobufContentType selects the protobuf codec, since twirp answers any
	// other content type in a JSON encoding of its own.
	protobufContentType = "application/protobuf"
	// errorBodyLimit caps how much of a failed response an error quotes.
	errorBodyLimit = 4096
)

// Image returns the ere-server image reference for a zkVM and an image tag,
// such as 0.18.0 or 77e2aae.
func Image(zkvm, version string) string {
	return "ghcr.io/eth-act/ere/ere-server-" + zkvm + ":" + version
}

// CostEstimation is one execution's cost per component and peak memory use.
type CostEstimation struct {
	// Cost is the cost per component. Each zkVM defines the unit.
	Cost map[string]uint64 `json:"cost"`
	// PeakHeapBytes is absent when the estimator cannot read the guest heap.
	PeakHeapBytes *uint64 `json:"peak_heap_bytes,omitempty"`
	// PeakStackBytes is the span of the guest stack pointer, which only a
	// profile measures.
	PeakStackBytes *uint64 `json:"peak_stack_bytes,omitempty"`
}

// CostProfile is a CostEstimation with the pprof profile that it sums.
type CostProfile struct {
	CostEstimation
	// Pprof is the pprof protobuf with the cost per guest call stack. A pprof
	// file holds it gzipped.
	Pprof []byte
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
	request := &api.ExecuteEstimatedCostRequest{InputStdin: stdin}
	var response api.ExecuteEstimatedCostResponse
	if err := c.post(ctx, executeEstimatedCostPath, "execute_estimated_cost", request, &response); err != nil {
		return nil, err
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

// Profile executes the guest program on stdin as ExecuteEstimatedCost does,
// and also returns the pprof profile of the execution.
func (c *Client) Profile(ctx context.Context, stdin []byte) (*CostProfile, error) {
	request := &api.ProfileRequest{InputStdin: stdin}
	var response api.ProfileResponse
	if err := c.post(ctx, profilePath, "profile", request, &response); err != nil {
		return nil, err
	}
	switch result := response.Result.(type) {
	case *api.ProfileResponse_Ok:
		cost, err := profileCost(result.Ok.Pprof)
		if err != nil {
			return nil, err
		}
		// The peaks are copies, so the CostEstimation holds no reference to
		// the response.
		peakHeapBytes, peakStackBytes := result.Ok.PeakHeapBytes, result.Ok.PeakStackBytes
		return &CostProfile{
			CostEstimation: CostEstimation{Cost: cost, PeakHeapBytes: &peakHeapBytes, PeakStackBytes: &peakStackBytes},
			Pprof:          result.Ok.Pprof,
		}, nil
	case *api.ProfileResponse_Err:
		return nil, &GuestError{Message: result.Err}
	default:
		return nil, errors.New("profile returned no result")
	}
}

// profileCost sums each cost.<component> sample type of a pprof profile over
// all samples. It is a port of CostProfile::cost_estimation in
// https://github.com/eth-act/ere/blob/v0.19.4/crates/prover/core/src/cost.rs#L188-L208.
func profileCost(encoded []byte) (map[string]uint64, error) {
	pprof, err := profile.ParseData(encoded)
	if err != nil {
		return nil, fmt.Errorf("decoding the pprof profile: %w", err)
	}
	cost := map[string]uint64{}
	for index, sampleType := range pprof.SampleType {
		component, ok := strings.CutPrefix(sampleType.Type, "cost.")
		if !ok {
			continue
		}
		var sum uint64
		for _, sample := range pprof.Sample {
			sum += uint64(sample.Value[index])
		}
		cost[component] = sum
	}
	// A profile without a cost component has nothing to record.
	if len(cost) == 0 {
		return nil, errors.New("profile returned no cost")
	}
	return cost, nil
}

// post sends request to the Twirp method at path and decodes the answer into
// response. method names the call in the errors.
func (c *Client) post(ctx context.Context, path, method string, request, response proto.Message) error {
	payload, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url(path), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", protobufContentType)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("%s status %d: %s", method, resp.StatusCode, readBody(resp.Body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(body, response); err != nil {
		return fmt.Errorf("decoding the %s response: %w", method, err)
	}
	return nil
}

func (c *Client) url(path string) string {
	return strings.TrimRight(c.BaseURL, "/") + path
}

func readBody(body io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(body, errorBodyLimit))
	return strings.TrimSpace(string(raw))
}
