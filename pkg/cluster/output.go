package cluster

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
)

// Output serialises progress lines from concurrent host operations.
type Output struct {
	mu sync.Mutex
	w  io.Writer
}

// NewOutput wraps a writer for concurrent line-oriented progress.
func NewOutput(w io.Writer) *Output {
	return &Output{w: w}
}

// Printf writes one progress line.
func (o *Output) Printf(format string, args ...any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fmt.Fprintf(o.w, format+"\n", args...)
}

// Prefixed returns a writer relaying streamed container output line by line
// under a node prefix. Flush emits any trailing partial line.
func (o *Output) Prefixed(prefix string) *PrefixWriter {
	return &PrefixWriter{out: o, prefix: prefix}
}

// PrefixWriter buffers a byte stream and prints it as prefixed lines.
type PrefixWriter struct {
	out     *Output
	prefix  string
	pending []byte
}

func (p *PrefixWriter) Write(data []byte) (int, error) {
	p.pending = append(p.pending, data...)
	for {
		newline := bytes.IndexByte(p.pending, '\n')
		if newline < 0 {
			break
		}
		line := strings.TrimRight(string(p.pending[:newline]), "\r")
		p.pending = p.pending[newline+1:]
		p.out.Printf("[%s] %s", p.prefix, line)
	}
	return len(data), nil
}

// Flush prints any buffered partial line.
func (p *PrefixWriter) Flush() {
	if len(p.pending) > 0 {
		p.out.Printf("[%s] %s", p.prefix, string(p.pending))
		p.pending = nil
	}
}

// ReportPhase reports a phase through onPhase when non-nil, skipping the
// phase already reported.
func ReportPhase(onPhase func(phase string), lastPhase *string, phase string) {
	if onPhase != nil && phase != *lastPhase {
		*lastPhase = phase
		onPhase(phase)
	}
}
