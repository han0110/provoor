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

// PrefixWriter buffers a byte stream and prints it as lines behind a fixed
// leading text.
type PrefixWriter struct {
	out     *Output
	lead    string
	pending []byte
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

// leading returns a writer that relays a streamed output line by line behind
// a fixed text.
func (o *Output) leading(lead string) *PrefixWriter {
	return &PrefixWriter{out: o, lead: lead}
}

// Prefixed returns a writer that relays streamed container output line by
// line under a prefix.
func (o *Output) Prefixed(prefix string) *PrefixWriter {
	return o.leading("[" + prefix + "] ")
}

func (p *PrefixWriter) Write(data []byte) (int, error) {
	p.pending = append(p.pending, data...)
	for {
		newline := bytes.IndexByte(p.pending, '\n')
		if newline < 0 {
			return len(data), nil
		}
		line := strings.TrimRight(string(p.pending[:newline]), "\r")
		p.pending = p.pending[newline+1:]
		p.out.Printf("%s%s", p.lead, line)
	}
}

// Flush prints a buffered partial line.
func (p *PrefixWriter) Flush() {
	if len(p.pending) > 0 {
		p.out.Printf("%s%s", p.lead, string(p.pending))
		p.pending = nil
	}
}
