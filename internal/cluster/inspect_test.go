package cluster

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// summaries answers a container listing with the names a daemon reports.
func summaries(names ...string) []container.Summary {
	listing := make([]container.Summary, len(names))
	for i, name := range names {
		listing[i] = container.Summary{ID: name, Names: []string{"/" + name}}
	}
	return listing
}

// fakeHosts dials every destination at one fake daemon, in the order given.
func fakeHosts(cli *client.Client, destinations ...string) *Hosts {
	hosts := &Hosts{clients: map[string]*client.Client{}, order: destinations}
	for _, destination := range destinations {
		hosts.clients[destination] = cli
	}
	return hosts
}

func TestListSelectsExactNames(t *testing.T) {
	var query url.Values
	cli := newFakeDaemon(t, map[string]http.HandlerFunc{
		"GET /containers/json": func(w http.ResponseWriter, r *http.Request) {
			query = r.URL.Query()
			// The daemon matches a name filter as a substring, so the probe of
			// an earlier deployment answers the worker filter too.
			writeJSON(w, http.StatusOK, summaries("zisk-worker-probe", "zisk-worker", "zisk-coordinator"))
		},
	})
	selection := []Deployed{
		{Name: "zisk-coordinator", Label: CoordinatorName},
		{Name: "zisk-worker", Label: "worker_0-gpu_0"},
	}

	listed, err := List(t.Context(), fakeHosts(cli, ""), selection, false)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(listed))
	for i, entry := range listed {
		names[i] = entry.Node() + "/" + entry.Name
	}
	if want := []string{"local/zisk-coordinator", "local/zisk-worker"}; !slices.Equal(names, want) {
		t.Errorf("listed = %v, want %v", names, want)
	}
	if got := query.Get("all"); got == "1" || got == "true" {
		t.Errorf("all = %q, want the running containers only", got)
	}
	if filter := query.Get("filters"); !strings.Contains(filter, "zisk-worker") || !strings.Contains(filter, "zisk-coordinator") {
		t.Errorf("filters = %q, want both selected names", filter)
	}
}

func TestListSelectsByLabel(t *testing.T) {
	var query url.Values
	cli := newFakeDaemon(t, map[string]http.HandlerFunc{
		"GET /containers/json": func(w http.ResponseWriter, r *http.Request) {
			query = r.URL.Query()
			writeJSON(w, http.StatusOK, summaries("zisk-worker", "provoor-node-local"))
		},
	})

	listed, err := List(t.Context(), fakeHosts(cli, ""), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(listed))
	for i, entry := range listed {
		names[i] = entry.Name
	}
	if want := []string{"provoor-node-local", "zisk-worker"}; !slices.Equal(names, want) {
		t.Errorf("listed = %v, want %v", names, want)
	}
	if filter := query.Get("filters"); !strings.Contains(filter, Label) {
		t.Errorf("filters = %q, want the %s label", filter, Label)
	}
	if got := query.Get("all"); got != "1" && got != "true" {
		t.Errorf("all = %q, want every container", got)
	}
}

// TestListFollowsDialOrder covers a worker host listed ahead of the
// coordinator host, an order a sort by node name reverses.
func TestListFollowsDialOrder(t *testing.T) {
	cli := newFakeDaemon(t, map[string]http.HandlerFunc{
		"GET /containers/json": func(w http.ResponseWriter, r *http.Request) {
			for _, name := range []string{"zisk-coordinator", "zisk-worker"} {
				if strings.Contains(r.URL.Query().Get("filters"), name) {
					writeJSON(w, http.StatusOK, summaries(name))
					return
				}
			}
		},
	})
	selection := []Deployed{
		{SSH: "user@10.0.0.2", Name: "zisk-coordinator", Label: CoordinatorName},
		{SSH: "user@10.0.0.9", Name: "zisk-worker", Label: "worker_0-gpu_0"},
	}

	listed, err := List(t.Context(), fakeHosts(cli, "user@10.0.0.9", "user@10.0.0.2"), selection, false)
	if err != nil {
		t.Fatal(err)
	}
	nodes := make([]string, len(listed))
	for i, entry := range listed {
		nodes[i] = entry.Node()
	}
	if want := []string{"10.0.0.9", "10.0.0.2"}; !slices.Equal(nodes, want) {
		t.Errorf("nodes = %v, want %v", nodes, want)
	}
}

func TestWriteTable(t *testing.T) {
	created := time.Now().Add(-3 * time.Hour).Unix()
	listed := []Listed{
		{SSH: "", Name: "zisk-coordinator", Summary: container.Summary{
			Image:   "ghcr.io/han0110/provoor/zisk:1.2.0-alpha",
			Command: "zisk-supervisor zisk-coordinator --api-port 7000",
			Created: created,
			Status:  "Up 3 hours",
			Ports: []container.Port{
				{PrivatePort: 50051, Type: "tcp"},
				{IP: "0.0.0.0", PrivatePort: 7000, PublicPort: 7000, Type: "tcp"},
			},
		}},
		{SSH: "ssh://user@10.0.0.2:2222", Name: "zisk-worker", Summary: container.Summary{
			Image:   "ghcr.io/han0110/provoor/zisk:1.2.0-alpha",
			Command: "mpirun",
			Created: created,
			Status:  "Exited (0) 2 minutes ago",
		}},
	}

	var buf bytes.Buffer
	if err := WriteTable(&buf, listed); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"NODE|NAME|IMAGE|COMMAND|CREATED|STATUS|PORTS",
		"local|zisk-coordinator|ghcr.io/han0110/provoor/zisk:1.2.0-alpha|" +
			"\"zisk-supervisor zis\u2026\"|3 hours ago|Up 3 hours|0.0.0.0:7000->7000/tcp, 50051/tcp",
		`10.0.0.2|zisk-worker|ghcr.io/han0110/provoor/zisk:1.2.0-alpha|"mpirun"|3 hours ago|Exited (0) 2 minutes ago`,
	}
	if got := columns(buf.String()); !slices.Equal(got, want) {
		t.Errorf("table =\n%v\nwant\n%v", got, want)
	}
}

// columns renders a table as one row per line with the cells joined by a bar,
// so a comparison ignores the padding.
func columns(table string) []string {
	rows := []string{}
	for _, line := range strings.Split(strings.TrimRight(table, "\n"), "\n") {
		cells := []string{}
		for _, cell := range strings.Split(strings.TrimRight(line, " "), "  ") {
			if trimmed := strings.TrimSpace(cell); trimmed != "" {
				cells = append(cells, trimmed)
			}
		}
		rows = append(rows, strings.Join(cells, "|"))
	}
	return rows
}

func TestStreamLogs(t *testing.T) {
	var mutex sync.Mutex
	queries := map[string]url.Values{}
	cli := newFakeDaemon(t, map[string]http.HandlerFunc{
		"GET /containers/{id}/logs": func(w http.ResponseWriter, r *http.Request) {
			mutex.Lock()
			queries[r.PathValue("id")] = r.URL.Query()
			mutex.Unlock()
			_, _ = w.Write(stdoutFrame("first\nsecond\n"))
		},
	})
	listed := []Listed{
		{SSH: "", Name: "zisk-coordinator"},
		{SSH: "user@10.0.0.2", Name: "zisk-worker"},
	}

	var buf bytes.Buffer
	if err := StreamLogs(t.Context(), fakeHosts(cli, "", "user@10.0.0.2"), listed, false, true, &buf); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	slices.Sort(lines)
	want := []string{
		"10.0.0.2/zisk-worker   | first",
		"10.0.0.2/zisk-worker   | second",
		"local/zisk-coordinator | first",
		"local/zisk-coordinator | second",
	}
	if !slices.Equal(lines, want) {
		t.Errorf("lines =\n%v\nwant\n%v", lines, want)
	}
	if strings.Contains(buf.String(), "\x1b") {
		t.Error("the prefixes carry color while the output is not a terminal")
	}
	if len(queries) != 2 {
		t.Fatalf("read the log of %d containers, want 2", len(queries))
	}
	for name, query := range queries {
		if query.Get("timestamps") != "1" {
			t.Errorf("%s timestamps = %q, want 1", name, query.Get("timestamps"))
		}
		if query.Get("follow") == "1" {
			t.Errorf("%s follows while follow is not asked for", name)
		}
	}
}

// signalWriter reports the first write, so a followed stream can be stopped
// once it produced output.
type signalWriter struct {
	mutex sync.Mutex
	buf   bytes.Buffer
	first chan struct{}
}

func (w *signalWriter) Write(data []byte) (int, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	select {
	case w.first <- struct{}{}:
	default:
	}
	return w.buf.Write(data)
}

func (w *signalWriter) String() string {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.buf.String()
}

// TestStreamLogsEndsOnCancel covers the interrupt that stops a followed
// stream, which the command answers with no error.
func TestStreamLogsEndsOnCancel(t *testing.T) {
	cli := newFakeDaemon(t, map[string]http.HandlerFunc{
		"GET /containers/{id}/logs": streamContainerLogs,
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	writer := &signalWriter{first: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() {
		done <- StreamLogs(ctx, fakeHosts(cli, ""), []Listed{{Name: "zisk-worker"}}, true, false, writer)
	}()

	<-writer.first
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if !strings.HasPrefix(writer.String(), "local/zisk-worker | line 0") {
		t.Errorf("output = %q, want the prefixed stream", writer.String())
	}
}
