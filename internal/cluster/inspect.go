package cluster

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-units"
	"golang.org/x/sync/errgroup"
)

// Label marks every container up deploys, so a listing finds them without a
// configuration.
const Label = "provoor"

// commandWidth is the width docker ps truncates a command to.
const commandWidth = 20

// logColors cycles over the log prefixes, in the palette docker compose
// prints them in.
var logColors = []int{36, 33, 32, 35, 34, 96, 93, 92, 95, 94}

// Deployed is one container a configuration deploys, on the host it names.
// Label is the progress-line name of a coordinator or worker, and the kind of
// a sidecar.
type Deployed struct {
	SSH     string
	Name    string
	Label   string
	Sidecar bool
}

// Listed is one container a daemon reports, with the destination its daemon
// was dialed at.
type Listed struct {
	SSH     string
	Name    string
	Summary container.Summary
}

// Node names the machine the container runs on.
func (l Listed) Node() string {
	return HostName(l.SSH)
}

// List lists the containers of a selection, host by host in dial order and by
// name within a host. A nil selection covers every container the provoor
// label marks, and any other selection covers its entries by name on the host
// each one names. all covers the containers that are not running.
func List(ctx context.Context, hosts *Hosts, selection []Deployed, all bool) ([]Listed, error) {
	listed := []Listed{}
	for _, destination := range hosts.Destinations() {
		names := selectedNames(selection, destination)
		if selection != nil && len(names) == 0 {
			continue
		}
		summaries, err := hosts.Client(destination).ContainerList(ctx, listOptions(names, all))
		if err != nil {
			return nil, fmt.Errorf("listing containers on %s: %w", HostName(destination), err)
		}
		host := []Listed{}
		for _, summary := range summaries {
			if name := listedName(summary, names); name != "" {
				host = append(host, Listed{SSH: destination, Name: name, Summary: summary})
			}
		}
		slices.SortFunc(host, func(a, b Listed) int { return strings.Compare(a.Name, b.Name) })
		listed = append(listed, host...)
	}
	return listed, nil
}

// WriteTable prints a listing as the table docker ps prints, with the node of
// every container ahead of its name.
func WriteTable(w io.Writer, listed []Listed) error {
	table := tabwriter.NewWriter(w, 10, 1, 3, ' ', 0)
	fmt.Fprintln(table, "NODE\tNAME\tIMAGE\tCOMMAND\tCREATED\tSTATUS\tPORTS")
	for _, entry := range listed {
		created := units.HumanDuration(time.Now().UTC().Sub(time.Unix(entry.Summary.Created, 0))) + " ago"
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			entry.Node(), entry.Name, entry.Summary.Image, strconv.Quote(ellipsis(entry.Summary.Command, commandWidth)),
			created, entry.Summary.Status, displayablePorts(entry.Summary.Ports))
	}
	return table.Flush()
}

// StreamLogs prints the log of every listed container, each line behind the
// node and the name of its container. follow keeps every stream open until
// ctx ends, which is not a failure.
func StreamLogs(ctx context.Context, hosts *Hosts, listed []Listed, follow, timestamps bool, w io.Writer) error {
	prefixes := make([]string, len(listed))
	width := 0
	for i, entry := range listed {
		prefixes[i] = entry.Node() + "/" + entry.Name
		width = max(width, len(prefixes[i]))
	}
	colored := terminal(w)
	out := NewOutput(w)
	g, gctx := errgroup.WithContext(ctx)
	for i, entry := range listed {
		g.Go(func() error {
			writer := out.leading(logLead(prefixes[i], width, i, colored))
			return streamLog(gctx, hosts.Client(entry.SSH), entry, writer, follow, timestamps)
		})
	}
	if err := g.Wait(); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

// selectedNames lists the container names a selection places on one host.
func selectedNames(selection []Deployed, destination string) []string {
	if selection == nil {
		return nil
	}
	names := []string{}
	for _, deployed := range selection {
		if deployed.SSH == destination {
			names = append(names, deployed.Name)
		}
	}
	return names
}

// listOptions filters a listing by name, or by the provoor label when the
// selection names none.
func listOptions(names []string, all bool) container.ListOptions {
	args := filters.NewArgs(filters.Arg("label", Label))
	if len(names) > 0 {
		args = filters.NewArgs()
		for _, name := range names {
			args.Add("name", name)
		}
	}
	return container.ListOptions{All: all, Filters: args}
}

// listedName is the name a summary belongs to the selection under, empty when
// none does. Docker matches a name filter as a substring, so a selection
// compares the full names.
func listedName(summary container.Summary, names []string) string {
	if names == nil {
		return strings.TrimPrefix(summary.Names[0], "/")
	}
	for _, name := range names {
		if slices.Contains(summary.Names, "/"+name) {
			return name
		}
	}
	return ""
}

// ellipsis truncates a value to a display width, as docker ps truncates the
// command.
func ellipsis(text string, width int) string {
	runes := []rune(text)
	if len(runes) <= width {
		return text
	}
	return string(runes[:width-1]) + "\u2026"
}

// displayablePorts renders the ports of a container as docker ps prints them,
// in port order rather than grouped into ranges, since no cluster container
// exposes a range. A container on the host network reports none.
func displayablePorts(ports []container.Port) string {
	sorted := slices.SortedFunc(slices.Values(ports), comparePorts)
	rendered := make([]string, len(sorted))
	for i, port := range sorted {
		rendered[i] = fmt.Sprintf("%d/%s", port.PrivatePort, port.Type)
		if port.IP != "" {
			rendered[i] = net.JoinHostPort(port.IP, strconv.Itoa(int(port.PublicPort))) + "->" + rendered[i]
		}
	}
	return strings.Join(rendered, ", ")
}

func comparePorts(a, b container.Port) int {
	return cmp.Or(
		cmp.Compare(a.PrivatePort, b.PrivatePort),
		cmp.Compare(a.IP, b.IP),
		cmp.Compare(a.PublicPort, b.PublicPort),
		cmp.Compare(a.Type, b.Type),
	)
}

// terminal reports whether a writer is a terminal, which decides whether the
// log prefixes carry color.
func terminal(w io.Writer) bool {
	file, ok := w.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

// logLead is the text every line of one container carries, padded to the
// widest prefix of the selection.
func logLead(prefix string, width, index int, colored bool) string {
	padded := fmt.Sprintf("%-*s", width, prefix)
	if colored {
		padded = fmt.Sprintf("\x1b[%dm%s\x1b[0m", logColors[index%len(logColors)], padded)
	}
	return padded + " | "
}

// streamLog relays one container's output and error streams into a writer.
func streamLog(ctx context.Context, cli *client.Client, entry Listed, output *PrefixWriter, follow, timestamps bool) error {
	logs, err := cli.ContainerLogs(ctx, entry.Name, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     follow,
		Timestamps: timestamps,
	})
	if err != nil {
		return fmt.Errorf("reading the log of %s on %s: %w", entry.Name, entry.Node(), err)
	}
	defer func() { _ = logs.Close() }()
	_, err = stdcopy.StdCopy(output, output, logs)
	output.Flush()
	if err != nil {
		return fmt.Errorf("reading the log of %s on %s: %w", entry.Name, entry.Node(), err)
	}
	return nil
}
