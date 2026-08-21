package cluster

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/docker/cli/cli/connhelper"
	"github.com/docker/cli/cli/connhelper/commandconn"
	"github.com/docker/cli/cli/connhelper/ssh"
	"github.com/docker/docker/client"
)

// tunnelConnectTimeout bounds the ssh connect of one tunnelled dial, so an
// unreachable bastion fails in seconds rather than on the TCP default.
const tunnelConnectTimeout = "10"

// daemonURL turns a config SSH destination into a Docker daemon URL. A bare
// destination becomes an ssh URL so the local ssh binary resolves it, and an
// explicit URL of any scheme passes through unchanged.
func daemonURL(destination string) string {
	if destination == "" || strings.Contains(destination, "://") {
		return destination
	}
	return "ssh://" + destination
}

// HostName derives the default node name from an SSH destination by dropping
// the scheme, the user, and the port.
func HostName(destination string) string {
	if destination == "" {
		return "local"
	}
	host := strings.TrimPrefix(destination, "ssh://")
	if at := strings.LastIndex(host, "@"); at >= 0 {
		host = host[at+1:]
	}
	if colon := strings.LastIndex(host, ":"); colon >= 0 {
		host = host[:colon]
	}
	return host
}

// Dial connects to the Docker daemon behind an SSH destination. An ssh URL is
// served by spawning the local ssh binary against the remote docker CLI, so
// the user's SSH configuration, agent, and proxies all apply and no key
// material is ever read. An empty destination uses the local environment's
// daemon.
func Dial(destination string) (*client.Client, error) {
	url := daemonURL(destination)
	options := []client.Opt{client.WithAPIVersionNegotiation()}
	switch {
	case url == "":
		options = append(options, client.FromEnv)
	case strings.HasPrefix(url, "ssh://"):
		helper, err := connhelper.GetConnectionHelper(url)
		if err != nil {
			return nil, fmt.Errorf("ssh destination %q: %w", destination, err)
		}
		options = append(options, client.WithHost(helper.Host), client.WithDialContext(helper.Dialer))
	default:
		options = append(options, client.WithHost(url))
	}

	cli, err := client.NewClientWithOpts(options...)
	if err != nil {
		return nil, fmt.Errorf("creating docker client for %q: %w", destination, err)
	}
	return cli, nil
}

// TunnelDialer reaches remoteAddr as the host behind an SSH destination sees
// it, by spawning the local ssh binary in stdio-forward mode. It is how a
// client reaches a service port that only the cluster's own network routes,
// which is the ordinary case for a deployment behind a bastion whose SSH proxy
// carries no cluster traffic. The local ssh binary does the work for the same
// reason [Dial] uses it, so the user's SSH configuration, agent, and proxies
// all apply and no key material is ever read.
//
// The returned dialer ignores the address it is called with, since the
// destination and remoteAddr already name both ends. An empty destination
// yields a nil dialer, leaving the caller to connect directly.
func TunnelDialer(destination, remoteAddr string) (func(context.Context, string) (net.Conn, error), error) {
	if destination == "" {
		return nil, nil
	}
	args, err := tunnelArgs(destination, remoteAddr)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, _ string) (net.Conn, error) {
		conn, err := commandconn.New(ctx, "ssh", args...)
		if err != nil {
			return nil, fmt.Errorf("tunnelling to %s through %q: %w", remoteAddr, destination, err)
		}
		return conn, nil
	}, nil
}

// tunnelArgs builds the ssh invocation of one stdio forward. -W implies -N and
// -T, so ssh forwards to remoteAddr and runs no remote command, which is why
// none of the remote-shell quoting the Docker connection helper needs applies
// here. A destination carrying no user leaves the account to the user's own
// SSH configuration.
func tunnelArgs(destination, remoteAddr string) ([]string, error) {
	spec, err := ssh.ParseURL(daemonURL(destination))
	if err != nil {
		return nil, fmt.Errorf("ssh destination %q: %w", destination, err)
	}
	var args []string
	if spec.User != "" {
		args = append(args, "-l", spec.User)
	}
	if spec.Port != "" {
		args = append(args, "-p", spec.Port)
	}
	return append(args, "-o", "ConnectTimeout="+tunnelConnectTimeout, "-W", remoteAddr, "--", spec.Host), nil
}
