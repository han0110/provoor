package cluster

import (
	"fmt"
	"strings"

	"github.com/docker/cli/cli/connhelper"
	"github.com/docker/docker/client"
)

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
