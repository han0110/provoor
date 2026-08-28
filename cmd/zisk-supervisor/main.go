// Command zisk-supervisor runs the ZisK coordinator and ends it when a client
// asks, which is how the record of the guests already set up is cleared. It
// ships inside the cluster image and takes the coordinator's own command as its
// arguments.
package main

import (
	"fmt"
	"net"
	"os"

	"github.com/han0110/provoor/pkg/cluster/zisk/supervisor"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: zisk-supervisor <coordinator command>")
		os.Exit(2)
	}
	port := os.Getenv("RESTART_PORT")
	if port == "" {
		fmt.Fprintln(os.Stderr, "RESTART_PORT must name the port restart requests arrive on")
		os.Exit(2)
	}

	listener, err := net.Listen("tcp", net.JoinHostPort("", port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "listening for restart requests: %v\n", err)
		os.Exit(1)
	}
	status, err := supervisor.Run(os.Args[1:], listener)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(status)
}
