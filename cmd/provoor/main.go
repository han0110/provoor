// Command provoor deploys zkVM proving clusters and forwards benchmark
// requests to them.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:           "provoor",
		Short:         "Deploys zkVM proving clusters and forwards benchmark requests to them",
		SilenceUsage:  true,
		SilenceErrors: false,
	}
	root.AddCommand(upCommand(), downCommand(), serveCommand())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := root.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}
