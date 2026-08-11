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

// version is stamped at build time with -ldflags "-X main.version=<tag>".
var version = "dev"

func main() {
	root := &cobra.Command{
		Use:           "provoor",
		Short:         "Deploys zkVM proving clusters and forwards benchmark requests to them",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: false,
	}
	root.AddCommand(upCommand(), downCommand(), serveCommand())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// Restoring the default disposition after the first signal leaves a second
	// interrupt able to terminate a cleanup that is not returning.
	go func() {
		<-ctx.Done()
		stop()
	}()

	if err := root.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}
