package main

import (
	"github.com/spf13/cobra"
)

func upCommand() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "up",
		Short: "Deploys the proving cluster and blocks until it is ready",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := loadCluster(configPath)
			if err != nil {
				return err
			}
			return c.Up(cmd.Context(), cmd.OutOrStdout())
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "cluster configuration file")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}
