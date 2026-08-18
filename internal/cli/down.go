package cli

import "github.com/spf13/cobra"

func downCommand(service Service) *cobra.Command {
	var project projectOptions
	var confirm bool
	cmd := &cobra.Command{
		Use:   "down",
		Short: "Stop the local Floci Compose environment",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !confirm {
				return usage("CONFIRMATION_REQUIRED", "pass --yes to stop the local Floci environment")
			}
			definition, dir, format, err := project.load()
			if err != nil {
				return err
			}
			if err := service.Down(cmd.Context(), definition, dir); err != nil {
				return convert(err)
			}
			return emit(cmd, "down", format, map[string]any{"stopped": true, "preserved_data": true}, nil)
		},
	}
	project.bind(cmd)
	cmd.Flags().BoolVar(&confirm, "yes", false, "confirm stopping the local environment")
	return cmd
}
