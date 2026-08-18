package cli

import "github.com/spf13/cobra"

func resetCommand(service Service) *cobra.Command {
	var project projectOptions
	var confirm bool
	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Stop Floci and remove its Compose-managed volumes",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !confirm {
				return usage("CONFIRMATION_REQUIRED", "pass --yes to reset the local Floci environment")
			}
			definition, dir, format, err := project.load()
			if err != nil {
				return err
			}
			if err := service.Reset(cmd.Context(), definition, dir); err != nil {
				return convert(err)
			}
			return emit(cmd, "reset", format, map[string]any{"stopped": true, "removed_volumes": true, "bundle_preserved": true}, nil)
		},
	}
	project.bind(cmd)
	cmd.Flags().BoolVar(&confirm, "yes", false, "confirm removing Compose-managed volumes")
	return cmd
}
