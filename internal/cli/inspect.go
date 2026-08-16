package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/nkootstra/floceed/internal/app"
	inspection "github.com/nkootstra/floceed/internal/inspect"
	"github.com/spf13/cobra"
)

func inspectCommand(service Service) *cobra.Command {
	var project projectOptions
	var compare string
	var runtime bool
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Validate and explain a generated bundle",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			definition, dir, format, err := project.load()
			if err != nil {
				return err
			}
			result, err := service.InspectWithOptions(cmd.Context(), definition, dir, app.InspectOptions{ComparePath: compare, Runtime: runtime})
			if err != nil {
				return convert(err)
			}
			if format == "json" {
				return emit(cmd, "inspect", format, result, result.Findings)
			}
			return writeInspectionText(cmd.OutOrStdout(), result)
		},
	}
	project.bind(cmd)
	cmd.Flags().StringVar(&compare, "compare", "", "compare with another generated bundle directory or project file")
	cmd.Flags().BoolVar(&runtime, "runtime", false, "include bounded Floci runtime readiness")
	return cmd
}

func writeInspectionText(w io.Writer, result inspection.Inspection) error {
	runtime := strings.ReplaceAll(string(result.Runtime.State), "_", " ")
	if _, err := fmt.Fprintf(w, "Bundle: valid\nIdentity: %s\nManifest schema: %d\nSource: %s / %s\nTarget: Floci %s\nResources: %d selected\nArtifacts: %d files, %d bytes\nRuntime: %s\n",
		result.BundleIdentity, result.ManifestSchema, result.Source.AccountID, result.Source.Region,
		result.Target.FlociVersion, result.SelectedResources, result.Artifacts.Files, result.Artifacts.Bytes, runtime); err != nil {
		return err
	}
	if len(result.Runtime.FailedScripts) > 0 {
		if _, err := fmt.Fprintf(w, "Failed scripts: %s\n", strings.Join(result.Runtime.FailedScripts, ", ")); err != nil {
			return err
		}
	}
	if result.Runtime.Diagnostic != "" {
		if _, err := fmt.Fprintf(w, "Runtime diagnostic: %s\n", result.Runtime.Diagnostic); err != nil {
			return err
		}
	}
	if len(result.Services) > 0 {
		if _, err := fmt.Fprintln(w, "\nServices"); err != nil {
			return err
		}
		for _, service := range result.Services {
			if _, err := fmt.Fprintf(w, "%s: %d resources, %d selected, %d records, %d bytes\n", service.Service, service.Resources, service.Selected, service.Records, service.SourceBytes); err != nil {
				return err
			}
		}
	}
	if len(result.Resources) > 0 {
		if _, err := fmt.Fprintln(w, "\nResources"); err != nil {
			return err
		}
		for _, resource := range result.Resources {
			state := "not selected"
			if resource.Selected {
				state = "selected"
			}
			if _, err := fmt.Fprintf(w, "%s/%s/%s: %s\n", resource.Identity.Service, resource.Identity.Type, resource.Identity.ID, state); err != nil {
				return err
			}
		}
	}
	if result.Receipt != nil {
		r := result.Receipt
		if _, err := fmt.Fprintf(w, "\nComparison\nBaseline: %s\nCurrent: %s\nChanges: %d added, %d removed, %d changed, %d unchanged\n", r.Baseline, r.Current, r.Counts.Added, r.Counts.Removed, r.Counts.Changed, r.Counts.Unchanged); err != nil {
			return err
		}
		for _, change := range r.Resources {
			detail := ""
			if len(change.Categories) > 0 {
				parts := make([]string, len(change.Categories))
				for i, category := range change.Categories {
					parts[i] = string(category)
				}
				detail = " (" + strings.Join(parts, ", ") + ")"
			}
			if _, err := fmt.Fprintf(w, "%s/%s/%s: %s%s\n", change.Resource.Service, change.Resource.Type, change.Resource.ID, change.Outcome, detail); err != nil {
				return err
			}
		}
	}
	return nil
}
