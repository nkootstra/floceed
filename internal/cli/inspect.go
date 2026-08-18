package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode"

	"github.com/nkootstra/floceed/internal/app"
	inspection "github.com/nkootstra/floceed/internal/inspect"
	"github.com/spf13/cobra"
)

func inspectCommand(service Service) *cobra.Command {
	var project projectOptions
	var compare string
	var runtime bool
	var artifacts bool
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Validate and explain a generated bundle",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			definition, dir, format, err := project.load()
			if err != nil {
				return err
			}
			result, err := service.InspectWithOptions(cmd.Context(), definition, dir, app.InspectOptions{ComparePath: compare, Runtime: runtime, Artifacts: artifacts})
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
	cmd.Flags().BoolVar(&artifacts, "artifacts", false, "include the verified artifact inventory")
	return cmd
}

func writeInspectionText(w io.Writer, result inspection.Inspection) error {
	runtime := terminalSafe(strings.ReplaceAll(string(result.Runtime.State), "_", " "))
	if _, err := fmt.Fprintf(w, "Bundle: valid\nIdentity: %s\nManifest schema: %d\nSource: %s / %s\nTarget: Floci %s\nResources: %d selected\nArtifacts: %d files, %d bytes\nRuntime: %s\n",
		terminalSafe(result.BundleIdentity), result.ManifestSchema, terminalSafe(result.Source.AccountID), terminalSafe(result.Source.Region),
		terminalSafe(result.Target.FlociVersion), result.SelectedResources, result.Artifacts.Files, result.Artifacts.Bytes, runtime); err != nil {
		return err
	}
	if len(result.Runtime.FailedScripts) > 0 {
		if _, err := fmt.Fprintf(w, "Failed scripts: %s\n", joinSafe(result.Runtime.FailedScripts)); err != nil {
			return err
		}
	}
	if result.Runtime.Diagnostic != "" {
		if _, err := fmt.Fprintf(w, "Runtime diagnostic: %s\n", terminalSafe(result.Runtime.Diagnostic)); err != nil {
			return err
		}
	}
	if err := writeFindings(w, "\nFindings\n", "", result.Findings); err != nil {
		return err
	}
	if len(result.Services) > 0 {
		if _, err := fmt.Fprintln(w, "\nServices"); err != nil {
			return err
		}
		for _, service := range result.Services {
			if _, err := fmt.Fprintf(w, "%s: %d resources, %d selected, %d records, %d bytes\n", terminalSafe(service.Service), service.Resources, service.Selected, service.Records, service.SourceBytes); err != nil {
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
			if _, err := fmt.Fprintf(w, "%s/%s/%s: %s\n", terminalSafe(resource.Identity.Service), terminalSafe(resource.Identity.Type), terminalSafe(resource.Identity.ID), state); err != nil {
				return err
			}
			if err := writeFindings(w, "  Findings\n", "  ", resource.Findings); err != nil {
				return err
			}
		}
	}
	if len(result.Artifacts.Entries) > 0 {
		if _, err := fmt.Fprintln(w, "\nArtifacts"); err != nil {
			return err
		}
		for _, artifact := range result.Artifacts.Entries {
			if _, err := fmt.Fprintf(w, "%s: %d bytes sha256:%s\n", terminalSafe(artifact.Path), artifact.Size, terminalSafe(artifact.SHA256)); err != nil {
				return err
			}
		}
	}
	if result.Receipt != nil {
		r := result.Receipt
		if _, err := fmt.Fprintf(w, "\nComparison\nBaseline: %s\nCurrent: %s\n", terminalSafe(r.Baseline), terminalSafe(r.Current)); err != nil {
			return err
		}
		if len(r.Categories) > 0 {
			categories := make([]string, len(r.Categories))
			for i, category := range r.Categories {
				categories[i] = string(category)
			}
			if _, err := fmt.Fprintf(w, "Categories: %s\n", joinSafe(categories)); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "Changes: %d added, %d removed, %d changed, %d unchanged\n", r.Counts.Added, r.Counts.Removed, r.Counts.Changed, r.Counts.Unchanged); err != nil {
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
			if _, err := fmt.Fprintf(w, "%s/%s/%s: %s%s\n", terminalSafe(change.Resource.Service), terminalSafe(change.Resource.Type), terminalSafe(change.Resource.ID), terminalSafe(string(change.Outcome)), terminalSafe(detail)); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeFindings(w io.Writer, heading, indent string, findings []inspection.Finding) error {
	if len(findings) == 0 {
		return nil
	}
	ordered := append([]inspection.Finding(nil), findings...)
	sort.SliceStable(ordered, func(i, j int) bool {
		left := ordered[i]
		right := ordered[j]
		return left.Code+"\x00"+left.Severity+"\x00"+left.Support+"\x00"+left.Resource+"\x00"+left.Property < right.Code+"\x00"+right.Severity+"\x00"+right.Support+"\x00"+right.Resource+"\x00"+right.Property
	})
	if _, err := fmt.Fprint(w, heading); err != nil {
		return err
	}
	for _, finding := range ordered {
		fields := make([]string, 0, 2)
		if finding.Resource != "" {
			fields = append(fields, "resource="+terminalSafe(finding.Resource))
		}
		if finding.Property != "" {
			fields = append(fields, "property="+terminalSafe(finding.Property))
		}
		suffix := ""
		if len(fields) > 0 {
			suffix = " [" + strings.Join(fields, ", ") + "]"
		}
		if _, err := fmt.Fprintf(w, "%s%s %s: %s%s\n", indent, terminalSafe(strings.ToUpper(finding.Severity)), terminalSafe(finding.Code), terminalSafe(finding.Support), suffix); err != nil {
			return err
		}
	}
	return nil
}

func joinSafe(values []string) string {
	safe := make([]string, len(values))
	for i, value := range values {
		safe[i] = terminalSafe(value)
	}
	return strings.Join(safe, ", ")
}

// terminalSafe preserves readable text while making control and Unicode format
// characters visible, preventing bundle or runtime data from forging or
// visually reordering terminal output.
func terminalSafe(value string) string {
	var result strings.Builder
	for _, char := range value {
		if unicode.IsControl(char) || unicode.Is(unicode.Cf, char) {
			if char <= 0xff {
				fmt.Fprintf(&result, `\x%02X`, char)
			} else {
				fmt.Fprintf(&result, `\u%04X`, char)
			}
			continue
		}
		result.WriteRune(char)
	}
	return result.String()
}
