package tui

import (
	"fmt"
	"strings"
	"unicode"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func (m Model) View() tea.View {
	var b strings.Builder
	b.WriteString(m.breadcrumb())
	b.WriteString("\n\n")
	if m.err != nil {
		fmt.Fprintf(&b, "Error: %v\n\n", m.err)
	}
	switch m.screen {
	case ScreenLoading:
		b.WriteString("Loading AWS profiles...")
	case ScreenProfiles:
		b.WriteString("Choose an AWS profile\n")
		for i, p := range m.profiles {
			b.WriteString(m.row(i, p.Name))
		}
		if len(m.profiles) == 0 {
			b.WriteString("No shared profiles found. Use AWS_CONFIG_FILE or configure ~/.aws/config.\n")
		}
	case ScreenRegion:
		b.WriteString("Choose the AWS region\n")
		b.WriteString(m.regionInput.View())
	case ScreenIdentity:
		if m.busy {
			b.WriteString("Confirming caller identity...")
		} else {
			fmt.Fprintf(&b, "Account: %s\nARN: %s\n\nPress Enter to confirm.", m.identity.AccountID, m.identity.ARN)
		}
	case ScreenServices:
		if m.busy {
			fmt.Fprintf(&b, "%s Discovering selected services...\n", m.spinner.View())
			b.WriteString("Press Esc to cancel discovery.\n")
		} else {
			b.WriteString("Choose services\n")
			for i, s := range m.services {
				b.WriteString(m.checkRow(i, m.serviceSelected[s.Name], s.DisplayName+" "+badge(s.Support, m.opts.NoColor)))
			}
		}
	case ScreenResources:
		if m.filtering {
			b.WriteString(m.filter.View() + "\n")
		} else if m.filter.Value() != "" {
			fmt.Fprintf(&b, "Filter: %s\n", m.filter.Value())
		}
		for i, r := range m.visibleResources() {
			b.WriteString(m.checkRow(i, m.selected[resourceKey(r.Ref)], r.Name+"  "+r.Ref.Service))
		}
	case ScreenOptions:
		b.WriteString("Import options (structure only by default)\n")
		for i, r := range m.selectedResources() {
			label := r.Name + "  structure only"
			if m.dataEnabled[resourceKey(r.Ref)] {
				if m.dataMode[resourceKey(r.Ref)] == config.DataModeFull {
					label = r.Name + "  full data (resumable; 1h replay timeout)"
				} else if r.Ref.Service == "s3" {
					label += fmt.Sprintf(" + data (%d objects, %s/object, %s total)", config.DefaultS3MaxObjects, bytesLabel(config.DefaultS3MaxObjectBytes), bytesLabel(config.DefaultS3MaxTotalBytes))
				} else {
					label += fmt.Sprintf(" + data (%d items, %d pages, gzip)", config.DefaultDynamoDBMaxItems, config.DefaultDynamoDBMaxPages)
				}
			}
			b.WriteString(m.checkRow(i, m.dataEnabled[resourceKey(r.Ref)], label))
		}
	case ScreenReview:
		fmt.Fprintf(&b, "Dependency and compatibility review\n%d dependencies | %d findings\n", len(m.plan.Dependencies), len(m.findings))
		for _, f := range m.findings {
			fmt.Fprintf(&b, "- %s %s: %s\n", badge(f.Support, m.opts.NoColor), f.Code, f.Message)
		}
	case ScreenSummary:
		fmt.Fprintf(&b, "Ready to generate\n%d resources | %d actions | estimated %s\n", len(m.selected), len(m.plan.Operations), bytesLabel(m.plan.EstimatedBytes))
	case ScreenConfirm:
		b.WriteString("Write floceed.yaml and generate the bundle? [y/N]")
	case ScreenProgress:
		if m.progress.Phase == "" {
			b.WriteString("Saving project and generating bundle...")
		} else {
			approx := ""
			if m.progress.TotalPrecision != "" && m.progress.TotalPrecision != "exact" {
				approx = "~"
			}
			fmt.Fprintf(&b, "%s %s %s", titleCase(m.progress.Phase), m.progress.Service, m.progress.Resource)
			if m.progress.TotalRecords > 0 {
				remaining := m.progress.RemainingRecords
				if remaining == 0 {
					remaining = max(0, m.progress.TotalRecords-m.progress.CompletedRecords)
				}
				fmt.Fprintf(&b, "\n%d / %s%d records (%d remaining)", m.progress.CompletedRecords, approx, m.progress.TotalRecords, remaining)
			} else if m.progress.CompletedRecords > 0 {
				fmt.Fprintf(&b, "\n%d records processed; total not known yet", m.progress.CompletedRecords)
			}
			if m.progress.TotalBytes > 0 {
				remaining := m.progress.RemainingBytes
				if remaining == 0 {
					remaining = max(0, m.progress.TotalBytes-m.progress.CompletedBytes)
				}
				fmt.Fprintf(&b, "\n%s / %s%s (%s remaining)", bytesLabel(m.progress.CompletedBytes), approx, bytesLabel(m.progress.TotalBytes), bytesLabel(remaining))
			}
			if m.progress.Resumed {
				b.WriteString("\nResumed from a verified checkpoint.")
			}
		}
	case ScreenResult:
		if m.err != nil {
			b.WriteString("Generation failed. The previous valid bundle was preserved.")
		} else {
			fmt.Fprintf(&b, "Bundle generated successfully.\n%d resources captured.", len(m.manifest.Selected))
		}
	}
	b.WriteString("\n\n")
	b.WriteString(m.help())
	view := tea.NewView(b.String())
	view.AltScreen = true
	return view
}

func (m Model) breadcrumb() string {
	steps := []struct {
		s    Screen
		name string
	}{{ScreenProfiles, "Profile"}, {ScreenRegion, "Region"}, {ScreenIdentity, "Identity"}, {ScreenServices, "Services"}, {ScreenResources, "Resources"}, {ScreenOptions, "Options"}, {ScreenReview, "Review"}, {ScreenSummary, "Summary"}, {ScreenProgress, "Generate"}, {ScreenResult, "Result"}}
	parts := make([]string, len(steps))
	for i, x := range steps {
		parts[i] = x.name
		if x.s == m.screen && !m.opts.NoColor {
			parts[i] = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6")).Render(x.name)
		}
	}
	return strings.Join(parts, " > ")
}

func (m Model) row(i int, text string) string {
	marker := "  "
	if i == m.cursor {
		marker = "> "
	}
	return marker + text + "\n"
}

func (m Model) checkRow(i int, checked bool, text string) string {
	box := "[ ]"
	if checked {
		box = "[x]"
	}
	return m.row(i, box+" "+text)
}

func (m Model) help() string {
	if m.screen == ScreenConfirm {
		return "y confirm | n/Esc back | Ctrl-C quit"
	}
	return "j/k or arrows move | Space select | / filter | Enter continue | Esc back | Ctrl-C quit"
}

func badge(s model.SupportState, plain bool) string {
	labels := map[model.SupportState]string{
		model.SupportFull:                "FULL",
		model.SupportStructureOnly:       "STRUCTURE ONLY",
		model.SupportPartial:             "PARTIAL",
		model.SupportImporterUnsupported: "IMPORTER UNSUPPORTED",
		model.SupportTargetUnsupported:   "TARGET UNSUPPORTED",
	}
	labelText, ok := labels[s]
	if !ok {
		labelText = "UNKNOWN"
	}
	label := "[" + labelText + "]"
	if plain {
		return label
	}
	color := "2"
	if s == model.SupportPartial || s == model.SupportStructureOnly {
		color = "3"
	}
	if s == model.SupportImporterUnsupported || s == model.SupportTargetUnsupported {
		color = "1"
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color(color)).Render(label)
}

func bytesLabel(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	if n < 1<<20 {
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	if n < 1<<30 {
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	}
	return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
}

// titleCase capitalizes the first rune of value; strings.Title is deprecated.
func titleCase(value string) string {
	if value == "" {
		return value
	}
	r := []rune(value)
	r[0] = unicode.ToUpper(r[0])
	return string(r)
}
