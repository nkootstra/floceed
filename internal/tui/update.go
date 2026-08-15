package tui

import (
	"slices"
	"strings"

	tea "charm.land/bubbletea/v2"
)

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case profilesLoadedMsg:
		m.busy, m.err = false, msg.err
		if msg.err == nil {
			m.profiles, m.screen, m.cursor = msg.profiles, ScreenProfiles, 0
		}
		return m, nil
	case identityLoadedMsg:
		m.busy, m.err, m.identity = false, msg.err, msg.identity
		return m, nil
	case scanFinishedMsg:
		m.busy, m.err = false, msg.err
		if msg.err == nil {
			m.mergeResources(msg.result.Resources)
			m.findings = slices.Clone(msg.result.Findings)
			m.screen, m.cursor = ScreenResources, 0
		}
		return m, nil
	case planFinishedMsg:
		m.busy, m.err, m.plan = false, msg.err, msg.plan
		if msg.err == nil {
			m.findings = slices.Clone(msg.plan.Findings)
			m.screen, m.cursor = ScreenReview, 0
		}
		return m, nil
	case pullFinishedMsg:
		m.busy, m.err, m.manifest, m.screen = false, msg.err, msg.manifest, ScreenResult
		return m, nil
	case tea.WindowSizeMsg:
		return m, nil
	case tea.KeyPressMsg:
		return m.updateKey(msg)
	}
	return m, nil
}

func (m Model) updateKey(key tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	k := key.String()
	if k == "ctrl+c" {
		return m, tea.Quit
	}
	if m.filtering {
		if k == "esc" {
			m.filtering = false
			m.filter.Blur()
			m.clampCursor()
			return m, nil
		}
		if k == "enter" {
			m.filtering = false
			m.filter.Blur()
			m.cursor = 0
			return m, nil
		}
		var cmd tea.Cmd
		m.filter, cmd = m.filter.Update(key)
		return m, cmd
	}
	if m.screen == ScreenRegion {
		if k == "enter" && strings.TrimSpace(m.regionInput.Value()) != "" {
			m.region = strings.TrimSpace(m.regionInput.Value())
			m.regionInput.Blur()
			m.screen, m.busy = ScreenIdentity, true
			return m, m.loadIdentity()
		}
		if k != "esc" {
			var cmd tea.Cmd
			m.regionInput, cmd = m.regionInput.Update(key)
			return m, cmd
		}
	}
	if k == "esc" {
		m.back()
		return m, nil
	}
	if m.busy {
		return m, nil
	}
	if k == "/" && m.screen == ScreenResources {
		m.filtering = true
		return m, m.filter.Focus()
	}
	items := m.itemCount()
	switch k {
	case "j", "down":
		if items > 0 {
			m.cursor = (m.cursor + 1) % items
		}
	case "k", "up":
		if items > 0 {
			m.cursor = (m.cursor - 1 + items) % items
		}
	case " ", "space":
		m.toggle()
	case "enter":
		return m.advance()
	case "y", "Y":
		if m.screen == ScreenConfirm {
			m.busy, m.screen = true, ScreenProgress
			return m, m.pull()
		}
	case "n", "N":
		if m.screen == ScreenConfirm {
			m.screen = ScreenSummary
		}
	}
	return m, nil
}

func (m *Model) advance() (tea.Model, tea.Cmd) {
	switch m.screen {
	case ScreenProfiles:
		if len(m.profiles) == 0 {
			return *m, nil
		}
		p := m.profiles[m.cursor]
		m.profile = p.Name
		if m.region == "" {
			m.region = p.Region
		}
		if m.region == "" {
			m.screen = ScreenRegion
			m.regionInput.Focus()
			return *m, nil
		}
		m.screen, m.busy = ScreenIdentity, true
		return *m, m.loadIdentity()
	case ScreenIdentity:
		if m.err == nil && m.identity.AccountID != "" {
			m.screen, m.cursor = ScreenServices, 0
		}
	case ScreenServices:
		m.busy = true
		return *m, m.scan()
	case ScreenResources:
		if len(m.selected) > 0 {
			m.screen, m.cursor = ScreenOptions, 0
		}
	case ScreenOptions:
		m.busy = true
		return *m, m.makePlan()
	case ScreenReview:
		m.screen = ScreenSummary
	case ScreenSummary:
		m.screen = ScreenConfirm
	case ScreenResult:
		return *m, tea.Quit
	}
	return *m, nil
}

func (m *Model) back() {
	switch m.screen {
	case ScreenRegion:
		m.screen = ScreenProfiles
	case ScreenIdentity:
		if m.busy {
			return
		}
		m.screen = ScreenProfiles
	case ScreenServices:
		m.screen = ScreenIdentity
	case ScreenResources:
		m.screen = ScreenServices
	case ScreenOptions:
		m.screen = ScreenResources
	case ScreenReview:
		m.screen = ScreenOptions
	case ScreenSummary:
		m.screen = ScreenReview
	case ScreenConfirm:
		m.screen = ScreenSummary
	}
	m.cursor = 0
}

func (m *Model) toggle() {
	switch m.screen {
	case ScreenServices:
		if len(m.services) > 0 {
			n := m.services[m.cursor].Name
			m.serviceSelected[n] = !m.serviceSelected[n]
		}
	case ScreenResources:
		items := m.visibleResources()
		if m.cursor >= 0 && m.cursor < len(items) {
			k := resourceKey(items[m.cursor].Ref)
			if m.selected[k] {
				delete(m.selected, k)
				delete(m.dataEnabled, k)
			} else {
				m.selected[k] = true
			}
		}
	case ScreenOptions:
		items := m.selectedResources()
		if m.cursor >= 0 && m.cursor < len(items) {
			k := resourceKey(items[m.cursor].Ref)
			m.dataEnabled[k] = !m.dataEnabled[k]
		}
	}
}

func (m *Model) clampCursor() {
	items := m.itemCount()
	if items == 0 {
		m.cursor = 0
	} else if m.cursor < 0 {
		m.cursor = 0
	} else if m.cursor >= items {
		m.cursor = items - 1
	}
}

func (m Model) itemCount() int {
	switch m.screen {
	case ScreenProfiles:
		return len(m.profiles)
	case ScreenServices:
		return len(m.services)
	case ScreenResources:
		return len(m.visibleResources())
	case ScreenOptions:
		return len(m.selectedResources())
	}
	return 0
}
