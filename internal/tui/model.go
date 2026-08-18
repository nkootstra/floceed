package tui

import (
	"context"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"github.com/nkootstra/floceed/internal/app"
	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type Screen string

const (
	ScreenLoading   Screen = "loading"
	ScreenProfiles  Screen = "profiles"
	ScreenRegion    Screen = "region"
	ScreenIdentity  Screen = "identity"
	ScreenServices  Screen = "services"
	ScreenResources Screen = "resources"
	ScreenOptions   Screen = "options"
	ScreenReview    Screen = "review"
	ScreenSummary   Screen = "summary"
	ScreenConfirm   Screen = "confirm"
	ScreenProgress  Screen = "progress"
	ScreenResult    Screen = "result"
)

type Options struct {
	NoColor        bool
	ProjectFile    string
	Profile        string
	Region         string
	FixtureProfile string
}

type Model struct {
	ctx             context.Context
	cancel          context.CancelFunc
	backend         Backend
	opts            Options
	screen          Screen
	profiles        []Profile
	profile, region string
	identity        awsconfig.Identity
	services        []model.ServiceDescriptor
	serviceSelected map[string]bool
	resources       []model.ResourceSummary
	selected        map[string]bool
	dataEnabled     map[string]bool
	dataMode        map[string]config.DataMode
	findings        []model.Finding
	plan            app.Plan
	manifest        model.Manifest
	cursor          int
	busy            bool
	filtering       bool
	filter          textinput.Model
	regionInput     textinput.Model
	err             error
	progress        model.ProgressEvent
	pullUpdates     chan tea.Msg
}

type profilesLoadedMsg struct {
	profiles []Profile
	err      error
}
type identityLoadedMsg struct {
	identity awsconfig.Identity
	err      error
}
type scanFinishedMsg struct {
	result app.ScanResult
	err    error
}
type planFinishedMsg struct {
	plan app.Plan
	err  error
}
type pullFinishedMsg struct {
	manifest model.Manifest
	err      error
}
type pullProgressMsg struct{ event model.ProgressEvent }

func NewModel(backend Backend, opts Options) Model {
	if opts.ProjectFile == "" {
		opts.ProjectFile = app.DefaultProjectFile
	}
	filter := textinput.New()
	filter.Placeholder = "filter resources"
	filter.Prompt = "/ "
	region := textinput.New()
	region.Placeholder = "eu-west-1"
	region.Prompt = "Region: "
	region.SetValue(opts.Region)
	if opts.NoColor {
		filter.SetStyles(textinput.Styles{})
		region.SetStyles(textinput.Styles{})
		filter.SetVirtualCursor(false)
		region.SetVirtualCursor(false)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return Model{
		ctx: ctx, cancel: cancel, backend: backend, opts: opts, screen: ScreenLoading,
		profile: opts.Profile, region: opts.Region, services: []model.ServiceDescriptor{
			{Name: "s3", DisplayName: "Amazon S3", Support: model.SupportPartial},
			{Name: "dynamodb", DisplayName: "Amazon DynamoDB", Support: model.SupportPartial},
			{Name: "kinesis", DisplayName: "Amazon Kinesis", Support: model.SupportStructureOnly},
		},
		serviceSelected: map[string]bool{"s3": true, "dynamodb": true, "kinesis": false}, selected: map[string]bool{},
		dataEnabled: map[string]bool{}, dataMode: map[string]config.DataMode{}, filter: filter, regionInput: region,
	}
}

func (m Model) Screen() Screen { return m.screen }
func (m Model) Init() tea.Cmd  { return m.loadProfiles() }

func (m Model) loadProfiles() tea.Cmd {
	return func() tea.Msg { p, err := m.backend.Profiles(m.ctx); return profilesLoadedMsg{p, err} }
}
func (m Model) loadIdentity() tea.Cmd {
	return func() tea.Msg {
		id, err := m.backend.Identity(m.ctx, m.profile, m.region)
		return identityLoadedMsg{id, err}
	}
}
func (m Model) scan() tea.Cmd {
	services := make([]string, 0, len(m.services))
	for _, service := range m.services {
		if m.serviceSelected[service.Name] {
			services = append(services, service.Name)
		}
	}
	return func() tea.Msg {
		r, err := m.backend.Scan(m.ctx, app.ScanRequest{Profile: m.profile, Region: m.region, Services: services})
		return scanFinishedMsg{r, err}
	}
}
func (m Model) makePlan() tea.Cmd {
	req := m.request()
	return func() tea.Msg { p, err := m.backend.Plan(m.ctx, req); return planFinishedMsg{p, err} }
}
func (m *Model) pull() tea.Cmd {
	m.pullUpdates = make(chan tea.Msg, 16)
	req := m.request()
	req.Progress = func(event model.ProgressEvent) {
		select {
		case m.pullUpdates <- pullProgressMsg{event}:
		case <-m.ctx.Done():
		}
	}
	updates := m.pullUpdates
	go func() {
		result, err := m.backend.SaveAndPull(m.ctx, req)
		select {
		case updates <- pullFinishedMsg{result, err}:
		case <-m.ctx.Done():
		}
		close(updates)
	}()
	return waitPullUpdate(updates)
}
func waitPullUpdate(updates <-chan tea.Msg) tea.Cmd { return func() tea.Msg { return <-updates } }
