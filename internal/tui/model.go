package tui

import (
	"context"
	"slices"

	"charm.land/bubbles/v2/spinner"
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
	ctx     context.Context
	cancel  context.CancelFunc
	backend Backend
	opts    Options
	screen  Screen
	// pending is the screen that launched an in-flight async operation.
	// Completion handlers only navigate forward if the user is still on that
	// screen (i.e. has not navigated away in the meantime).
	pending          Screen
	profiles         []Profile
	profile, region  string
	identity         awsconfig.Identity
	services         []model.ServiceDescriptor
	serviceSelected  map[string]bool
	resources        []model.ResourceSummary
	selected         map[string]bool
	dataChoice       map[string]config.DataMode
	findings         []model.Finding
	permissionChecks []app.Check
	plan             app.Plan
	manifest         model.Manifest
	cursor           int
	busy             bool
	filtering        bool
	filter           textinput.Model
	regionInput      textinput.Model
	spinner          spinner.Model
	scanCancel       context.CancelFunc
	scanToken        uint64
	planToken        uint64
	pullToken        uint64
	err              error
	progress         model.ProgressEvent
	pullUpdates      chan tea.Msg
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
	token  uint64
}
type planFinishedMsg struct {
	plan        app.Plan
	permissions app.PermissionResult
	err         error
	token       uint64
}
type pullFinishedMsg struct {
	manifest model.Manifest
	err      error
	token    uint64
}
type pullProgressMsg struct {
	event model.ProgressEvent
	token uint64
}

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
	progressSpinner := spinner.New(spinner.WithSpinner(spinner.Dot))
	if opts.NoColor {
		filter.SetStyles(textinput.Styles{})
		region.SetStyles(textinput.Styles{})
		filter.SetVirtualCursor(false)
		region.SetVirtualCursor(false)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return Model{
		ctx: ctx, cancel: cancel, backend: backend, opts: opts, screen: ScreenLoading,
		profile: opts.Profile, region: opts.Region, services: defaultServiceDescriptors(),
		serviceSelected: map[string]bool{"s3": true, "dynamodb": true, "kinesis": false, "sns": false, "sqs": false, "events": false, "lambda": false, "secretsmanager": false, "ssm": false, "apigateway": false, "stepfunctions": false, "logs": false}, selected: map[string]bool{},
		dataChoice: map[string]config.DataMode{}, filter: filter, regionInput: region, spinner: progressSpinner,
	}
}

func defaultServiceDescriptors() []model.ServiceDescriptor {
	facts := model.SupportedServiceFacts()
	preferred := []string{"s3", "dynamodb"}
	services := make([]model.ServiceDescriptor, 0, len(facts))
	for _, name := range preferred {
		for _, fact := range facts {
			if fact.Name == name {
				services = append(services, fact.ServiceDescriptor)
			}
		}
	}
	for _, fact := range facts {
		if !slices.Contains(preferred, fact.Name) {
			services = append(services, fact.ServiceDescriptor)
		}
	}
	return services
}

func (m Model) Screen() Screen { return m.screen }
func (m Model) Init() tea.Cmd  { return m.loadProfiles() }

func (m Model) hasFailedPermissions() bool {
	for _, check := range m.permissionChecks {
		if check.Blocking && !check.OK {
			return true
		}
	}
	return false
}

func (m Model) loadProfiles() tea.Cmd {
	return func() tea.Msg { p, err := m.backend.Profiles(m.ctx); return profilesLoadedMsg{p, err} }
}
func (m Model) loadIdentity() tea.Cmd {
	return func() tea.Msg {
		id, err := m.backend.Identity(m.ctx, m.profile, m.region)
		return identityLoadedMsg{id, err}
	}
}
func (m *Model) scan() tea.Cmd {
	services := make([]string, 0, len(m.services))
	for _, service := range m.services {
		if m.serviceSelected[service.Name] {
			services = append(services, service.Name)
		}
	}
	m.scanToken++
	token := m.scanToken
	scanCtx, cancel := context.WithCancel(m.ctx)
	m.scanCancel = cancel
	return func() tea.Msg {
		defer cancel()
		r, err := m.backend.Scan(scanCtx, app.ScanRequest{Profile: m.profile, Region: m.region, Services: services})
		return scanFinishedMsg{result: r, err: err, token: token}
	}
}
func (m *Model) makePlan() tea.Cmd {
	m.planToken++
	token := m.planToken
	req := m.request()
	return func() tea.Msg {
		p, err := m.backend.Plan(m.ctx, req)
		if err != nil {
			return planFinishedMsg{plan: p, err: err, token: token}
		}
		permissions, err := m.backend.Preflight(m.ctx, req)
		return planFinishedMsg{plan: p, permissions: permissions, err: err, token: token}
	}
}
func (m *Model) pull() tea.Cmd {
	m.pullToken++
	token := m.pullToken
	m.pullUpdates = make(chan tea.Msg, 16)
	req := m.request()
	req.Progress = func(event model.ProgressEvent) {
		select {
		case m.pullUpdates <- pullProgressMsg{event: event, token: token}:
		case <-m.ctx.Done():
		}
	}
	updates := m.pullUpdates
	go func() {
		result, err := m.backend.SaveAndPull(m.ctx, req)
		select {
		case updates <- pullFinishedMsg{manifest: result, err: err, token: token}:
		case <-m.ctx.Done():
		}
		close(updates)
	}()
	return waitPullUpdate(updates)
}
func waitPullUpdate(updates <-chan tea.Msg) tea.Cmd { return func() tea.Msg { return <-updates } }
