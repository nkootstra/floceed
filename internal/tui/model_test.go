package tui

import (
	"context"
	"reflect"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/nkootstra/floceed/internal/app"
	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/model"
)

type fakeBackend struct {
	scanRequests chan<- app.ScanRequest
	planRequests chan<- ProjectRequest
}

func (fakeBackend) Profiles(context.Context) ([]Profile, error) {
	return []Profile{{Name: "dev", Region: "eu-west-1"}}, nil
}
func (fakeBackend) Identity(context.Context, string, string) (awsconfig.Identity, error) {
	return awsconfig.Identity{AccountID: "123456789012", ARN: "arn:aws:iam::123456789012:user/dev"}, nil
}
func (b fakeBackend) Scan(_ context.Context, req app.ScanRequest) (app.ScanResult, error) {
	if b.scanRequests != nil {
		b.scanRequests <- req
	}
	return app.ScanResult{}, nil
}
func (b fakeBackend) Plan(_ context.Context, req ProjectRequest) (app.Plan, error) {
	if b.planRequests != nil {
		b.planRequests <- req
	}
	return app.Plan{}, nil
}
func (fakeBackend) SaveAndPull(context.Context, ProjectRequest) (model.Manifest, error) {
	return model.Manifest{SchemaVersion: 1}, nil
}

func TestProfileToRegionAndIdentityFlow(t *testing.T) {
	m := NewModel(fakeBackend{}, Options{})
	m = update(t, m, profilesLoadedMsg{profiles: []Profile{{Name: "dev", Region: "eu-west-1"}}})
	if m.Screen() != ScreenProfiles {
		t.Fatalf("screen = %s", m.Screen())
	}
	m = press(t, m, "enter")
	if m.Screen() != ScreenIdentity {
		t.Fatalf("profile with region should skip region entry, got %s", m.Screen())
	}
	m = update(t, m, identityLoadedMsg{identity: awsconfig.Identity{AccountID: "123456789012"}})
	m = press(t, m, "enter")
	if m.Screen() != ScreenServices {
		t.Fatalf("screen = %s", m.Screen())
	}
}

func TestScanRequestsOnlySelectedServices(t *testing.T) {
	requests := make(chan app.ScanRequest, 1)
	m := NewModel(fakeBackend{scanRequests: requests}, Options{})
	m.profile = "dev"
	m.region = "eu-west-1"
	m.serviceSelected["dynamodb"] = false

	msg := m.scan()()
	if result := msg.(scanFinishedMsg); result.err != nil {
		t.Fatal(result.err)
	}
	req := <-requests
	if !reflect.DeepEqual(req.Services, []string{"s3"}) {
		t.Fatalf("services = %#v, want []string{\"s3\"}", req.Services)
	}
}

func TestMissingProfileRegionRequiresRegionEntry(t *testing.T) {
	m := NewModel(fakeBackend{}, Options{})
	m = update(t, m, profilesLoadedMsg{profiles: []Profile{{Name: "dev"}}})
	m = press(t, m, "enter")
	if m.Screen() != ScreenRegion {
		t.Fatalf("screen = %s", m.Screen())
	}
}

func TestResourceSelectionsSurviveServiceFailure(t *testing.T) {
	m := NewModel(fakeBackend{}, Options{})
	m.screen = ScreenResources
	m.resources = []model.ResourceSummary{{Ref: model.ResourceRef{Service: "s3", ID: "assets"}, Name: "assets"}}
	m.selected[resourceKey(m.resources[0].Ref)] = true
	m = update(t, m, scanFinishedMsg{result: app.ScanResult{
		Resources: []model.ResourceSummary{{Ref: model.ResourceRef{Service: "dynamodb", ID: "users"}, Name: "users"}},
		Findings:  []model.Finding{{Code: "SERVICE_DISCOVERY_FAILED", Resource: "s3", Message: "denied"}},
	}})
	if !m.selected["s3/assets"] {
		t.Fatal("completed selection was lost")
	}
	if len(m.findings) != 1 {
		t.Fatalf("findings = %d", len(m.findings))
	}
}

func TestEscapingNarrowedResourceFilterClampsCursor(t *testing.T) {
	m := NewModel(fakeBackend{}, Options{})
	m.screen = ScreenResources
	m.resources = []model.ResourceSummary{
		{Ref: model.ResourceRef{Service: "s3", ID: "archive"}, Name: "archive"},
		{Ref: model.ResourceRef{Service: "s3", ID: "assets"}, Name: "assets"},
		{Ref: model.ResourceRef{Service: "s3", ID: "logs"}, Name: "logs"},
	}
	m.cursor = 2
	m.filtering = true
	m.filter.SetValue("assets")

	m = press(t, m, "esc")
	if m.cursor != 0 {
		t.Fatalf("cursor = %d, want 0 for the sole visible resource", m.cursor)
	}
	m = press(t, m, " ")
	if !m.selected["s3/assets"] {
		t.Fatal("space did not select the visible resource after escaping the filter")
	}
}

func TestRescanDropsDeselectedServiceStateAndPreservesSelectedServiceResources(t *testing.T) {
	m := NewModel(fakeBackend{}, Options{})
	m.screen = ScreenResources
	m.resources = []model.ResourceSummary{
		{Ref: model.ResourceRef{Service: "s3", ID: "assets"}, Name: "assets"},
		{Ref: model.ResourceRef{Service: "dynamodb", ID: "users"}, Name: "users"},
	}
	m.selected["s3/assets"] = true
	m.selected["dynamodb/users"] = true
	m.dataEnabled["s3/assets"] = true
	m.dataEnabled["dynamodb/users"] = true

	m.back()
	m.cursor = 0 // s3
	m.toggle()
	m.mergeResources(nil) // Both discoveries failed during the rescan.

	if len(m.resources) != 1 || resourceKey(m.resources[0].Ref) != "dynamodb/users" {
		t.Fatalf("resources = %#v, want only retained dynamodb resource", m.resources)
	}
	if m.selected["s3/assets"] || m.dataEnabled["s3/assets"] {
		t.Fatal("deselected service retained selection or data state")
	}
	if !m.selected["dynamodb/users"] || !m.dataEnabled["dynamodb/users"] {
		t.Fatal("selected service state was lost during transient discovery failure")
	}
}

func TestAsyncResultsReplaceStaleFindings(t *testing.T) {
	m := NewModel(fakeBackend{}, Options{})
	m.findings = []model.Finding{{Code: "STALE"}}

	m = update(t, m, scanFinishedMsg{result: app.ScanResult{
		Findings: []model.Finding{{Code: "SCAN_WARNING"}},
	}})
	if len(m.findings) != 1 || m.findings[0].Code != "SCAN_WARNING" {
		t.Fatalf("scan findings = %#v, want only current scan findings", m.findings)
	}

	m = update(t, m, planFinishedMsg{plan: app.Plan{
		Findings: []model.Finding{{Code: "PLAN_WARNING"}},
	}})
	if len(m.findings) != 1 || m.findings[0].Code != "PLAN_WARNING" {
		t.Fatalf("plan findings = %#v, want only current plan findings", m.findings)
	}
}

func TestSafeDataDefaultsAndFinalConfirmation(t *testing.T) {
	m := NewModel(fakeBackend{}, Options{})
	m.profile, m.region = "dev", "eu-west-1"
	m.identity = awsconfig.Identity{AccountID: "123456789012"}
	m.resources = []model.ResourceSummary{
		{Ref: model.ResourceRef{Service: "s3", ID: "assets"}, Name: "assets"},
		{Ref: model.ResourceRef{Service: "dynamodb", ID: "users"}, Name: "users"},
	}
	m.selected["s3/assets"] = true
	m.selected["dynamodb/users"] = true
	m.screen = ScreenOptions
	m = press(t, m, "space")
	project := m.Project()
	if d := project.Resources.S3[0].Data; d == nil || d.MaxObjects != 100 || d.MaxObjectBytes != 10<<20 || d.MaxTotalBytes != 100<<20 {
		t.Fatalf("unsafe S3 defaults: %#v", d)
	}
	m.cursor = 1
	m = press(t, m, "space")
	project = m.Project()
	if d := project.Resources.DynamoDB[0].Data; d == nil || d.MaxItems != 1000 || d.MaxPages != 100 || d.Gzip == nil || !*d.Gzip {
		t.Fatalf("unsafe DynamoDB defaults: %#v", d)
	}
	m.screen = ScreenSummary
	m = press(t, m, "enter")
	if m.Screen() != ScreenConfirm {
		t.Fatalf("screen = %s", m.Screen())
	}
	m = press(t, m, "y")
	if m.Screen() != ScreenProgress {
		t.Fatalf("confirmation did not start generation: screen=%s", m.Screen())
	}
}

func TestFixtureProfileOptionReachesPlanFromInteractiveFlow(t *testing.T) {
	requests := make(chan ProjectRequest, 1)
	m := NewModel(fakeBackend{planRequests: requests}, Options{FixtureProfile: "share-safe"})
	m.screen = ScreenOptions

	updated, cmd := m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("entering the review flow did not start planning")
	}
	msg := cmd()
	if result := msg.(planFinishedMsg); result.err != nil {
		t.Fatal(result.err)
	}
	req := <-requests
	if req.FixtureProfile != "share-safe" {
		t.Fatalf("fixture profile = %q, want share-safe", req.FixtureProfile)
	}
}

func TestPlainViewHasBreadcrumbAndHelp(t *testing.T) {
	m := NewModel(fakeBackend{}, Options{NoColor: true})
	m.screen = ScreenServices
	v := m.View().Content
	if !containsAll(v, "Profile", "Identity", "Services", "j/k", "Enter", "Ctrl-C") {
		t.Fatalf("view lacks navigation context:\n%s", v)
	}
}

func TestProgressViewShowsExplicitRemainingWork(t *testing.T) {
	m := NewModel(fakeBackend{}, Options{NoColor: true})
	m.screen = ScreenProgress
	m.progress = model.ProgressEvent{Phase: "capture", Service: "s3", Resource: "assets", CompletedRecords: 25, TotalRecords: 100, RemainingRecords: 75, CompletedBytes: 1 << 20, TotalBytes: 4 << 20, RemainingBytes: 3 << 20}
	view := m.View().Content
	if !containsAll(view, "25 / 100 records (75 remaining)", "1.0 MiB / 4.0 MiB (3.0 MiB remaining)") {
		t.Fatalf("progress view omitted remaining work:\n%s", view)
	}
}

func TestNoColorViewsContainNoANSI(t *testing.T) {
	m := NewModel(fakeBackend{}, Options{NoColor: true})
	m.screen = ScreenRegion
	m.regionInput.Focus()
	if view := m.View().Content; strings.Contains(view, "\x1b[") {
		t.Fatalf("plain view contains ANSI: %q", view)
	}
	for _, state := range []model.SupportState{model.SupportFull, model.SupportStructureOnly, model.SupportPartial, model.SupportImporterUnsupported, model.SupportTargetUnsupported} {
		if got := badge(state, true); got == "[]" || strings.Contains(got, "\x1b[") {
			t.Fatalf("plain badge %s = %q", state, got)
		}
	}
	if got := badge(model.SupportTargetUnsupported, true); got != "[TARGET UNSUPPORTED]" {
		t.Fatalf("target unsupported badge = %q", got)
	}
}

func TestIdentityRunsAsTypedCommand(t *testing.T) {
	m := NewModel(fakeBackend{}, Options{})
	m.profile, m.region, m.screen = "dev", "eu-west-1", ScreenIdentity
	msg := m.loadIdentity()()
	loaded, ok := msg.(identityLoadedMsg)
	if !ok || loaded.err != nil || loaded.identity.AccountID != "123456789012" {
		t.Fatalf("identity command returned %#v", msg)
	}
}

func update(t *testing.T, m Model, msg tea.Msg) Model {
	t.Helper()
	updated, _ := m.Update(msg)
	return updated.(Model)
}

func press(t *testing.T, m Model, key string) Model {
	t.Helper()
	code := []rune(key)[0]
	text := key
	switch key {
	case "enter":
		code, text = tea.KeyEnter, ""
	case " ":
		code, text = tea.KeySpace, " "
	}
	return update(t, m, tea.KeyPressMsg(tea.Key{Code: code, Text: text}))
}

func containsAll(value string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(value, needle) {
			return false
		}
	}
	return true
}
