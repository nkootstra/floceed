package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/services/dynamodb"
	"github.com/nkootstra/floceed/internal/services/s3"
)

type captureTestAdapter struct {
	capture func(context.Context, model.ResourceRef) (*model.Snapshot, error)
	audit   func(model.ResourceRef, model.CaptureOptions)

	validateMu    sync.Mutex
	validated     []string
	validationLog bool
}

func (*captureTestAdapter) Service() model.ServiceDescriptor {
	return model.ServiceDescriptor{Name: "s3"}
}
func (a *captureTestAdapter) Plan(p config.Project, _ bool) catalog.PlanContribution {
	result := catalog.PlanContribution{RequiredIAMActions: []string{"s3:GetBucketLocation"}}
	for _, resource := range p.Resources.S3 {
		result.Selections = append(result.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "s3", Type: "bucket", ID: resource.Name}})
	}
	return result
}
func (*captureTestAdapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}
func (*captureTestAdapter) Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error) {
	return model.DiscoveryResult{}, nil
}
func (a *captureTestAdapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, options model.CaptureOptions) (*model.Snapshot, error) {
	if a.audit != nil {
		a.audit(ref, options)
	}
	return a.capture(ctx, ref)
}
func (*captureTestAdapter) Dependencies(*model.Snapshot) []model.Dependency { return nil }
func (a *captureTestAdapter) Validate(snapshot *model.Snapshot, _ model.Capabilities) []model.Finding {
	if a.validationLog {
		a.validateMu.Lock()
		a.validated = append(a.validated, snapshot.Resource.ID)
		a.validateMu.Unlock()
	}
	return nil
}

func captureTestProject(names ...string) config.Project {
	project := config.Project{Source: config.Source{Region: "eu-west-1"}}
	for _, name := range names {
		project.Resources.S3 = append(project.Resources.S3, config.S3Resource{Name: name})
	}
	return project
}

func captureTestApplication(t *testing.T, adapter catalog.Adapter) *Application {
	t.Helper()
	registry, err := catalog.New(adapter)
	if err != nil {
		t.Fatal(err)
	}
	service := New("test")
	service.Factory = registryFactory{registry: registry}
	return service
}

func TestCaptureBoundsConcurrentAdapterCalls(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	started := make(chan struct{}, 12)
	release := make(chan struct{})
	adapter := &captureTestAdapter{capture: func(ctx context.Context, ref model.ResourceRef) (*model.Snapshot, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		started <- struct{}{}
		select {
		case <-release:
			return model.NewSnapshot(ref, "s3", map[string]string{"name": ref.ID})
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	service := captureTestApplication(t, adapter)
	done := make(chan error, 1)
	go func() {
		_, err := service.capture(context.Background(), captureRequest{Project: captureTestProject("09", "08", "07", "06", "05", "04", "03", "02", "01")})
		done <- err
	}()

	for range captureConcurrency {
		<-started
	}
	if got := maximum.Load(); got != captureConcurrency {
		t.Fatalf("maximum concurrent captures = %d, want %d", got, captureConcurrency)
	}
	select {
	case <-started:
		t.Fatalf("more than %d captures started while the limit was occupied", captureConcurrency)
	default:
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := maximum.Load(); got > captureConcurrency {
		t.Fatalf("maximum concurrent captures = %d, exceeds %d", got, captureConcurrency)
	}
}

func TestCaptureFoldsCompletedWorkInSelectionOrder(t *testing.T) {
	gates := map[string]chan struct{}{"a": make(chan struct{}), "b": make(chan struct{}), "c": make(chan struct{})}
	started := make(chan string, len(gates))
	completed := make(chan string, len(gates))
	adapter := &captureTestAdapter{validationLog: true}
	adapter.capture = func(_ context.Context, ref model.ResourceRef) (*model.Snapshot, error) {
		started <- ref.ID
		<-gates[ref.ID]
		completed <- ref.ID
		return model.NewSnapshot(ref, "s3", map[string]string{"name": ref.ID})
	}
	service := captureTestApplication(t, adapter)
	type result struct {
		plan      Plan
		snapshots []model.Snapshot
		err       error
	}
	done := make(chan result, 1)
	go func() {
		captured, err := service.capture(context.Background(), captureRequest{Project: captureTestProject("c", "a", "b")})
		done <- result{plan: captured.Plan, snapshots: captured.Snapshots, err: err}
	}()
	for range gates {
		<-started
	}
	for _, name := range []string{"c", "b", "a"} {
		close(gates[name])
		if got := <-completed; got != name {
			t.Fatalf("completed capture = %q, want %q", got, name)
		}
	}
	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	for i, want := range []string{"a", "b", "c"} {
		if got.snapshots[i].Resource.ID != want || got.plan.Selected[i].ID != want || adapter.validated[i] != want {
			t.Fatalf("index %d: snapshot=%q selected=%q validated=%q, want %q", i, got.snapshots[i].Resource.ID, got.plan.Selected[i].ID, adapter.validated[i], want)
		}
	}
}

func TestCaptureAggregatesGovernanceAuditDeterministicallyAfterConcurrentCaptures(t *testing.T) {
	gates := map[string]chan struct{}{"a": make(chan struct{}), "b": make(chan struct{})}
	adapter := &captureTestAdapter{
		audit: func(ref model.ResourceRef, options model.CaptureOptions) {
			for range map[string]int{"a": 1, "b": 10}[ref.ID] {
				options.GovernanceAudit.Record("rule-" + ref.ID)
			}
		},
		capture: func(_ context.Context, ref model.ResourceRef) (*model.Snapshot, error) {
			<-gates[ref.ID]
			return model.NewSnapshot(ref, "s3", map[string]string{"name": ref.ID})
		},
	}
	service := captureTestApplication(t, adapter)
	project := captureTestProject("b", "a")
	project.FixtureProfiles = map[string]config.FixtureProfile{"safe": {Rules: []config.GovernanceRule{
		{ID: "rule-b", Service: "s3", Resource: "b", Target: config.GovernanceTarget{Kind: "s3_text_body"}, Action: "replace", Replacement: "safe"},
		{ID: "rule-a", Service: "s3", Resource: "a", Target: config.GovernanceTarget{Kind: "s3_text_body"}, Action: "omit"},
	}}}
	done := make(chan Plan, 1)
	policy, err := project.ResolveFixtureProfile("safe", func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		captured, _ := service.capture(context.Background(), captureRequest{Project: project, Governance: policy})
		done <- captured.Plan
	}()
	close(gates["b"])
	close(gates["a"])
	plan := <-done
	if plan.Governance == nil || len(plan.Governance.Rules) != 2 {
		t.Fatalf("governance audit = %#v", plan.Governance)
	}
	if got := plan.Governance.Rules; got[0].RuleID != "rule-a" || got[0].Count != model.CountBucket1To9 || got[1].RuleID != "rule-b" || got[1].Count != model.CountBucket10To99 {
		t.Fatalf("rule audit = %#v, want stable rule order and disclosure buckets", got)
	}
}

func TestCaptureReturnsLowestSelectionFailure(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	adapter := &captureTestAdapter{capture: func(_ context.Context, ref model.ResourceRef) (*model.Snapshot, error) {
		started <- struct{}{}
		<-release
		return nil, fmt.Errorf("capture %s failed", ref.ID)
	}}
	service := captureTestApplication(t, adapter)
	done := make(chan error, 1)
	go func() {
		_, err := service.capture(context.Background(), captureRequest{Project: captureTestProject("b", "a")})
		done <- err
	}()
	<-started
	<-started
	close(release)
	err := <-done
	if err == nil || err.Error() != "capture a failed" {
		t.Fatalf("capture error = %v, want lowest sorted selection failure", err)
	}
	var appErr *Error
	if !errors.As(err, &appErr) || appErr.Kind != ErrorSource || appErr.Code != "AWS_SOURCE_FAILED" {
		t.Fatalf("capture error = %#v, want mapped source error", err)
	}
}

func TestCaptureErrorMapsCheckpointSentinelsToDistinctCodes(t *testing.T) {
	// The sentinel errors are package-level and only reachable through real
	// adapters; drive captureError directly to pin the code mapping.
	for _, tt := range []struct {
		err  error
		code string
	}{
		{errors.New("aws exploded"), "AWS_SOURCE_FAILED"},
		{fmt.Errorf("wrap: %w", dynamodb.ErrCheckpointCorrupt), "CHECKPOINT_CORRUPT"},
		{fmt.Errorf("wrap: %w", dynamodb.ErrCheckpointIncompatible), "CHECKPOINT_INCOMPATIBLE"},
		{fmt.Errorf("wrap: %w", s3.ErrCheckpointCorrupt), "CHECKPOINT_CORRUPT"},
		{fmt.Errorf("wrap: %w", s3.ErrCheckpointIncompatible), "CHECKPOINT_INCOMPATIBLE"},
	} {
		var appErr *Error
		if !errors.As(captureError(tt.err), &appErr) {
			t.Fatalf("captureError(%v) = %T, want *Error", tt.err, captureError(tt.err))
		}
		if appErr.Code != tt.code {
			t.Fatalf("captureError(%v) code = %q, want %q", tt.err, appErr.Code, tt.code)
		}
	}
}
