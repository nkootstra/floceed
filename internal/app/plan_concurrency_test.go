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
)

type captureTestAdapter struct {
	capture func(context.Context, model.ResourceRef) (*model.Snapshot, error)

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
func (a *captureTestAdapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, _ model.CaptureOptions) (*model.Snapshot, error) {
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
		_, _, err := service.capture(context.Background(), captureRequest{Project: captureTestProject("09", "08", "07", "06", "05", "04", "03", "02", "01")})
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
		plan, snapshots, err := service.capture(context.Background(), captureRequest{Project: captureTestProject("c", "a", "b")})
		done <- result{plan: plan, snapshots: snapshots, err: err}
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
		_, _, err := service.capture(context.Background(), captureRequest{Project: captureTestProject("b", "a")})
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
