package catalog

import (
	"context"
	"fmt"
	"sort"

	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type Selection struct {
	Resource model.ResourceRef
	Options  model.CaptureOptions
}

type PlanContribution struct {
	Selections         []Selection
	RequiredIAMActions []string
}

// Planner is implemented by adapters that translate project configuration into
// capture work. FinalizePlanning lets the adapter remove or report links that
// cannot be represented by the current set of supported services.
type Planner interface {
	Plan(config.Project, bool) PlanContribution
	FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error)
}

type Adapter interface {
	Planner
	Service() model.ServiceDescriptor
	Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error)
	Capture(context.Context, model.SourceScope, model.ResourceRef, model.CaptureOptions) (*model.Snapshot, error)
	Dependencies(*model.Snapshot) []model.Dependency
	Validate(*model.Snapshot, model.Capabilities) []model.Finding
}

type Registry struct{ adapters map[string]Adapter }

func New(adapters ...Adapter) (*Registry, error) {
	r := &Registry{adapters: map[string]Adapter{}}
	for _, adapter := range adapters {
		name := adapter.Service().Name
		if name == "" {
			return nil, fmt.Errorf("adapter service name is empty")
		}
		if _, exists := r.adapters[name]; exists {
			return nil, fmt.Errorf("duplicate adapter %q", name)
		}
		r.adapters[name] = adapter
	}
	return r, nil
}
func (r *Registry) Get(name string) (Adapter, bool) { a, ok := r.adapters[name]; return a, ok }
func (r *Registry) All() []Adapter {
	names := make([]string, 0, len(r.adapters))
	for name := range r.adapters {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]Adapter, 0, len(names))
	for _, name := range names {
		out = append(out, r.adapters[name])
	}
	return out
}
