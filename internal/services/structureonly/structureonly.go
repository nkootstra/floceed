// Package structureonly provides an embedded base for the structure-only
// service adapters. These adapters capture topology metadata and never allow
// data capture, so their Service, Plan, FinalizePlanning, Discover,
// Dependencies, and Validate methods are identical apart from per-service
// facts declared in Descriptor.
package structureonly

import (
	"context"
	"fmt"

	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

// Descriptor declares the per-service facts a structure-only adapter provides.
type Descriptor struct {
	ServiceName  string
	DisplayName  string
	ResourceType string
	// Resources returns the explicitly selected resources from the project.
	Resources func(config.Project) []Named
	// IAMActions lists the read actions Capture needs, emitted once per plan.
	IAMActions []string
}

// Named is an explicitly selected resource with an optional ARN.
type Named struct {
	Name string
	ARN  string
}

// Select maps a slice of project resources onto Named via fields.
func Select[T any](resources []T, fields func(T) (name, arn string)) []Named {
	out := make([]Named, len(resources))
	for i, r := range resources {
		out[i].Name, out[i].ARN = fields(r)
	}
	return out
}

// Base implements the catalog.Adapter boilerplate shared by structure-only
// services. Adapters embed Base and add their own client and Capture method.
type Base struct {
	descriptor Descriptor
}

// New builds a Base from the per-service descriptor.
func New(descriptor Descriptor) Base {
	return Base{descriptor: descriptor}
}

func (b Base) Service() model.ServiceDescriptor {
	return model.ServiceDescriptor{Name: b.descriptor.ServiceName, DisplayName: b.descriptor.DisplayName, Support: model.SupportStructureOnly}
}

func (b Base) Plan(project config.Project, _ bool) catalog.PlanContribution {
	named := b.descriptor.Resources(project)
	out := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(named))}
	for _, resource := range named {
		out.Selections = append(out.Selections, catalog.Selection{Resource: model.ResourceRef{Service: b.descriptor.ServiceName, Type: b.descriptor.ResourceType, ID: resource.Name, ARN: resource.ARN}})
	}
	if len(named) != 0 {
		out.RequiredIAMActions = append([]string(nil), b.descriptor.IAMActions...)
	}
	return out
}

func (Base) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}

func (Base) Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error) {
	return model.DiscoveryResult{}, nil
}

func (Base) Dependencies(*model.Snapshot) []model.Dependency              { return nil }
func (Base) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }

// CheckStructureOnly enforces the structure-only contract. It returns
// ErrValidation when data capture is requested.
func (b Base) CheckStructureOnly(opts model.CaptureOptions) error {
	if opts.IncludeData {
		return fmt.Errorf("%s data capture is unsupported: %w", b.descriptor.ServiceName, model.ErrValidation)
	}
	return nil
}

// Snapshot builds a snapshot for the descriptor's service.
func (b Base) Snapshot(ref model.ResourceRef, structure any) (*model.Snapshot, error) {
	return model.NewSnapshot(ref, b.descriptor.ServiceName, structure)
}
