// Package sqs implements metadata-only capture of explicitly selected queues.
package sqs

import (
	"context"

	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type Adapter struct{}

func New() *Adapter { return &Adapter{} }

func (*Adapter) Service() model.ServiceDescriptor {
	return model.ServiceDescriptor{Name: "sqs", DisplayName: "SQS", Support: model.SupportStructureOnly}
}

func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	contribution := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.SQS))}
	for _, resource := range project.Resources.SQS {
		contribution.Selections = append(contribution.Selections, catalog.Selection{
			Resource: model.ResourceRef{Service: "sqs", Type: "queue", ID: resource.Name, ARN: resource.ARN},
		})
	}
	return contribution
}

func (*Adapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}

func (*Adapter) Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error) {
	return model.DiscoveryResult{}, nil
}

func (*Adapter) Capture(_ context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if opts.IncludeData {
		return nil, model.ErrValidation
	}
	return model.NewSnapshot(ref, "sqs", map[string]string{"name": ref.ID, "arn": ref.ARN})
}

func (*Adapter) Dependencies(*model.Snapshot) []model.Dependency { return nil }

func (*Adapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }
