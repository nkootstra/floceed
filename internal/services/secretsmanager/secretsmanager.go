// Package secretsmanager captures secret metadata and never reads secret values.
package secretsmanager

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsSecrets "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type Client interface {
	DescribeSecret(context.Context, *awsSecrets.DescribeSecretInput, ...func(*awsSecrets.Options)) (*awsSecrets.DescribeSecretOutput, error)
}
type Adapter struct{ client Client }

func New(client ...Client) *Adapter {
	var c Client
	if len(client) > 0 {
		c = client[0]
	}
	return &Adapter{client: c}
}
func (*Adapter) Service() model.ServiceDescriptor {
	return model.ServiceDescriptor{Name: "secretsmanager", DisplayName: "Secrets Manager", Support: model.SupportStructureOnly}
}
func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	out := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.Secrets))}
	for _, r := range project.Resources.Secrets {
		out.Selections = append(out.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "secretsmanager", Type: "secret", ID: r.Name, ARN: r.ARN}})
	}
	return out
}
func (*Adapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}
func (*Adapter) Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error) {
	return model.DiscoveryResult{}, nil
}
func (*Adapter) Dependencies(*model.Snapshot) []model.Dependency              { return nil }
func (*Adapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }
func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if opts.IncludeData {
		return nil, model.ErrValidation
	}
	structure := map[string]any{"name": ref.ID, "arn": ref.ARN, "value_captured": false}
	if a.client != nil {
		out, err := a.client.DescribeSecret(ctx, &awsSecrets.DescribeSecretInput{SecretId: aws.String(ref.ID)})
		if err != nil {
			return nil, err
		}
		structure["description"] = aws.ToString(out.Description)
		structure["kms_key_id"] = aws.ToString(out.KmsKeyId)
		structure["rotation_enabled"] = out.RotationEnabled
		structure["last_changed_date"] = out.LastChangedDate
	}
	return model.NewSnapshot(ref, "secretsmanager", structure)
}
