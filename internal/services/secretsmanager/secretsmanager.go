// Package secretsmanager captures secret metadata and never reads secret values.
package secretsmanager

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSecrets "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/services/structureonly"
)

type Client interface {
	DescribeSecret(context.Context, *awsSecrets.DescribeSecretInput, ...func(*awsSecrets.Options)) (*awsSecrets.DescribeSecretOutput, error)
}

type Adapter struct {
	structureonly.Base
	client Client
}

var _ catalog.Adapter = (*Adapter)(nil)

func New(client ...Client) *Adapter {
	var c Client
	if len(client) > 0 {
		c = client[0]
	}
	return &Adapter{
		Base: structureonly.New(structureonly.Descriptor{
			ServiceName:  "secretsmanager",
			DisplayName:  "Secrets Manager",
			ResourceType: "secret",
			IAMActions:   []string{"secretsmanager:DescribeSecret"},
			Resources: func(project config.Project) []structureonly.Named {
				return structureonly.Select(project.Resources.Secrets, func(r config.SecretResource) (string, string) { return r.Name, r.ARN })
			},
		}),
		client: c,
	}
}

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if err := a.Base.CheckStructureOnly(opts); err != nil {
		return nil, err
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
	return a.Base.Snapshot(ref, structure)
}
