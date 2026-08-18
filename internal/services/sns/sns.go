// Package sns implements metadata-only capture of explicitly selected topics.
package sns

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSNS "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type Client interface {
	ListSubscriptionsByTopic(context.Context, *awsSNS.ListSubscriptionsByTopicInput, ...func(*awsSNS.Options)) (*awsSNS.ListSubscriptionsByTopicOutput, error)
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
	return model.ServiceDescriptor{Name: "sns", DisplayName: "SNS", Support: model.SupportStructureOnly}
}

func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	contribution := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.SNS))}
	for _, resource := range project.Resources.SNS {
		contribution.Selections = append(contribution.Selections, catalog.Selection{
			Resource: model.ResourceRef{Service: "sns", Type: "topic", ID: resource.Name, ARN: resource.ARN},
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

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if opts.IncludeData {
		return nil, model.ErrValidation
	}
	structure := map[string]any{"name": ref.ID, "arn": ref.ARN}
	if a.client != nil {
		var subscriptions []map[string]string
		var token *string
		for {
			out, err := a.client.ListSubscriptionsByTopic(ctx, &awsSNS.ListSubscriptionsByTopicInput{TopicArn: &ref.ARN, NextToken: token})
			if err != nil {
				return nil, err
			}
			for _, subscription := range out.Subscriptions {
				subscriptions = append(subscriptions, map[string]string{"arn": aws.ToString(subscription.SubscriptionArn), "protocol": aws.ToString(subscription.Protocol), "endpoint": aws.ToString(subscription.Endpoint), "topic_arn": aws.ToString(subscription.TopicArn)})
			}
			if out.NextToken == nil || *out.NextToken == "" {
				break
			}
			token = out.NextToken
		}
		structure["subscriptions"] = subscriptions
	}
	return model.NewSnapshot(ref, "sns", structure)
}

func (*Adapter) Dependencies(*model.Snapshot) []model.Dependency { return nil }

func (*Adapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }
