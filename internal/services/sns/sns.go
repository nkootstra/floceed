// Package sns implements metadata-only capture of explicitly selected topics.
package sns

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsSNS "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/services/structureonly"
)

type Client interface {
	ListSubscriptionsByTopic(context.Context, *awsSNS.ListSubscriptionsByTopicInput, ...func(*awsSNS.Options)) (*awsSNS.ListSubscriptionsByTopicOutput, error)
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
			ServiceName:  "sns",
			DisplayName:  "SNS",
			ResourceType: "topic",
			IAMActions:   []string{"sns:ListSubscriptionsByTopic"},
			Resources: func(project config.Project) []structureonly.Named {
				return structureonly.Select(project.Resources.SNS, func(r config.SNSResource) (string, string) { return r.Name, r.ARN })
			},
		}),
		client: c,
	}
}

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if err := a.CheckStructureOnly(opts); err != nil {
		return nil, err
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
	return a.Snapshot(ref, structure)
}
