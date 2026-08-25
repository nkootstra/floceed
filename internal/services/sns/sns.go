// Package sns implements metadata-only capture of explicitly selected topics.
package sns

import (
	"context"
	"fmt"

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

type topicStructure struct {
	Name          string          `json:"name"`
	ARN           string          `json:"arn"`
	Subscriptions *[]subscription `json:"subscriptions,omitempty"`
}

type subscription struct {
	ARN      string `json:"arn"`
	Protocol string `json:"protocol"`
	Endpoint string `json:"endpoint"`
	TopicARN string `json:"topic_arn"`
}

var _ catalog.Adapter = (*Adapter)(nil)

func New(c Client) *Adapter {
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
	if a.client == nil {
		return nil, fmt.Errorf("SNS capture requires a client: %w", model.ErrValidation)
	}
	structure := topicStructure{Name: ref.ID, ARN: ref.ARN}
	{
		subscriptions := make([]subscription, 0)
		var token *string
		for {
			out, err := a.client.ListSubscriptionsByTopic(ctx, &awsSNS.ListSubscriptionsByTopicInput{TopicArn: &ref.ARN, NextToken: token})
			if err != nil {
				return nil, err
			}
			for _, item := range out.Subscriptions {
				subscriptions = append(subscriptions, subscription{ARN: aws.ToString(item.SubscriptionArn), Protocol: aws.ToString(item.Protocol), Endpoint: aws.ToString(item.Endpoint), TopicARN: aws.ToString(item.TopicArn)})
			}
			if out.NextToken == nil || *out.NextToken == "" {
				break
			}
			token = out.NextToken
		}
		structure.Subscriptions = &subscriptions
	}
	return a.Snapshot(ref, structure)
}
