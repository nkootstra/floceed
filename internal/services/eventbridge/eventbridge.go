// Package eventbridge captures EventBridge bus topology without event history.
package eventbridge

import (
	"context"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsEvents "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/services/structureonly"
)

type Client interface {
	ListRules(context.Context, *awsEvents.ListRulesInput, ...func(*awsEvents.Options)) (*awsEvents.ListRulesOutput, error)
	ListTargetsByRule(context.Context, *awsEvents.ListTargetsByRuleInput, ...func(*awsEvents.Options)) (*awsEvents.ListTargetsByRuleOutput, error)
}

type Adapter struct {
	structureonly.Base
	client Client
}

type eventBusStructure struct {
	Name  string      `json:"name"`
	ARN   string      `json:"arn"`
	Rules []eventRule `json:"rules"`
}

type eventRule struct {
	Name         string        `json:"name"`
	ARN          string        `json:"arn"`
	State        string        `json:"state"`
	EventPattern string        `json:"event_pattern"`
	Description  string        `json:"description"`
	Targets      []eventTarget `json:"targets"`
}

type eventTarget struct {
	ID      string `json:"id"`
	ARN     string `json:"arn"`
	RoleARN string `json:"role_arn"`
}

var _ catalog.Adapter = (*Adapter)(nil)

func New(c Client) *Adapter {
	return &Adapter{
		Base: structureonly.New(structureonly.Descriptor{
			ServiceName:  "events",
			DisplayName:  "EventBridge",
			ResourceType: "event_bus",
			IAMActions:   []string{"events:ListRules", "events:ListTargetsByRule"},
			Resources: func(project config.Project) []structureonly.Named {
				return structureonly.Select(project.Resources.EventBridge, func(r config.EventBridgeResource) (string, string) { return r.Name, r.ARN })
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
		return nil, fmt.Errorf("EventBridge capture requires a client: %w", model.ErrValidation)
	}
	structure := eventBusStructure{Name: ref.ID, ARN: ref.ARN, Rules: make([]eventRule, 0)}
	{
		rules := make([]eventRule, 0)
		var token *string
		for {
			out, err := a.client.ListRules(ctx, &awsEvents.ListRulesInput{EventBusName: aws.String(ref.ARN), NextToken: token})
			if err != nil {
				return nil, err
			}
			for _, rule := range out.Rules {
				entry := eventRule{Name: aws.ToString(rule.Name), ARN: aws.ToString(rule.Arn), State: string(rule.State), EventPattern: aws.ToString(rule.EventPattern), Description: aws.ToString(rule.Description), Targets: make([]eventTarget, 0)}
				if rule.Name != nil {
					var targetToken *string
					for {
						targetsOut, err := a.client.ListTargetsByRule(ctx, &awsEvents.ListTargetsByRuleInput{EventBusName: aws.String(ref.ARN), Rule: rule.Name, NextToken: targetToken})
						if err != nil {
							return nil, err
						}
						for _, target := range targetsOut.Targets {
							entry.Targets = append(entry.Targets, eventTarget{ID: aws.ToString(target.Id), ARN: aws.ToString(target.Arn), RoleARN: aws.ToString(target.RoleArn)})
						}
						if targetsOut.NextToken == nil || *targetsOut.NextToken == "" {
							break
						}
						targetToken = targetsOut.NextToken
					}
				}
				sort.Slice(entry.Targets, func(i, j int) bool { return entry.Targets[i].ID < entry.Targets[j].ID })
				rules = append(rules, entry)
			}
			if out.NextToken == nil || *out.NextToken == "" {
				break
			}
			token = out.NextToken
		}
		sort.Slice(rules, func(i, j int) bool { return rules[i].Name < rules[j].Name })
		structure.Rules = rules
	}
	return a.Snapshot(ref, structure)
}
