// Package eventbridge captures EventBridge bus topology without event history.
package eventbridge

import (
	"context"
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

var _ catalog.Adapter = (*Adapter)(nil)

func New(client ...Client) *Adapter {
	var c Client
	if len(client) > 0 {
		c = client[0]
	}
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
	structure := map[string]any{"name": ref.ID, "arn": ref.ARN, "rules": []any{}}
	if a.client != nil {
		var rules []map[string]any
		var token *string
		for {
			out, err := a.client.ListRules(ctx, &awsEvents.ListRulesInput{EventBusName: aws.String(ref.ARN), NextToken: token})
			if err != nil {
				return nil, err
			}
			for _, rule := range out.Rules {
				entry := map[string]any{"name": aws.ToString(rule.Name), "arn": aws.ToString(rule.Arn), "state": string(rule.State), "event_pattern": aws.ToString(rule.EventPattern), "description": aws.ToString(rule.Description)}
				var targets []map[string]string
				if rule.Name != nil {
					var targetToken *string
					for {
						targetsOut, err := a.client.ListTargetsByRule(ctx, &awsEvents.ListTargetsByRuleInput{EventBusName: aws.String(ref.ARN), Rule: rule.Name, NextToken: targetToken})
						if err != nil {
							return nil, err
						}
						for _, target := range targetsOut.Targets {
							targets = append(targets, map[string]string{"id": aws.ToString(target.Id), "arn": aws.ToString(target.Arn), "role_arn": aws.ToString(target.RoleArn)})
						}
						if targetsOut.NextToken == nil || *targetsOut.NextToken == "" {
							break
						}
						targetToken = targetsOut.NextToken
					}
				}
				sort.Slice(targets, func(i, j int) bool { return targets[i]["id"] < targets[j]["id"] })
				entry["targets"] = targets
				rules = append(rules, entry)
			}
			if out.NextToken == nil || *out.NextToken == "" {
				break
			}
			token = out.NextToken
		}
		sort.Slice(rules, func(i, j int) bool { return rules[i]["name"].(string) < rules[j]["name"].(string) })
		structure["rules"] = rules
	}
	return a.Snapshot(ref, structure)
}
