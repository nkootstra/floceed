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
)

type Client interface {
	ListRules(context.Context, *awsEvents.ListRulesInput, ...func(*awsEvents.Options)) (*awsEvents.ListRulesOutput, error)
	ListTargetsByRule(context.Context, *awsEvents.ListTargetsByRuleInput, ...func(*awsEvents.Options)) (*awsEvents.ListTargetsByRuleOutput, error)
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
	return model.ServiceDescriptor{Name: "events", DisplayName: "EventBridge", Support: model.SupportStructureOnly}
}

func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	out := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.EventBridge))}
	for _, resource := range project.Resources.EventBridge {
		out.Selections = append(out.Selections, catalog.Selection{Resource: model.ResourceRef{Service: "events", Type: "event_bus", ID: resource.Name, ARN: resource.ARN}})
		out.RequiredIAMActions = append(out.RequiredIAMActions, "events:ListRules", "events:ListTargetsByRule")
	}
	return out
}

func (*Adapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}
func (*Adapter) Dependencies(*model.Snapshot) []model.Dependency              { return nil }
func (*Adapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }
func (*Adapter) Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error) {
	return model.DiscoveryResult{}, nil
}

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if opts.IncludeData {
		return nil, model.ErrValidation
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
	return model.NewSnapshot(ref, "events", structure)
}
