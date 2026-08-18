package tui

import (
	"cmp"
	"sort"
	"strings"

	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

func (m *Model) mergeResources(in []model.ResourceSummary) {
	byKey := map[string]model.ResourceSummary{}
	for _, r := range m.resources {
		if m.serviceSelected[r.Ref.Service] {
			byKey[resourceKey(r.Ref)] = r
		}
	}
	for _, r := range in {
		if m.serviceSelected[r.Ref.Service] {
			byKey[resourceKey(r.Ref)] = r
		}
	}
	for key := range m.selected {
		service, _, _ := strings.Cut(key, "/")
		if !m.serviceSelected[service] {
			delete(m.selected, key)
		}
	}
	for key := range m.dataEnabled {
		service, _, _ := strings.Cut(key, "/")
		if !m.serviceSelected[service] {
			delete(m.dataEnabled, key)
			delete(m.dataMode, key)
		}
	}
	m.resources = m.resources[:0]
	for _, r := range byKey {
		m.resources = append(m.resources, r)
	}
	sort.Slice(m.resources, func(i, j int) bool {
		left, right := m.resources[i].Ref, m.resources[j].Ref
		return cmp.Or(cmp.Compare(left.Service, right.Service), cmp.Compare(left.ID, right.ID)) < 0
	})
}

func (m Model) visibleResources() []model.ResourceSummary {
	q := strings.ToLower(strings.TrimSpace(m.filter.Value()))
	if q == "" {
		return m.resources
	}
	out := make([]model.ResourceSummary, 0, len(m.resources))
	for _, r := range m.resources {
		if strings.Contains(strings.ToLower(r.Name), q) || strings.Contains(strings.ToLower(r.Ref.Service), q) {
			out = append(out, r)
		}
	}
	return out
}

func (m Model) selectedResources() []model.ResourceSummary {
	var out []model.ResourceSummary
	for _, r := range m.resources {
		if m.selected[resourceKey(r.Ref)] {
			out = append(out, r)
		}
	}
	return out
}

func resourceKey(ref model.ResourceRef) string { return ref.Service + "/" + ref.ID }

func (m Model) Project() config.Project {
	p := config.NewProject()
	p.Source = config.Source{Profile: m.profile, Region: m.region, ExpectedAccountID: m.identity.AccountID}
	for _, r := range m.selectedResources() {
		data := m.dataEnabled[resourceKey(r.Ref)]
		switch r.Ref.Service {
		case "s3":
			entry := config.S3Resource{Name: r.Ref.ID}
			if data {
				entry.Data = config.NewS3DataPolicy()
				entry.Data.Mode = m.dataMode[resourceKey(r.Ref)]
				if entry.Data.Mode == config.DataModeFull {
					entry.Data.MaxObjects = 0
					entry.Data.MaxObjectBytes = 0
					entry.Data.MaxTotalBytes = 0
					// Floor the replay timeout for full mode, but never downgrade an
					// explicit larger value already present in the loaded project.
					if p.Target.HookTimeoutSeconds <= config.DefaultHookTimeoutSeconds {
						p.Target.HookTimeoutSeconds = 3600
					}
				}
			}
			p.Resources.S3 = append(p.Resources.S3, entry)
		case "dynamodb":
			entry := config.DynamoDBResource{Name: r.Ref.ID}
			if data {
				entry.Data = config.NewDynamoDBDataPolicy()
				entry.Data.Mode = m.dataMode[resourceKey(r.Ref)]
				if entry.Data.Mode == config.DataModeFull {
					entry.Data.MaxItems = 0
					entry.Data.MaxPages = 0
					if p.Target.HookTimeoutSeconds <= config.DefaultHookTimeoutSeconds {
						p.Target.HookTimeoutSeconds = 3600
					}
				}
			}
			p.Resources.DynamoDB = append(p.Resources.DynamoDB, entry)
		case "kinesis":
			p.Resources.Kinesis = append(p.Resources.Kinesis, config.KinesisResource{Name: r.Ref.ID, ARN: r.Ref.ARN})
		case "events":
			p.Resources.EventBridge = append(p.Resources.EventBridge, config.EventBridgeResource{Name: r.Ref.ID, ARN: r.Ref.ARN})
		case "lambda":
			p.Resources.Lambda = append(p.Resources.Lambda, config.LambdaResource{Name: r.Ref.ID, ARN: r.Ref.ARN})
		case "secretsmanager":
			p.Resources.Secrets = append(p.Resources.Secrets, config.SecretResource{Name: r.Ref.ID, ARN: r.Ref.ARN})
		case "ssm":
			p.Resources.Parameters = append(p.Resources.Parameters, config.ParameterResource{Name: r.Ref.ID, ARN: r.Ref.ARN})
		case "apigateway":
			p.Resources.APIs = append(p.Resources.APIs, config.APIResource{Name: r.Ref.ID, ARN: r.Ref.ARN})
		case "stepfunctions":
			p.Resources.StateMachines = append(p.Resources.StateMachines, config.StateMachineResource{Name: r.Ref.ID, ARN: r.Ref.ARN})
		case "logs":
			p.Resources.LogGroups = append(p.Resources.LogGroups, config.LogGroupResource{Name: r.Ref.ID, ARN: r.Ref.ARN})
		}
	}
	return p
}

func (m Model) request() ProjectRequest {
	return ProjectRequest{Project: m.Project(), ProjectFile: m.opts.ProjectFile, FixtureProfile: m.opts.FixtureProfile}
}
