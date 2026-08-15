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
			}
			p.Resources.S3 = append(p.Resources.S3, entry)
		case "dynamodb":
			entry := config.DynamoDBResource{Name: r.Ref.ID}
			if data {
				entry.Data = config.NewDynamoDBDataPolicy()
			}
			p.Resources.DynamoDB = append(p.Resources.DynamoDB, entry)
		}
	}
	return p
}

func (m Model) request() ProjectRequest {
	return ProjectRequest{Project: m.Project(), ProjectFile: m.opts.ProjectFile}
}
