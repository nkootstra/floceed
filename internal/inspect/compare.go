package inspect

import (
	"reflect"
	"sort"
)

// Compare returns a deterministic, disclosure-safe semantic receipt. It uses
// only the canonical projection's identities and component digests.
func Compare(baseline, current Projection) Receipt {
	receipt := Receipt{
		SchemaVersion: ReceiptSchemaVersion,
		Baseline:      baseline.Digest,
		Current:       current.Digest,
		Categories:    changedBundleCategories(baseline, current),
	}
	base := indexResources(baseline.Resources)
	next := indexResources(current.Resources)
	keys := make(map[string]ResourceIdentity, len(base)+len(next))
	for key, resource := range base {
		keys[key] = resource.Identity
	}
	for key, resource := range next {
		keys[key] = resource.Identity
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)

	sourceChanged := baseline.Source != current.Source
	targetChanged := baseline.Target != current.Target
	for _, key := range ordered {
		before, existed := base[key]
		after, exists := next[key]
		change := ResourceChange{Resource: keys[key]}
		switch {
		case !existed:
			change.Outcome = OutcomeAdded
			receipt.Counts.Added++
		case !exists:
			change.Outcome = OutcomeRemoved
			receipt.Counts.Removed++
		default:
			change.Categories = changedCategories(before, after, sourceChanged, targetChanged)
			if len(change.Categories) == 0 {
				change.Outcome = OutcomeUnchanged
				receipt.Counts.Unchanged++
			} else {
				change.Outcome = OutcomeChanged
				receipt.Counts.Changed++
			}
		}
		receipt.Resources = append(receipt.Resources, change)
	}
	return receipt
}

func changedBundleCategories(before, after Projection) []ChangeCategory {
	var categories []ChangeCategory
	if !reflect.DeepEqual(before.Governance, after.Governance) {
		categories = append(categories, CategoryGovernance)
	}
	if !reflect.DeepEqual(before.Operations, after.Operations) {
		categories = append(categories, CategoryOperations)
	}
	if !reflect.DeepEqual(before.Findings, after.Findings) {
		categories = append(categories, CategoryFindings)
	}
	if before.Source != after.Source {
		categories = append(categories, CategorySource)
	}
	if before.Target != after.Target {
		categories = append(categories, CategoryTarget)
	}
	return categories
}

func indexResources(resources []ProjectedResource) map[string]ProjectedResource {
	indexed := make(map[string]ProjectedResource, len(resources))
	for _, resource := range resources {
		indexed[identityKey(resource.Identity)] = resource
	}
	return indexed
}

func identityKey(identity ResourceIdentity) string {
	return identity.Service + "\x00" + identity.Type + "\x00" + identity.ID
}

func changedCategories(before, after ProjectedResource, sourceChanged, targetChanged bool) []ChangeCategory {
	var categories []ChangeCategory
	if before.StructureDigest != after.StructureDigest {
		categories = append(categories, CategoryStructure)
	}
	if before.DatasetDigest != after.DatasetDigest {
		categories = append(categories, CategoryDataset)
	}
	if before.GovernanceDigest != after.GovernanceDigest {
		categories = append(categories, CategoryGovernance)
	}
	if before.OperationsDigest != after.OperationsDigest {
		categories = append(categories, CategoryOperations)
	}
	if before.FindingsDigest != after.FindingsDigest {
		categories = append(categories, CategoryFindings)
	}
	if before.Selected != after.Selected {
		categories = append(categories, CategorySelection)
	}
	if sourceChanged {
		categories = append(categories, CategorySource)
	}
	if targetChanged {
		categories = append(categories, CategoryTarget)
	}
	return categories
}
