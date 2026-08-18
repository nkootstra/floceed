package app

import (
	"github.com/nkootstra/floceed/internal/model"
	"testing"
)

func TestValidateDependencyGraphRejectsRequiredCycles(t *testing.T) {
	a := model.ResourceRef{Service: "events", Type: "event_bus", ID: "a", ARN: "arn:aws:events:eu-west-1:123456789012:event-bus/a"}
	b := model.ResourceRef{Service: "events", Type: "event_bus", ID: "b", ARN: "arn:aws:events:eu-west-1:123456789012:event-bus/b"}
	snapshots := []model.Snapshot{{Resource: a}, {Resource: b}}
	selected := map[string]model.ResourceRef{resourceIdentityKey("events", "event_bus", "a"): a, resourceIdentityKey("events", "event_bus", "b"): b}
	deps := [][]model.Dependency{{{From: a, To: b, Required: true}}, {{From: b, To: a, Required: true}}}
	if err := validateDependencyGraph(deps, snapshots, selected); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestValidateDependencyGraphIgnoresOptionalCycles(t *testing.T) {
	a := model.ResourceRef{Service: "events", Type: "event_bus", ID: "a", ARN: "arn:aws:events:eu-west-1:123456789012:event-bus/a"}
	b := model.ResourceRef{Service: "events", Type: "event_bus", ID: "b", ARN: "arn:aws:events:eu-west-1:123456789012:event-bus/b"}
	snapshots := []model.Snapshot{{Resource: a}, {Resource: b}}
	selected := map[string]model.ResourceRef{resourceIdentityKey("events", "event_bus", "a"): a, resourceIdentityKey("events", "event_bus", "b"): b}
	deps := [][]model.Dependency{{{From: a, To: b}}, {{From: b, To: a}}}
	if err := validateDependencyGraph(deps, snapshots, selected); err != nil {
		t.Fatal(err)
	}
}
