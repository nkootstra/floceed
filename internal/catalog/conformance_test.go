package catalog_test

import (
	"testing"

	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	ddb "github.com/nkootstra/floceed/internal/services/dynamodb"
	events "github.com/nkootstra/floceed/internal/services/eventbridge"
	"github.com/nkootstra/floceed/internal/services/kinesis"
	lambda "github.com/nkootstra/floceed/internal/services/lambda"
	"github.com/nkootstra/floceed/internal/services/s3"
	secrets "github.com/nkootstra/floceed/internal/services/secretsmanager"
	"github.com/nkootstra/floceed/internal/services/sns"
	"github.com/nkootstra/floceed/internal/services/sqs"
	ssm "github.com/nkootstra/floceed/internal/services/ssm"
)

func TestAdaptersConformToStableContracts(t *testing.T) {
	registry, err := catalog.New(
		s3.New(nil), ddb.New(nil), kinesis.New(), sqs.New(), sns.New(), events.New(), lambda.New(), secrets.New(), ssm.New(),
	)
	if err != nil {
		t.Fatal(err)
	}
	adapters := registry.All()
	if len(adapters) != 9 {
		t.Fatalf("adapter count = %d", len(adapters))
	}
	for _, adapter := range adapters {
		descriptor := adapter.Service()
		if descriptor.Name == "" || descriptor.DisplayName == "" {
			t.Fatalf("invalid descriptor: %#v", descriptor)
		}
		if got := adapter.Plan(config.Project{}, false); len(got.Selections) != 0 || len(got.RequiredIAMActions) != 0 {
			t.Fatalf("empty project plan for %s = %#v", descriptor.Name, got)
		}
	}
}

func TestRegistryRejectsDuplicateServices(t *testing.T) {
	if _, err := catalog.New(sqs.New(), sqs.New()); err == nil {
		t.Fatal("expected duplicate service error")
	}
}
