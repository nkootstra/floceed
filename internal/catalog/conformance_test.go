package catalog_test

import (
	"testing"

	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	apigateway "github.com/nkootstra/floceed/internal/services/apigateway"
	logs "github.com/nkootstra/floceed/internal/services/cloudwatchlogs"
	ddb "github.com/nkootstra/floceed/internal/services/dynamodb"
	events "github.com/nkootstra/floceed/internal/services/eventbridge"
	"github.com/nkootstra/floceed/internal/services/kinesis"
	lambda "github.com/nkootstra/floceed/internal/services/lambda"
	"github.com/nkootstra/floceed/internal/services/s3"
	secrets "github.com/nkootstra/floceed/internal/services/secretsmanager"
	"github.com/nkootstra/floceed/internal/services/sns"
	"github.com/nkootstra/floceed/internal/services/sqs"
	ssm "github.com/nkootstra/floceed/internal/services/ssm"
	stepfunctions "github.com/nkootstra/floceed/internal/services/stepfunctions"
)

func TestAdaptersConformToStableContracts(t *testing.T) {
	registry, err := catalog.New(
		s3.New(nil), ddb.New(nil), kinesis.New(), sqs.New(), sns.New(nil), events.New(nil), lambda.New(nil), secrets.New(nil), ssm.New(nil), apigateway.New(nil), stepfunctions.New(nil), logs.New(nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	adapters := registry.All()
	if len(adapters) != len(model.SupportedServiceFacts()) {
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
	facts := model.SupportedServiceFacts()
	for _, fact := range facts {
		adapter, ok := registry.Get(fact.Name)
		if !ok || adapter.Service().DisplayName != fact.DisplayName || adapter.Service().Support != fact.Support {
			t.Fatalf("adapter metadata for %s = %#v, want %#v", fact.Name, adapter, fact.ServiceDescriptor)
		}
	}
}

func TestRegistryRejectsDuplicateServices(t *testing.T) {
	if _, err := catalog.New(sqs.New(), sqs.New()); err == nil {
		t.Fatal("expected duplicate service error")
	}
}
