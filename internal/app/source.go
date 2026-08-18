package app

import (
	"cmp"
	"context"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/model"
	apigatewayservice "github.com/nkootstra/floceed/internal/services/apigateway"
	cloudwatchlogsservice "github.com/nkootstra/floceed/internal/services/cloudwatchlogs"
	ddbservice "github.com/nkootstra/floceed/internal/services/dynamodb"
	eventbridgeservice "github.com/nkootstra/floceed/internal/services/eventbridge"
	kinesisservice "github.com/nkootstra/floceed/internal/services/kinesis"
	lambdaservice "github.com/nkootstra/floceed/internal/services/lambda"
	s3service "github.com/nkootstra/floceed/internal/services/s3"
	secretsservice "github.com/nkootstra/floceed/internal/services/secretsmanager"
	snsservice "github.com/nkootstra/floceed/internal/services/sns"
	sqsservice "github.com/nkootstra/floceed/internal/services/sqs"
	ssmservice "github.com/nkootstra/floceed/internal/services/ssm"
	stepfunctionsservice "github.com/nkootstra/floceed/internal/services/stepfunctions"
)

type Source struct {
	Scope    model.SourceScope
	Identity awsconfig.Identity
	Registry *catalog.Registry
}

type SourceRequest struct {
	Profile       string
	Region        string
	S3Names       []string
	DynamoDBNames []string
}

type SourceFactory interface {
	Open(context.Context, SourceRequest) (Source, error)
}

type AWSFactory struct{}

func (AWSFactory) Open(ctx context.Context, req SourceRequest) (Source, error) {
	cfg, err := awsconfig.Load(ctx, req.Profile, req.Region)
	if err != nil {
		return Source{}, err
	}
	identity, err := awsconfig.CallerIdentity(ctx, sts.NewFromConfig(cfg), req.Profile)
	if err != nil {
		return Source{}, err
	}
	s3client := s3.NewFromConfig(cfg)
	ddbclient := dynamodb.NewFromConfig(cfg)
	kinesisClient := kinesis.NewFromConfig(cfg)
	sqsClient := sqs.NewFromConfig(cfg)
	snsClient := sns.NewFromConfig(cfg)
	eventsClient := eventbridge.NewFromConfig(cfg)
	lambdaClient := lambda.NewFromConfig(cfg)
	secretsClient := secretsmanager.NewFromConfig(cfg)
	ssmClient := ssm.NewFromConfig(cfg)
	apiClient := apigatewayv2.NewFromConfig(cfg)
	stepFunctionsClient := sfn.NewFromConfig(cfg)
	logsClient := cloudwatchlogs.NewFromConfig(cfg)
	registry, err := newAWSRegistry(s3client, ddbclient, kinesisClient, sqsClient, snsClient, eventsClient, lambdaClient, secretsClient, ssmClient, apiClient, stepFunctionsClient, logsClient, req.S3Names)
	if err != nil {
		return Source{}, err
	}
	return Source{Scope: model.SourceScope{Profile: req.Profile, AccountID: identity.AccountID, Region: req.Region}, Identity: identity, Registry: registry}, nil
}

func newAWSRegistry(s3client s3service.Client, ddbclient ddbservice.Client, kinesisClient kinesisservice.Client, sqsClient sqsservice.Client, snsClient snsservice.Client, eventsClient *eventbridge.Client, lambdaClient *lambda.Client, secretsClient *secretsmanager.Client, ssmClient *ssm.Client, apiClient *apigatewayv2.Client, stepFunctionsClient *sfn.Client, logsClient *cloudwatchlogs.Client, s3Names []string) (*catalog.Registry, error) {
	return catalog.New(s3service.NewFiltered(s3client, s3Names), ddbservice.New(ddbclient), sqsservice.New(sqsClient), snsservice.New(snsClient), kinesisservice.New(kinesisClient), eventbridgeservice.New(eventsClient), lambdaservice.New(lambdaClient), secretsservice.New(secretsClient), ssmservice.New(ssmClient), apigatewayservice.New(apiClient), stepfunctionsservice.New(stepFunctionsClient), cloudwatchlogsservice.New(logsClient))
}

// Identity resolves the standard AWS configuration chain and confirms the
// caller without constructing service adapters or discovering resources.
func (a *Application) Identity(ctx context.Context, profile, region string) (awsconfig.Identity, error) {
	cfg, err := awsconfig.Load(ctx, profile, region)
	if err != nil {
		return awsconfig.Identity{}, sourceError(err)
	}
	identity, err := awsconfig.CallerIdentity(ctx, sts.NewFromConfig(cfg), profile)
	if err != nil {
		return awsconfig.Identity{}, sourceError(err)
	}
	return identity, nil
}

type ScanRequest struct {
	Profile, Region        string
	S3Names, DynamoDBNames []string
	Services               []string
}

type ScanResult struct {
	Identity  awsconfig.Identity      `json:"identity"`
	Resources []model.ResourceSummary `json:"resources"`
	Findings  []model.Finding         `json:"findings,omitempty"`
}

func (a *Application) Scan(ctx context.Context, req ScanRequest) (ScanResult, error) {
	source, err := a.Factory.Open(ctx, SourceRequest{
		Profile:       req.Profile,
		Region:        req.Region,
		S3Names:       req.S3Names,
		DynamoDBNames: req.DynamoDBNames,
	})
	if err != nil {
		return ScanResult{}, sourceError(err)
	}
	result := ScanResult{Identity: source.Identity}
	type discovery struct {
		service string
		result  model.DiscoveryResult
		err     error
	}
	adapters := source.Registry.All()
	if req.Services != nil {
		selected := make(map[string]struct{}, len(req.Services))
		for _, service := range req.Services {
			selected[service] = struct{}{}
		}
		filtered := adapters[:0]
		for _, adapter := range adapters {
			if _, ok := selected[adapter.Service().Name]; ok {
				filtered = append(filtered, adapter)
			}
		}
		adapters = filtered
	}
	discoveries := make([]discovery, len(adapters))
	var workers sync.WaitGroup
	workers.Add(len(adapters))
	for i, adapter := range adapters {
		discoveries[i].service = adapter.Service().Name
		go func() {
			defer workers.Done()
			discoveries[i].result, discoveries[i].err = adapter.Discover(ctx, source.Scope)
		}()
	}
	workers.Wait()
	for _, discovery := range discoveries {
		if discovery.err != nil {
			result.Findings = append(result.Findings, model.Finding{Code: "SERVICE_DISCOVERY_FAILED", Severity: model.SeverityWarning, Support: model.SupportPartial, Resource: discovery.service, Message: discovery.err.Error()})
			continue
		}
		result.Resources = append(result.Resources, discovery.result.Resources...)
		result.Findings = append(result.Findings, discovery.result.Findings...)
		for _, r := range discovery.result.Resources {
			result.Findings = append(result.Findings, r.Findings...)
		}
	}
	sort.Slice(result.Resources, func(i, j int) bool {
		left, right := result.Resources[i].Ref, result.Resources[j].Ref
		return cmp.Or(cmp.Compare(left.Service, right.Service), cmp.Compare(left.ID, right.ID)) < 0
	})
	sortFindings(result.Findings)
	return result, nil
}
