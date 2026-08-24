//go:build integration

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/nkootstra/floceed/internal/capabilities"
	"github.com/nkootstra/floceed/internal/testfixture"
)

const appUserName = "floceed-app-user"

const (
	deniedObjectKey = "fixtures/denied.txt"
	adminObjectKey  = "fixtures/admin-allowed.txt"
)

func TestIAMEnforcementDeniesActionsBeyondAppPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	bundleRoot := renderSyntheticBundle(t, ctx, replayFixture{
		SchemaVersion: 3,
		ObjectBody:    "enforced replay fixture\n",
		ItemID:        "enforced-fixture",
	})
	container := startFlociWithEnv(t, ctx, bundleRoot, t.TempDir(), map[string]string{
		"FLOCI_SERVICES_IAM_ENFORCEMENT_ENABLED": "true",
		"FLOCI_DEFAULT_ACCOUNT_ID":               accountID,
	})
	endpoint := endpointFor(t, ctx, container)

	waitForReady(t, ctx, container, endpoint)
	verifySnapshot(t, ctx, endpoint, "enforced replay fixture\n", "enforced-fixture")

	appCredentials := seedAppIdentity(t, ctx, endpoint)
	restricted := clientsWithCredentials(endpoint, appCredentials)
	assertAllowed(t, ctx, restricted)
	assertDenied(t, ctx, "dynamodb:Scan", func(ctx context.Context) error {
		_, err := restricted.ddb.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(table)})
		return err
	})
	assertDenied(t, ctx, "s3:PutObject", func(ctx context.Context) error {
		_, err := restricted.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(deniedObjectKey), Body: strings.NewReader("denied")})
		return err
	})

	noPolicyCredentials := seedUnprivilegedIdentity(t, ctx, endpoint)
	adminCredentials := seedWildcardAdmin(t, ctx, endpoint)
	admin := clientsWithCredentials(endpoint, adminCredentials)
	assertSupportedServicePermissions(t, ctx, endpoint, noPolicyCredentials, appCredentials)
	if _, err := admin.ddb.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(table)}); err != nil {
		t.Fatalf("wildcard policy must allow dynamodb:Scan under enforcement: %v", err)
	}
	if _, err := admin.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(adminObjectKey), Body: strings.NewReader("allowed")}); err != nil {
		t.Fatalf("wildcard policy must allow s3:PutObject under enforcement: %v", err)
	}
	assertObjectAbsent(t, ctx, admin, deniedObjectKey)
}

type localCredentials struct {
	accessKeyID     string
	secretAccessKey string
}

type permissionCall struct {
	service  string
	action   string
	knownGap string
	call     func(context.Context) error
}

func assertSupportedServicePermissions(t *testing.T, ctx context.Context, endpoint string, denied, granted localCredentials) {
	t.Helper()
	deniedCalls := supportedServicePermissionCalls(endpoint, denied)
	grantedCalls := supportedServicePermissionCalls(endpoint, granted)
	assertPermissionMatrixCoversSupportedServices(t, deniedCalls)
	assertPermissionMatrixMatchesAppPolicy(t, deniedCalls)
	if len(deniedCalls) != len(grantedCalls) {
		t.Fatalf("permission call matrix mismatch: denied=%d granted=%d", len(deniedCalls), len(grantedCalls))
	}
	for i, deniedCall := range deniedCalls {
		grantedCall := grantedCalls[i]
		if deniedCall.action != grantedCall.action {
			t.Fatalf("permission call matrix action mismatch: denied=%s granted=%s", deniedCall.action, grantedCall.action)
		}
		t.Run(deniedCall.action, func(t *testing.T) {
			deniedErr := deniedCall.call(ctx)
			assertGranted(t, ctx, grantedCall)
			if deniedCall.knownGap != "" {
				if deniedErr != nil {
					if hasHTTPStatus(deniedErr, 403) {
						t.Fatalf("known IAM enforcement gap is resolved; remove the exception for %s", deniedCall.action)
					}
					t.Fatalf("probe for known IAM enforcement gap failed unexpectedly: %v", deniedErr)
				}
				t.Skip(deniedCall.knownGap)
			}
			assertAccessDenied(t, deniedCall.action, deniedErr)
		})
	}
}

func assertGranted(t *testing.T, ctx context.Context, call permissionCall) {
	t.Helper()
	if err := call.call(ctx); err != nil {
		t.Fatalf("representative app policy must allow %s under enforcement: %v", call.action, err)
	}
}

func assertPermissionMatrixCoversSupportedServices(t *testing.T, calls []permissionCall) {
	t.Helper()
	covered := make(map[string]bool, len(calls))
	for _, call := range calls {
		if covered[call.service] {
			t.Fatalf("permission matrix contains duplicate service %q", call.service)
		}
		covered[call.service] = true
	}
	for _, service := range capabilities.Current("test").Services {
		if !covered[service.Service] {
			t.Errorf("permission matrix does not cover supported service %q", service.Service)
		}
		delete(covered, service.Service)
	}
	for service := range covered {
		t.Errorf("permission matrix covers unadvertised service %q", service)
	}
}

func assertPermissionMatrixMatchesAppPolicy(t *testing.T, calls []permissionCall) {
	t.Helper()
	grantedActions := testfixture.RepresentativeServiceDiscoveryIAMActions()
	granted := make(map[string]bool, len(grantedActions))
	for _, action := range grantedActions {
		granted[action] = true
	}
	matrix := make(map[string]bool, len(calls))
	for _, call := range calls {
		matrix[call.action] = true
		if !granted[call.action] {
			t.Errorf("representative app policy does not grant matrix action %q", call.action)
		}
	}
	for _, action := range grantedActions {
		if !matrix[action] {
			t.Errorf("representative app policy grants action %q that is absent from the permission matrix", action)
		}
	}
}

func supportedServicePermissionCalls(endpoint string, credentials localCredentials) []permissionCall {
	config := awsConfigWithCredentials(credentials)
	endpointValue := aws.String(endpoint)
	apiGateway := apigatewayv2.NewFromConfig(config, func(options *apigatewayv2.Options) { options.BaseEndpoint = endpointValue })
	logs := cloudwatchlogs.NewFromConfig(config, func(options *cloudwatchlogs.Options) { options.BaseEndpoint = endpointValue })
	ddb := dynamodb.NewFromConfig(config, func(options *dynamodb.Options) { options.BaseEndpoint = endpointValue })
	events := eventbridge.NewFromConfig(config, func(options *eventbridge.Options) { options.BaseEndpoint = endpointValue })
	streams := kinesis.NewFromConfig(config, func(options *kinesis.Options) { options.BaseEndpoint = endpointValue })
	functions := lambda.NewFromConfig(config, func(options *lambda.Options) { options.BaseEndpoint = endpointValue })
	objects := s3.NewFromConfig(config, func(options *s3.Options) {
		options.BaseEndpoint = endpointValue
		options.UsePathStyle = true
	})
	secrets := secretsmanager.NewFromConfig(config, func(options *secretsmanager.Options) { options.BaseEndpoint = endpointValue })
	states := sfn.NewFromConfig(config, func(options *sfn.Options) { options.BaseEndpoint = endpointValue })
	topics := sns.NewFromConfig(config, func(options *sns.Options) { options.BaseEndpoint = endpointValue })
	queues := sqs.NewFromConfig(config, func(options *sqs.Options) { options.BaseEndpoint = endpointValue })
	parameters := ssm.NewFromConfig(config, func(options *ssm.Options) { options.BaseEndpoint = endpointValue })

	return []permissionCall{
		{service: "apigateway", action: "apigateway:GET", knownGap: "Floci 1.6.0 does not enforce IAM for API Gateway v2 REST routes", call: func(ctx context.Context) error {
			_, err := apiGateway.GetApis(ctx, &apigatewayv2.GetApisInput{})
			return err
		}},
		{service: "logs", action: "logs:DescribeLogGroups", call: func(ctx context.Context) error {
			_, err := logs.DescribeLogGroups(ctx, &cloudwatchlogs.DescribeLogGroupsInput{})
			return err
		}},
		{service: "dynamodb", action: "dynamodb:ListTables", call: func(ctx context.Context) error {
			_, err := ddb.ListTables(ctx, &dynamodb.ListTablesInput{})
			return err
		}},
		{service: "events", action: "events:ListEventBuses", call: func(ctx context.Context) error {
			_, err := events.ListEventBuses(ctx, &eventbridge.ListEventBusesInput{})
			return err
		}},
		{service: "kinesis", action: "kinesis:ListStreams", call: func(ctx context.Context) error {
			_, err := streams.ListStreams(ctx, &kinesis.ListStreamsInput{})
			return err
		}},
		{service: "lambda", action: "lambda:ListFunctions", call: func(ctx context.Context) error {
			_, err := functions.ListFunctions(ctx, &lambda.ListFunctionsInput{})
			return err
		}},
		{service: "s3", action: "s3:ListAllMyBuckets", call: func(ctx context.Context) error {
			_, err := objects.ListBuckets(ctx, &s3.ListBucketsInput{})
			return err
		}},
		{service: "secretsmanager", action: "secretsmanager:ListSecrets", call: func(ctx context.Context) error {
			_, err := secrets.ListSecrets(ctx, &secretsmanager.ListSecretsInput{})
			return err
		}},
		{service: "stepfunctions", action: "states:ListStateMachines", call: func(ctx context.Context) error {
			_, err := states.ListStateMachines(ctx, &sfn.ListStateMachinesInput{})
			return err
		}},
		{service: "sns", action: "sns:ListTopics", call: func(ctx context.Context) error { _, err := topics.ListTopics(ctx, &sns.ListTopicsInput{}); return err }},
		{service: "sqs", action: "sqs:ListQueues", call: func(ctx context.Context) error { _, err := queues.ListQueues(ctx, &sqs.ListQueuesInput{}); return err }},
		{service: "ssm", action: "ssm:DescribeParameters", call: func(ctx context.Context) error {
			_, err := parameters.DescribeParameters(ctx, &ssm.DescribeParametersInput{})
			return err
		}},
	}
}

func seedAppIdentity(t *testing.T, ctx context.Context, endpoint string) localCredentials {
	t.Helper()
	return seedIdentity(t, ctx, endpoint, appUserName, "representative-app-read", string(testfixture.RepresentativeAppIAMPolicy(bucket)))
}

func seedUnprivilegedIdentity(t *testing.T, ctx context.Context, endpoint string) localCredentials {
	t.Helper()
	return seedIdentity(t, ctx, endpoint, appUserName+"-unprivileged", "", "")
}

func seedWildcardAdmin(t *testing.T, ctx context.Context, endpoint string) localCredentials {
	t.Helper()
	const adminPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	return seedIdentity(t, ctx, endpoint, appUserName+"-admin", "wildcard-admin", adminPolicy)
}

func seedIdentity(t *testing.T, ctx context.Context, endpoint, userName, policyName, policyDocument string) localCredentials {
	t.Helper()
	iamClient := iamClientFor(endpoint)
	if _, err := iamClient.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(userName)}); err != nil {
		t.Fatalf("create local IAM user %q: %v", userName, err)
	}
	if policyDocument != "" {
		if _, err := iamClient.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
			UserName:       aws.String(userName),
			PolicyName:     aws.String(policyName),
			PolicyDocument: aws.String(policyDocument),
		}); err != nil {
			t.Fatalf("attach policy %q to local IAM user %q: %v", policyName, userName, err)
		}
	}
	key, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{UserName: aws.String(userName)})
	if err != nil {
		t.Fatalf("create access key for local IAM user %q: %v", userName, err)
	}
	return localCredentials{
		accessKeyID:     aws.ToString(key.AccessKey.AccessKeyId),
		secretAccessKey: aws.ToString(key.AccessKey.SecretAccessKey),
	}
}

func iamClientFor(endpoint string) *iam.Client {
	awsConfig := awsConfigWithCredentials(localCredentials{accessKeyID: accountID, secretAccessKey: "test"})
	return iam.NewFromConfig(awsConfig, func(options *iam.Options) { options.BaseEndpoint = aws.String(endpoint) })
}

type enforcedClients struct {
	s3  *s3.Client
	ddb *dynamodb.Client
}

func clientsWithCredentials(endpoint string, credentials localCredentials) enforcedClients {
	awsConfig := awsConfigWithCredentials(credentials)
	return enforcedClients{
		s3: s3.NewFromConfig(awsConfig, func(options *s3.Options) {
			options.BaseEndpoint = aws.String(endpoint)
			options.UsePathStyle = true
		}),
		ddb: dynamodb.NewFromConfig(awsConfig, func(options *dynamodb.Options) { options.BaseEndpoint = aws.String(endpoint) }),
	}
}

func awsConfigWithCredentials(credentials localCredentials) aws.Config {
	provider := aws.NewCredentialsCache(awscredentials.NewStaticCredentialsProvider(credentials.accessKeyID, credentials.secretAccessKey, ""))
	return aws.Config{Region: region, Credentials: provider}
}

func assertAllowed(t *testing.T, ctx context.Context, client enforcedClients) {
	t.Helper()
	object, err := client.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("fixtures/hello.txt")})
	if err != nil {
		t.Fatalf("policy-granted s3:GetObject failed: %v", err)
	}
	object.Body.Close()
	item, err := client.ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table), Key: map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "enforced-fixture"}},
	})
	if err != nil {
		t.Fatalf("policy-granted dynamodb:GetItem failed: %v", err)
	}
	if len(item.Item) == 0 {
		t.Fatal("policy-granted dynamodb:GetItem returned no item")
	}
}

func assertDenied(t *testing.T, ctx context.Context, action string, call func(context.Context) error) {
	t.Helper()
	assertAccessDenied(t, action, call(ctx))
}

func assertAccessDenied(t *testing.T, action string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s was allowed but the representative app policy does not grant it", action)
	}
	var apiErr interface{ ErrorCode() string }
	message := err.Error()
	if errors.As(err, &apiErr) && apiErr.ErrorCode() != "" {
		message = apiErr.ErrorCode()
	}
	if !strings.Contains(message, "AccessDenied") || !hasHTTPStatus(err, 403) {
		t.Fatalf("%s failed with %v, want an HTTP 403 AccessDenied rejection", action, err)
	}
}

func assertObjectAbsent(t *testing.T, ctx context.Context, client enforcedClients, key string) {
	t.Helper()
	object, err := client.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err == nil {
		object.Body.Close()
		t.Fatalf("denied s3:PutObject created %q", key)
	}
	if !hasHTTPStatus(err, 404) {
		t.Fatalf("read denied object %q: %v, want HTTP 404", key, err)
	}
}

func hasHTTPStatus(err error, want int) bool {
	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) {
		return httpErr.HTTPStatusCode() == want
	}
	return false
}
