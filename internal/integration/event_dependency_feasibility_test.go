//go:build integration

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// TestEventDependencyFeasibility proves the narrow v0.6 source boundary against
// the pinned Floci image: target identity comes from the local queue/topic APIs,
// S3 accepts typed destinations and filters, and repeated configuration is
// idempotent. It intentionally does not send, receive, publish, or subscribe.
func TestEventDependencyFeasibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	bundleRoot := renderSyntheticBundle(t, ctx, replayFixture{SchemaVersion: 3, ObjectBody: "event feasibility\n", ItemID: "event-feasibility"})
	container := startFloci(t, ctx, bundleRoot, t.TempDir())
	defer container.Terminate(context.Background())
	endpoint := endpointFor(t, ctx, container)
	waitForReady(t, ctx, container, endpoint)

	s3Client, _ := clients(endpoint)
	credentialCache := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(accountID, "test", ""))
	awsConfig := aws.Config{Region: region, Credentials: credentialCache}
	sqsClient := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(endpoint) })
	snsClient := sns.NewFromConfig(awsConfig, func(options *sns.Options) { options.BaseEndpoint = aws.String(endpoint) })

	queue, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("floceed-event-jobs")})
	if err != nil {
		t.Fatalf("create standard queue: %v", err)
	}
	queueURL := aws.ToString(queue.QueueUrl)
	if queueURL == "" {
		t.Fatal("standard queue returned no URL")
	}
	queueAttributes, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: queue.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatalf("get standard queue attributes: %v", err)
	}
	queueARN := queueAttributes.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	if queueARN == "" {
		t.Fatal("standard queue returned no ARN")
	}

	fifo, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("floceed-event-jobs.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}})
	if err != nil {
		t.Fatalf("create FIFO queue: %v", err)
	}
	if aws.ToString(fifo.QueueUrl) == "" {
		t.Fatal("FIFO queue returned no URL")
	}

	topic, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("floceed-event-topic")})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	topicARN := aws.ToString(topic.TopicArn)
	if topicARN == "" {
		t.Fatal("topic returned no ARN")
	}

	configuration := &s3types.NotificationConfiguration{
		QueueConfigurations: []s3types.QueueConfiguration{{
			Id:       aws.String("queue-created"),
			QueueArn: aws.String(queueARN),
			Events:   []s3types.Event{s3types.EventS3ObjectCreated},
			Filter:   &s3types.NotificationConfigurationFilter{Key: &s3types.S3KeyFilter{FilterRules: []s3types.FilterRule{{Name: s3types.FilterRuleNamePrefix, Value: aws.String("incoming/")}}}},
		}},
		TopicConfigurations: []s3types.TopicConfiguration{{
			Id:       aws.String("topic-created"),
			TopicArn: aws.String(topicARN),
			Events:   []s3types.Event{s3types.EventS3ObjectCreatedPut},
		}},
	}
	if _, err := s3Client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String(bucket), NotificationConfiguration: configuration}); err != nil {
		t.Fatalf("configure S3 notifications: %v", err)
	}
	got, err := s3Client.GetBucketNotificationConfiguration(ctx, &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("read S3 notifications: %v", err)
	}
	if len(got.QueueConfigurations) != 1 || aws.ToString(got.QueueConfigurations[0].QueueArn) != queueARN || len(got.TopicConfigurations) != 1 || aws.ToString(got.TopicConfigurations[0].TopicArn) != topicARN {
		t.Fatalf("notification targets = %#v, want queue %s and topic %s", got, queueARN, topicARN)
	}
	if got.QueueConfigurations[0].Filter == nil || got.QueueConfigurations[0].Filter.Key == nil || len(got.QueueConfigurations[0].Filter.Key.FilterRules) != 1 || aws.ToString(got.QueueConfigurations[0].Filter.Key.FilterRules[0].Value) != "incoming/" {
		t.Fatalf("queue filter was not preserved: %#v", got.QueueConfigurations[0].Filter)
	}
	if _, err := s3Client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String(bucket), NotificationConfiguration: configuration}); err != nil {
		t.Fatalf("repeat S3 notification configuration: %v", err)
	}
	second, err := s3Client.GetBucketNotificationConfiguration(ctx, &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("read repeated S3 notifications: %v", err)
	}
	if len(second.QueueConfigurations) != 1 || len(second.TopicConfigurations) != 1 || !strings.HasSuffix(queueARN, "floceed-event-jobs") || !strings.HasSuffix(topicARN, "floceed-event-topic") {
		t.Fatalf("repeated notification configuration is not idempotent: %#v", second)
	}
}
