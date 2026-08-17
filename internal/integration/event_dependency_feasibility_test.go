//go:build integration

package integration_test

import (
	"context"
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
	fifoAttributes, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: fifo.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatalf("get FIFO queue attributes: %v", err)
	}
	fifoARN := fifoAttributes.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	if fifoARN == "" {
		t.Fatal("FIFO queue returned no ARN")
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
		}, {
			Id:       aws.String("fifo-queue-created"),
			QueueArn: aws.String(fifoARN),
			Events:   []s3types.Event{s3types.EventS3ObjectCreated},
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
	assertNotificationConfiguration(t, got, queueARN, fifoARN, topicARN)
	if _, err := s3Client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String(bucket), NotificationConfiguration: configuration}); err != nil {
		t.Fatalf("repeat S3 notification configuration: %v", err)
	}
	second, err := s3Client.GetBucketNotificationConfiguration(ctx, &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatalf("read repeated S3 notifications: %v", err)
	}
	assertNotificationConfiguration(t, second, queueARN, fifoARN, topicARN)
}

func assertNotificationConfiguration(t *testing.T, got *s3.GetBucketNotificationConfigurationOutput, queueARN, fifoARN, topicARN string) {
	t.Helper()
	if len(got.QueueConfigurations) != 2 || len(got.TopicConfigurations) != 1 {
		t.Fatalf("notification counts = %d queues, %d topics", len(got.QueueConfigurations), len(got.TopicConfigurations))
	}
	if aws.ToString(got.QueueConfigurations[0].Id) != "queue-created" || aws.ToString(got.QueueConfigurations[0].QueueArn) != queueARN || len(got.QueueConfigurations[0].Events) != 1 || got.QueueConfigurations[0].Events[0] != s3types.EventS3ObjectCreated {
		t.Fatalf("standard queue configuration = %#v", got.QueueConfigurations[0])
	}
	filter := got.QueueConfigurations[0].Filter
	if filter == nil || filter.Key == nil || len(filter.Key.FilterRules) != 1 || filter.Key.FilterRules[0].Name != s3types.FilterRuleNamePrefix || aws.ToString(filter.Key.FilterRules[0].Value) != "incoming/" {
		t.Fatalf("standard queue filter = %#v", filter)
	}
	if aws.ToString(got.QueueConfigurations[1].Id) != "fifo-queue-created" || aws.ToString(got.QueueConfigurations[1].QueueArn) != fifoARN || len(got.QueueConfigurations[1].Events) != 1 || got.QueueConfigurations[1].Events[0] != s3types.EventS3ObjectCreated {
		t.Fatalf("FIFO queue configuration = %#v", got.QueueConfigurations[1])
	}
	if aws.ToString(got.TopicConfigurations[0].Id) != "topic-created" || aws.ToString(got.TopicConfigurations[0].TopicArn) != topicARN || len(got.TopicConfigurations[0].Events) != 1 || got.TopicConfigurations[0].Events[0] != s3types.EventS3ObjectCreatedPut {
		t.Fatalf("topic configuration = %#v", got.TopicConfigurations[0])
	}
}
