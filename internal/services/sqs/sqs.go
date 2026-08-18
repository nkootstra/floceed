// Package sqs captures queue structure and bounded message fixtures.
package sqs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	awsSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
	"github.com/nkootstra/floceed/internal/services/ndjson"
)

type Client interface {
	GetQueueUrl(context.Context, *awsSQS.GetQueueUrlInput, ...func(*awsSQS.Options)) (*awsSQS.GetQueueUrlOutput, error)
	ReceiveMessage(context.Context, *awsSQS.ReceiveMessageInput, ...func(*awsSQS.Options)) (*awsSQS.ReceiveMessageOutput, error)
}

type Adapter struct{ client Client }

var _ catalog.Adapter = (*Adapter)(nil)

func New(client ...Client) *Adapter {
	var c Client
	if len(client) > 0 {
		c = client[0]
	}
	return &Adapter{client: c}
}

func (*Adapter) Service() model.ServiceDescriptor {
	return model.ServiceDescriptor{Name: "sqs", DisplayName: "SQS", Support: model.SupportPartial}
}

func (*Adapter) Plan(project config.Project, includeData bool) catalog.PlanContribution {
	contribution := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.SQS))}
	messageCapture := false
	for _, resource := range project.Resources.SQS {
		selection := catalog.Selection{Resource: model.ResourceRef{Service: "sqs", Type: "queue", ID: resource.Name, ARN: resource.ARN}}
		if resource.Data != nil {
			messageCapture = messageCapture || includeData && resource.Data.Enabled
			selection.Options.IncludeData = resource.Data.Enabled
			selection.Options.Mode = string(resource.Data.Mode)
			selection.Options.Limits.MaxItems = resource.Data.MaxMessages
			selection.Options.Limits.MaxTotalBytes = resource.Data.MaxBytes
		}
		contribution.Selections = append(contribution.Selections, selection)
	}
	if messageCapture {
		contribution.RequiredIAMActions = []string{"sqs:GetQueueUrl", "sqs:ReceiveMessage"}
	}
	return contribution
}

func (*Adapter) FinalizePlanning(*model.Snapshot, []model.Dependency) ([]model.Finding, error) {
	return nil, nil
}
func (*Adapter) Discover(context.Context, model.SourceScope) (model.DiscoveryResult, error) {
	return model.DiscoveryResult{}, nil
}

func (a *Adapter) Capture(ctx context.Context, _ model.SourceScope, ref model.ResourceRef, opts model.CaptureOptions) (*model.Snapshot, error) {
	if !opts.IncludeData {
		return model.NewSnapshot(ref, "sqs", map[string]string{"name": ref.ID, "arn": ref.ARN})
	}
	if a.client == nil || opts.ArtifactDirectory == "" {
		return nil, fmt.Errorf("SQS message capture requires a client and artifact directory: %w", model.ErrValidation)
	}
	url, err := a.client.GetQueueUrl(ctx, &awsSQS.GetQueueUrlInput{QueueName: &ref.ID})
	if err != nil {
		return nil, err
	}
	max := int32(opts.Limits.MaxItems)
	if max <= 0 || max > 10 {
		max = 10
	}
	messages, err := a.client.ReceiveMessage(ctx, &awsSQS.ReceiveMessageInput{QueueUrl: url.QueueUrl, MaxNumberOfMessages: max, VisibilityTimeout: 0, WaitTimeSeconds: 0, MessageAttributeNames: []string{"All"}})
	if err != nil {
		return nil, err
	}
	writer, err := ndjson.Create(opts.ArtifactDirectory, "bundle/data/sqs/"+ref.ID+".ndjson", opts.Limits.MaxTotalBytes)
	if err != nil {
		return nil, err
	}
	var count int64
	for _, message := range messages.Messages {
		body := []byte("")
		if message.Body != nil {
			body = []byte(*message.Body)
		}
		value, err := json.Marshal(map[string]any{"body_base64": base64.StdEncoding.EncodeToString(body), "message_attributes": message.MessageAttributes})
		if err != nil {
			writer.Abort()
			return nil, err
		}
		written, err := writer.Write(value)
		if err != nil {
			writer.Abort()
			return nil, err
		}
		if !written {
			break
		}
		count++
		if opts.Progress != nil {
			opts.Progress(model.ProgressEvent{Operation: "pull", Phase: "capture", Service: "sqs", Resource: ref.ID, CompletedRecords: count, CompletedBytes: writer.Size(), TotalRecords: int64(opts.Limits.MaxItems), TotalBytes: opts.Limits.MaxTotalBytes, TotalPrecision: "unknown"})
		}
	}
	artifact, err := writer.Commit()
	if err != nil {
		return nil, err
	}
	snapshot, err := model.NewSnapshot(ref, "sqs", map[string]string{"name": ref.ID, "arn": ref.ARN})
	if err != nil {
		return nil, err
	}
	snapshot.Dataset = &model.Dataset{Format: "sqs-messages-ndjson-v1", Records: count, SourceBytes: artifact.Size, Consistency: "best_effort", Chunks: []model.DataChunk{{Data: artifact, Records: count, SourceBytes: artifact.Size}}}
	return snapshot, nil
}

func (*Adapter) Dependencies(*model.Snapshot) []model.Dependency              { return nil }
func (*Adapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }
