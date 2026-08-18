// Package sqs captures queue structure and bounded message fixtures.
package sqs

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	awsSQS "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
	"github.com/nkootstra/floceed/internal/model"
)

type Client interface {
	GetQueueUrl(context.Context, *awsSQS.GetQueueUrlInput, ...func(*awsSQS.Options)) (*awsSQS.GetQueueUrlOutput, error)
	ReceiveMessage(context.Context, *awsSQS.ReceiveMessageInput, ...func(*awsSQS.Options)) (*awsSQS.ReceiveMessageOutput, error)
}

type Adapter struct{ client Client }

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

func (*Adapter) Plan(project config.Project, _ bool) catalog.PlanContribution {
	contribution := catalog.PlanContribution{Selections: make([]catalog.Selection, 0, len(project.Resources.SQS))}
	for _, resource := range project.Resources.SQS {
		selection := catalog.Selection{Resource: model.ResourceRef{Service: "sqs", Type: "queue", ID: resource.Name, ARN: resource.ARN}}
		if resource.Data != nil {
			contribution.RequiredIAMActions = append(contribution.RequiredIAMActions, "sqs:GetQueueUrl")
			selection.Options.IncludeData = resource.Data.Enabled
			selection.Options.Mode = string(resource.Data.Mode)
			selection.Options.Limits.MaxItems = resource.Data.MaxMessages
			selection.Options.Limits.MaxTotalBytes = resource.Data.MaxBytes
			contribution.RequiredIAMActions = append(contribution.RequiredIAMActions, "sqs:ReceiveMessage")
		}
		contribution.Selections = append(contribution.Selections, selection)
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
	name := "bundle/data/sqs/" + ref.ID + ".ndjson"
	clean := filepath.Clean(filepath.FromSlash(name))
	if filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("unsafe SQS artifact path: %w", model.ErrValidation)
	}
	destination := filepath.Join(opts.ArtifactDirectory, clean)
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	writer := bufio.NewWriter(io.MultiWriter(file, hash))
	var count, size int64
	for _, message := range messages.Messages {
		body := []byte("")
		if message.Body != nil {
			body = []byte(*message.Body)
		}
		value, _ := json.Marshal(map[string]any{"body_base64": base64.StdEncoding.EncodeToString(body), "message_attributes": message.MessageAttributes})
		value = append(value, '\n')
		if opts.Limits.MaxTotalBytes > 0 && size+int64(len(value)) > opts.Limits.MaxTotalBytes {
			break
		}
		if _, err := writer.Write(value); err != nil {
			file.Close()
			return nil, err
		}
		count++
		size += int64(len(value))
		if opts.Progress != nil {
			opts.Progress(model.ProgressEvent{Operation: "pull", Phase: "capture", Service: "sqs", Resource: ref.ID, CompletedRecords: count, CompletedBytes: size, TotalRecords: int64(opts.Limits.MaxItems), TotalBytes: opts.Limits.MaxTotalBytes, TotalPrecision: "unknown"})
		}
	}
	if err := writer.Flush(); err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	snapshot, err := model.NewSnapshot(ref, "sqs", map[string]string{"name": ref.ID, "arn": ref.ARN})
	if err != nil {
		return nil, err
	}
	snapshot.Dataset = &model.Dataset{Format: "sqs-messages-ndjson-v1", Records: count, SourceBytes: size, Consistency: "best_effort", Chunks: []model.DataChunk{{Data: model.ArtifactRef{Path: filepath.ToSlash(name), SHA256: hex.EncodeToString(hash.Sum(nil)), Size: size}, Records: count, SourceBytes: size}}}
	return snapshot, nil
}

func (*Adapter) Dependencies(*model.Snapshot) []model.Dependency              { return nil }
func (*Adapter) Validate(*model.Snapshot, model.Capabilities) []model.Finding { return nil }
