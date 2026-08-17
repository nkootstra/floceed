package s3

import (
	"errors"
	"sort"
	"strings"

	"github.com/aws/smithy-go"
	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/model"
)

func truncated(resource, property string) model.Finding {
	return model.Finding{Code: "S3_DATA_LIMIT_REACHED", Severity: model.SeverityInfo, Support: model.SupportPartial, Resource: resource, Property: property, Message: "The configured fixture boundary was reached; the captured fixture is intentionally truncated."}
}
func unsupported(resource, property, code, message string) model.Finding {
	return model.Finding{Code: code, Severity: model.SeverityWarning, Support: model.SupportTargetUnsupported, Resource: resource, Property: property, Message: message}
}
func optionalFinding(resource, code, property string, err error) model.Finding {
	classified := awsconfig.Classify(err, "read S3 "+property, "")
	return model.Finding{Code: code, Severity: model.SeverityWarning, Support: model.SupportPartial, Resource: resource, Property: property, Message: classified.Error(), Remediation: "Grant the corresponding read-only S3 permission and retry."}
}
func isAbsent(err error, codes map[string]bool) bool {
	var api smithy.APIError
	if errors.As(err, &api) {
		return codes[api.ErrorCode()]
	}
	return false
}
func (a *Adapter) Dependencies(s *model.Snapshot) []model.Dependency {
	b, err := model.DecodeStructure[Bucket](s)
	if err != nil {
		return nil
	}
	root, ok := b.Notifications.(map[string]any)
	if !ok {
		return nil
	}
	typesByField := map[string]string{"QueueConfigurations": "queue", "TopicConfigurations": "topic", "LambdaFunctionConfigurations": "function"}
	var out []model.Dependency
	seen := make(map[string]struct{})
	for field, resourceType := range typesByField {
		values, _ := root[field].([]any)
		for _, value := range values {
			configuration, _ := value.(map[string]any)
			arn, _ := configuration[map[string]string{"queue": "QueueArn", "topic": "TopicArn", "function": "LambdaFunctionArn"}[resourceType]].(string)
			if arn == "" {
				continue
			}
			if _, exists := seen[arn]; exists {
				continue
			}
			seen[arn] = struct{}{}
			parts := strings.Split(arn, ":")
			id := parts[len(parts)-1]
			service := map[string]string{"queue": "sqs", "topic": "sns", "function": "lambda"}[resourceType]
			out = append(out, model.Dependency{From: s.Resource, To: model.ResourceRef{Service: service, Type: resourceType, ID: id, ARN: arn}, Kind: "s3_notification", Required: true})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].To.ARN < out[j].To.ARN })
	return out
}
func (a *Adapter) Validate(s *model.Snapshot, _ model.Capabilities) []model.Finding {
	b, err := model.DecodeStructure[Bucket](s)
	if err != nil {
		return nil
	}
	var out []model.Finding
	if b.Lifecycle != nil {
		out = append(out, model.Finding{Code: "FLOCI_S3_LIFECYCLE_SEMANTICS", Severity: model.SeverityWarning, Support: model.SupportStructureOnly, Resource: b.Name, Property: "lifecycle", Message: "Floci 1.6.0 stores lifecycle rules but does not execute lifecycle transitions."})
	}
	if b.Website != nil {
		out = append(out, model.Finding{Code: "FLOCI_S3_WEBSITE_PARTIAL", Severity: model.SeverityWarning, Support: model.SupportPartial, Resource: b.Name, Property: "website", Message: "Floci 1.6.0 supports index and error documents; advanced redirects may not be faithful."})
	}
	return out
}
