package app

import (
	"context"
	"fmt"

	"github.com/nkootstra/floceed/internal/awsconfig"
	"github.com/nkootstra/floceed/internal/catalog"
	"github.com/nkootstra/floceed/internal/config"
)

// PermissionResult contains only identity and permission metadata. Source
// payloads returned by probes are deliberately not represented here.
type PermissionResult struct {
	Identity awsconfig.Identity `json:"identity"`
	Checks   []Check            `json:"checks"`
}

func (a *Application) Preflight(ctx context.Context, p config.Project, profile, region string) (PermissionResult, error) {
	return a.PreflightWithOptions(ctx, p, profile, region, true)
}

// PreflightWithOptions runs source permission probes. StructureCapture controls
// whether adapters without a specialized bounded probe may capture metadata as
// a read-only permission check. Pull disables that fallback to avoid a second
// structure capture before its normal capture/reuse path.
func (a *Application) PreflightWithOptions(ctx context.Context, p config.Project, profile, region string, structureCapture bool) (PermissionResult, error) {
	if err := p.Validate(); err != nil {
		return PermissionResult{}, &Error{Kind: ErrorPlan, Code: "PROJECT_INVALID", Message: err.Error(), Err: err}
	}
	if profile == "" {
		profile = p.Source.Profile
	}
	if region == "" {
		region = p.Source.Region
	}
	source, err := a.Factory.Open(ctx, SourceRequest{Profile: profile, Region: region, S3Names: s3Names(p), DynamoDBNames: ddbNames(p)})
	if err != nil {
		return PermissionResult{}, sourceError(err)
	}
	return a.preflightSource(ctx, p, source, structureCapture)
}

func (a *Application) preflightSource(ctx context.Context, p config.Project, source Source, structureCapture bool) (PermissionResult, error) {
	if p.Source.ExpectedAccountID != "" && p.Source.ExpectedAccountID != source.Identity.AccountID {
		return PermissionResult{}, &Error{Kind: ErrorSource, Code: "SOURCE_ACCOUNT_MISMATCH", Message: fmt.Sprintf("AWS profile resolved to account %s, expected %s", source.Identity.AccountID, p.Source.ExpectedAccountID)}
	}
	result := PermissionResult{Identity: source.Identity}
	if source.Registry == nil {
		return result, nil
	}
	for _, adapter := range source.Registry.All() {
		checker, ok := adapter.(catalog.PermissionChecker)
		for _, selection := range adapter.Plan(p, true).Selections {
			if !ok {
				if !structureCapture {
					continue
				}
				options := selection.Options
				options.IncludeData = false
				_, captureErr := adapter.Capture(ctx, source.Scope, selection.Resource, options)
				result.Checks = append(result.Checks, Check{
					Name:     fmt.Sprintf("aws:%s:%s:structure", selection.Resource.Service, selection.Resource.ID),
					OK:       captureErr == nil,
					Blocking: true,
					Message:  errorMessage(captureErr, "structure permission verified"),
				})
				continue
			}
			for _, check := range checker.CheckPermissions(ctx, source.Scope, selection.Resource, selection.Options) {
				name := fmt.Sprintf("aws:%s:%s:%s", check.Service, check.Resource, check.Action)
				message := check.Message
				if message == "" {
					message = "permission verified"
				}
				if !check.OK {
					message = fmt.Sprintf("missing %s on %s: %s", check.Action, check.ARN, message)
				}
				result.Checks = append(result.Checks, Check{Name: name, OK: check.OK, Blocking: check.Blocking, Message: message})
			}
		}
	}
	for _, check := range result.Checks {
		if check.Blocking && !check.OK {
			return result, &Error{Kind: ErrorSource, Code: "PERMISSION_PREFLIGHT_FAILED", Message: "one or more AWS permissions are missing"}
		}
	}
	return result, nil
}

func errorMessage(err error, success string) string {
	if err == nil {
		return success
	}
	return err.Error()
}

func permissionPreflightError(result PermissionResult, fallback error) error {
	for _, check := range result.Checks {
		if check.Blocking && !check.OK {
			return &Error{Kind: ErrorSource, Code: "PERMISSION_PREFLIGHT_FAILED", Message: check.Message, Remediation: "Grant the missing AWS permission and retry.", Err: fallback}
		}
	}
	return fallback
}
