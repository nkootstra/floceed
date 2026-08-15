// Package awsconfig loads source AWS configuration without owning credentials.
package awsconfig

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

type ErrorKind string

const (
	ErrorAuthentication ErrorKind = "authentication"
	ErrorExpiredSSO     ErrorKind = "expired_sso"
	ErrorAccessDenied   ErrorKind = "access_denied"
	ErrorConfiguration  ErrorKind = "configuration"
)

type SourceError struct {
	Kind               ErrorKind
	Operation, Profile string
	Err                error
}

func (e *SourceError) Error() string {
	if e.Kind == ErrorExpiredSSO && e.Profile != "" {
		return fmt.Sprintf("%s: AWS SSO session expired; run `aws sso login --profile %s`", e.Operation, e.Profile)
	}
	var api smithy.APIError
	if errors.As(e.Err, &api) {
		return fmt.Sprintf("%s: %s (%s)", e.Operation, e.Kind, api.ErrorCode())
	}
	return fmt.Sprintf("%s: %s", e.Operation, e.Kind)
}
func (e *SourceError) Unwrap() error { return e.Err }

func Classify(err error, operation, profile string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		code := strings.ToLower(api.ErrorCode())
		if strings.Contains(code, "accessdenied") || strings.Contains(code, "unauthorized") {
			return &SourceError{ErrorAccessDenied, operation, profile, err}
		}
		if strings.Contains(code, "expiredtoken") || strings.Contains(code, "invalidclienttoken") {
			return &SourceError{ErrorAuthentication, operation, profile, err}
		}
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "sso") && (strings.Contains(msg, "expired") || strings.Contains(msg, "invalid token") || strings.Contains(msg, "login")) {
		return &SourceError{ErrorExpiredSSO, operation, profile, err}
	}
	return &SourceError{ErrorConfiguration, operation, profile, err}
}

// Profiles reads section names, never values. The AWS SDK remains responsible
// for interpreting and resolving the selected profile.
func Profiles(configFile, credentialsFile string) ([]string, error) {
	seen := map[string]struct{}{}
	for _, f := range []struct {
		path   string
		config bool
	}{{configFile, true}, {credentialsFile, false}} {
		if f.path == "" {
			continue
		}
		file, err := os.Open(f.path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read AWS profile names: %w", err)
		}
		if err := collectProfileNames(file, f.config, seen); err != nil {
			return nil, fmt.Errorf("read AWS profile names: %w", err)
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

func collectProfileNames(file *os.File, configFile bool, seen map[string]struct{}) error {
	defer file.Close()
	s := bufio.NewScanner(file)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if len(line) < 3 || line[0] != '[' || line[len(line)-1] != ']' {
			continue
		}
		name := strings.TrimSpace(line[1 : len(line)-1])
		// In the config file only `default` and `profile NAME` sections name
		// profiles; sections such as `sso-session NAME` are not profiles.
		if configFile && name != "default" {
			if strings.HasPrefix(name, "profile ") {
				name = strings.TrimSpace(strings.TrimPrefix(name, "profile "))
			} else {
				continue
			}
		}
		if name != "" {
			seen[name] = struct{}{}
		}
	}
	return s.Err()
}

func DefaultProfilePaths() (string, string) {
	home, _ := os.UserHomeDir()
	cfg := os.Getenv("AWS_CONFIG_FILE")
	if cfg == "" {
		cfg = filepath.Join(home, ".aws", "config")
	}
	cred := os.Getenv("AWS_SHARED_CREDENTIALS_FILE")
	if cred == "" {
		cred = filepath.Join(home, ".aws", "credentials")
	}
	return cfg, cred
}
func AvailableProfiles() ([]string, error) { return Profiles(DefaultProfilePaths()) }

func Load(ctx context.Context, profile, region string) (aws.Config, error) {
	opts := []func(*awscfg.LoadOptions) error{awscfg.WithRegion(region)}
	if profile != "" {
		opts = append(opts, awscfg.WithSharedConfigProfile(profile))
	}
	cfg, err := awscfg.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, Classify(err, "load AWS configuration", profile)
	}
	if _, err = cfg.Credentials.Retrieve(ctx); err != nil {
		return aws.Config{}, Classify(err, "retrieve AWS credentials", profile)
	}
	return cfg, nil
}

type STSAPI interface {
	GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}
type Identity struct{ AccountID, ARN, UserID string }

func CallerIdentity(ctx context.Context, client STSAPI, profile string) (Identity, error) {
	o, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return Identity{}, Classify(err, "confirm AWS caller identity", profile)
	}
	if o.Account == nil || len(*o.Account) != 12 {
		return Identity{}, &SourceError{ErrorAuthentication, "confirm AWS caller identity", profile, errors.New("AWS returned no valid account ID")}
	}
	return Identity{aws.ToString(o.Account), aws.ToString(o.Arn), aws.ToString(o.UserId)}, nil
}
