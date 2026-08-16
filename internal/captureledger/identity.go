package captureledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// SourceIdentity is the resolved AWS identity that owns a capture. It
// deliberately excludes local profile and project directory names.
type SourceIdentity struct {
	AccountID string `json:"account_id"`
	Region    string `json:"region"`
}

type ResourceDescriptor struct {
	Service string `json:"service"`
	Type    string `json:"type"`
	ID      string `json:"id"`
}

type Limits struct {
	MaxObjects     int   `json:"max_objects,omitempty"`
	MaxItems       int   `json:"max_items,omitempty"`
	MaxPages       int   `json:"max_pages,omitempty"`
	MaxObjectBytes int64 `json:"max_object_bytes,omitempty"`
	MaxTotalBytes  int64 `json:"max_total_bytes,omitempty"`
}

// CaptureDefinition contains only fields that can change the captured bytes
// or their interpretation. Transient paths, callbacks, estimates and profile
// aliases are intentionally absent.
type CaptureDefinition struct {
	Source              SourceIdentity     `json:"source"`
	Resource            ResourceDescriptor `json:"resource"`
	Mode                string             `json:"mode"`
	Prefixes            []string           `json:"prefixes,omitempty"`
	Limits              Limits             `json:"limits,omitempty"`
	Overwrite           string             `json:"overwrite,omitempty"`
	Gzip                bool               `json:"gzip,omitempty"`
	PreserveProvisioned bool               `json:"preserve_provisioned,omitempty"`
	AllowPartialData    bool               `json:"allow_partial_data,omitempty"`
	PolicyIdentity      string             `json:"policy_identity,omitempty"`
	DatasetFormat       string             `json:"dataset_format"`
	DatasetVersion      int                `json:"dataset_version,omitempty"`
	StructureVersion    int                `json:"structure_version"`
}

func DigestCaptureDefinition(definition CaptureDefinition) (string, error) {
	normalized := normalizeCaptureDefinition(definition)
	if err := normalized.validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("encode capture definition: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func normalizeCaptureDefinition(definition CaptureDefinition) CaptureDefinition {
	if definition.Mode == "" {
		definition.Mode = "bounded"
	}
	if definition.Resource.Service == "s3" {
		if definition.Overwrite == "" {
			definition.Overwrite = "if-different"
		}
		if len(definition.Prefixes) == 0 {
			definition.Prefixes = []string{""}
		} else {
			definition.Prefixes = append([]string(nil), definition.Prefixes...)
			slices.Sort(definition.Prefixes)
			definition.Prefixes = slices.Compact(definition.Prefixes)
			compacted := definition.Prefixes[:0]
			for _, prefix := range definition.Prefixes {
				if len(compacted) == 0 || !strings.HasPrefix(prefix, compacted[len(compacted)-1]) {
					compacted = append(compacted, prefix)
				}
			}
			definition.Prefixes = compacted
		}
	} else if len(definition.Prefixes) == 0 {
		definition.Prefixes = nil
	}
	return definition
}

func (definition CaptureDefinition) validate() error {
	if err := definition.Source.validate(); err != nil {
		return err
	}
	if err := definition.Resource.validate(); err != nil {
		return err
	}
	if definition.Mode != "bounded" && definition.Mode != "full" {
		return fmt.Errorf("unsupported capture mode %q", definition.Mode)
	}
	if definition.Limits.MaxObjects < 0 || definition.Limits.MaxItems < 0 || definition.Limits.MaxPages < 0 || definition.Limits.MaxObjectBytes < 0 || definition.Limits.MaxTotalBytes < 0 {
		return fmt.Errorf("capture limits must be nonnegative")
	}
	if definition.PolicyIdentity != "" && !validSHA256(definition.PolicyIdentity) {
		return fmt.Errorf("policy identity must be a SHA-256 digest")
	}
	if strings.TrimSpace(definition.DatasetFormat) == "" || definition.DatasetVersion < 0 || definition.StructureVersion <= 0 {
		return fmt.Errorf("dataset format and positive structure version are required")
	}
	return nil
}

func (source SourceIdentity) validate() error {
	if len(source.AccountID) != 12 {
		return fmt.Errorf("source account ID must be 12 digits")
	}
	for _, char := range source.AccountID {
		if char < '0' || char > '9' {
			return fmt.Errorf("source account ID must be 12 digits")
		}
	}
	if strings.TrimSpace(source.Region) == "" {
		return fmt.Errorf("source region is required")
	}
	return nil
}

func (resource ResourceDescriptor) validate() error {
	if strings.TrimSpace(resource.Service) == "" || strings.TrimSpace(resource.Type) == "" || strings.TrimSpace(resource.ID) == "" {
		return fmt.Errorf("resource service, type, and ID are required")
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
