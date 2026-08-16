package policy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/nkootstra/floceed/internal/model"
	"gopkg.in/yaml.v3"
)

const CurrentSchemaVersion = 1

type Policy struct {
	SchemaVersion       int             `yaml:"schema_version" json:"schema_version"`
	AllowedAccounts     []string        `yaml:"allowed_accounts" json:"allowed_accounts"`
	MaxAge              time.Duration   `yaml:"-" json:"max_age"`
	MaxAgeText          string          `yaml:"max_age" json:"-"`
	AllowPartial        bool            `yaml:"allow_partial" json:"allow_partial"`
	AllowTruncated      bool            `yaml:"allow_truncated" json:"allow_truncated"`
	AllowedFindingCodes []string        `yaml:"allowed_finding_codes" json:"allowed_finding_codes"`
	AllowedSeverities   []string        `yaml:"allowed_severities" json:"allowed_severities"`
	Producer            ProducerBinding `yaml:"producer" json:"producer"`
}

type ProducerBinding struct {
	Repository string `yaml:"repository" json:"repository"`
	Workflow   string `yaml:"workflow" json:"workflow"`
}

type Facts struct {
	Identity   string
	Manifest   model.Manifest
	CapturedAt time.Time
	Provenance *model.Provenance
	ProducerOK bool
}

type Decision struct {
	Allowed      bool     `json:"allowed"`
	PolicySchema int      `json:"policy_schema"`
	PolicyDigest string   `json:"policy_digest"`
	Fixture      string   `json:"fixture_identity"`
	EvaluatedAt  string   `json:"evaluated_at"`
	Reasons      []string `json:"reasons,omitempty"`
}

func Load(data []byte) (Policy, error) {
	var raw struct {
		SchemaVersion       int             `yaml:"schema_version"`
		AllowedAccounts     []string        `yaml:"allowed_accounts"`
		MaxAge              string          `yaml:"max_age"`
		AllowPartial        bool            `yaml:"allow_partial"`
		AllowTruncated      bool            `yaml:"allow_truncated"`
		AllowedFindingCodes []string        `yaml:"allowed_finding_codes"`
		AllowedSeverities   []string        `yaml:"allowed_severities"`
		Producer            ProducerBinding `yaml:"producer"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil {
		return Policy{}, fmt.Errorf("decode admission policy: %w", err)
	}
	if raw.SchemaVersion != CurrentSchemaVersion {
		return Policy{}, fmt.Errorf("unsupported admission policy schema %d", raw.SchemaVersion)
	}
	maxAge := time.Duration(0)
	if raw.MaxAge != "" {
		var err error
		maxAge, err = time.ParseDuration(raw.MaxAge)
		if err != nil || maxAge < 0 {
			return Policy{}, fmt.Errorf("invalid max_age %q", raw.MaxAge)
		}
	}
	for _, account := range raw.AllowedAccounts {
		if !validAccount(account) {
			return Policy{}, fmt.Errorf("invalid allowed account %q", account)
		}
	}
	sort.Strings(raw.AllowedAccounts)
	sort.Strings(raw.AllowedFindingCodes)
	sort.Strings(raw.AllowedSeverities)
	return Policy{SchemaVersion: raw.SchemaVersion, AllowedAccounts: raw.AllowedAccounts, MaxAge: maxAge, MaxAgeText: raw.MaxAge, AllowPartial: raw.AllowPartial, AllowTruncated: raw.AllowTruncated, AllowedFindingCodes: raw.AllowedFindingCodes, AllowedSeverities: raw.AllowedSeverities, Producer: raw.Producer}, nil
}

func (p Policy) CanonicalDigest() (string, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:]), nil
}

func (p Policy) Evaluate(f Facts, now time.Time) Decision {
	digest, _ := p.CanonicalDigest()
	d := Decision{Allowed: true, PolicySchema: p.SchemaVersion, PolicyDigest: digest, Fixture: f.Identity, EvaluatedAt: now.UTC().Format(time.RFC3339Nano)}
	deny := func(reason string) { d.Allowed = false; d.Reasons = append(d.Reasons, reason) }
	if f.Manifest.Source.AccountID == "" || !contains(p.AllowedAccounts, f.Manifest.Source.AccountID) {
		deny("source_account_not_allowed")
	}
	if p.MaxAge > 0 && f.CapturedAt.IsZero() {
		deny("capture_time_missing")
	} else if p.MaxAge > 0 && now.Sub(f.CapturedAt) > p.MaxAge {
		deny("fixture_expired")
	}
	for _, finding := range append(append([]model.Finding(nil), f.Manifest.Findings...), snapshotFindings(f.Manifest.Snapshots)...) {
		if contains(p.AllowedFindingCodes, finding.Code) || contains(p.AllowedSeverities, string(finding.Severity)) {
			continue
		}
		deny("finding_" + finding.Code)
	}
	if !f.ProducerOK && (p.Producer.Repository != "" || p.Producer.Workflow != "") {
		deny("producer_binding_unverified")
	}
	sort.Strings(d.Reasons)
	d.Reasons = unique(d.Reasons)
	return d
}

func snapshotFindings(snapshots []model.Snapshot) []model.Finding {
	var out []model.Finding
	for _, snapshot := range snapshots {
		out = append(out, snapshot.Findings...)
	}
	return out
}
func validAccount(account string) bool {
	if len(account) != 12 {
		return false
	}
	for _, r := range account {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
func contains(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}
func unique(values []string) []string {
	out := values[:0]
	for _, value := range values {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}
