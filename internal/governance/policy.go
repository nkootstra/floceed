package governance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	PseudonymAlgorithm  = "pseudonym/v1"
	CohortRankAlgorithm = "cohort-rank/v1"
	HashAlgorithm       = "hash/v1"
)

type Action string

const (
	ActionOmit         Action = "omit"
	ActionReplace      Action = "replace"
	ActionHash         Action = "hash"
	ActionPseudonymize Action = "pseudonymize"
)

type Service string

const (
	ServiceS3       Service = "s3"
	ServiceDynamoDB Service = "dynamodb"
)

type TargetKind string

const (
	TargetDynamoDBAttribute TargetKind = "dynamodb_attribute"
	TargetS3Metadata        TargetKind = "s3_metadata"
	TargetS3TextBody        TargetKind = "s3_text_body"
)

type Target struct {
	Kind TargetKind `json:"kind"`
	Path string     `json:"path,omitempty"`
}

type Rule struct {
	ID           string   `json:"id"`
	Service      Service  `json:"service"`
	Resource     string   `json:"resource"`
	Target       Target   `json:"target"`
	Action       Action   `json:"action"`
	Replacement  string   `json:"replacement,omitempty"`
	KeyID        string   `json:"key_id,omitempty"`
	Scope        string   `json:"scope,omitempty"`
	Algorithm    string   `json:"algorithm,omitempty"`
	ContentTypes []string `json:"content_types,omitempty"`
}

type Predicate struct {
	Attribute string `json:"attribute"`
	Value     any    `json:"value"`
}

type Cohort struct {
	Resource         string      `json:"resource"`
	KeyID            string      `json:"key_id"`
	Scope            string      `json:"scope,omitempty"`
	Algorithm        string      `json:"algorithm"`
	KeyPaths         []string    `json:"key_paths"`
	Limit            int         `json:"limit"`
	MaxRetainedBytes int64       `json:"max_retained_bytes"`
	Predicates       []Predicate `json:"predicates,omitempty"`
}

// EffectivePolicy is the normalized runtime policy. Secret material is kept
// private and is deliberately excluded from serialization.
type EffectivePolicy struct {
	Profile        string   `json:"profile"`
	Rules          []Rule   `json:"rules,omitempty"`
	Cohorts        []Cohort `json:"cohorts,omitempty"`
	Identity       string   `json:"identity"`
	secretVerifier string
	secret         []byte
}

func NewEffectivePolicy(profile string, rules []Rule, cohorts []Cohort, secret []byte) (*EffectivePolicy, error) {
	p := &EffectivePolicy{Profile: strings.TrimSpace(profile), Rules: append([]Rule(nil), rules...), Cohorts: append([]Cohort(nil), cohorts...), secret: append([]byte(nil), secret...)}
	for i := range p.Rules {
		normalizeRule(&p.Rules[i])
	}
	for i := range p.Cohorts {
		normalizeCohort(&p.Cohorts[i])
	}
	for _, rule := range p.Rules {
		switch rule.Action {
		case ActionHash:
			if rule.Algorithm != HashAlgorithm {
				return nil, fmt.Errorf("invalid hash algorithm")
			}
		case ActionPseudonymize:
			if rule.Algorithm != PseudonymAlgorithm {
				return nil, fmt.Errorf("invalid pseudonym algorithm")
			}
		case ActionOmit, ActionReplace:
			if rule.Algorithm != "" {
				return nil, fmt.Errorf("action %s does not accept an algorithm", rule.Action)
			}
		default:
			return nil, fmt.Errorf("invalid governance action %q", rule.Action)
		}
	}
	for _, cohort := range p.Cohorts {
		if cohort.Algorithm != CohortRankAlgorithm {
			return nil, fmt.Errorf("invalid cohort algorithm")
		}
	}
	sort.Slice(p.Rules, func(i, j int) bool { return ruleSortKey(p.Rules[i]) < ruleSortKey(p.Rules[j]) })
	sort.Slice(p.Cohorts, func(i, j int) bool { return p.Cohorts[i].Resource < p.Cohorts[j].Resource })
	payload, err := json.Marshal(struct {
		Profile string   `json:"profile"`
		Rules   []Rule   `json:"rules"`
		Cohorts []Cohort `json:"cohorts"`
	}{p.Profile, p.Rules, p.Cohorts})
	if err != nil {
		return nil, fmt.Errorf("encode governance policy: %w", err)
	}
	if len(secret) != 0 {
		verifier := sha256.Sum256(append([]byte("floceed/governance-secret-verifier/v1\x00"), secret...))
		p.secretVerifier = hex.EncodeToString(verifier[:])
	}
	identityInput := append(append([]byte(nil), payload...), 0)
	identityInput = append(identityInput, p.secretVerifier...)
	digest := sha256.Sum256(identityInput)
	p.Identity = hex.EncodeToString(digest[:])
	return p, nil
}

// IdentityOf returns the stable identity of policy, or an empty identity when
// governance is disabled.
func IdentityOf(policy *EffectivePolicy) string {
	if policy == nil {
		return ""
	}
	return policy.Identity
}

func (p *EffectivePolicy) Secret() []byte {
	if p == nil {
		return nil
	}
	return append([]byte(nil), p.secret...)
}

func normalizeRule(rule *Rule) {
	rule.ID = strings.TrimSpace(rule.ID)
	rule.Resource = strings.TrimSpace(rule.Resource)
	rule.Target.Path = strings.TrimSpace(rule.Target.Path)
	if rule.Target.Kind == TargetS3Metadata {
		rule.Target.Path = strings.ToLower(rule.Target.Path)
	}
	rule.KeyID = strings.TrimSpace(rule.KeyID)
	rule.Scope = strings.TrimSpace(rule.Scope)
	rule.Algorithm = strings.TrimSpace(rule.Algorithm)
	if rule.Algorithm == "" {
		switch rule.Action {
		case ActionHash:
			rule.Algorithm = HashAlgorithm
		case ActionPseudonymize:
			rule.Algorithm = PseudonymAlgorithm
		}
	}
	for i := range rule.ContentTypes {
		rule.ContentTypes[i] = strings.ToLower(strings.TrimSpace(rule.ContentTypes[i]))
	}
	sort.Strings(rule.ContentTypes)
}

func normalizeCohort(cohort *Cohort) {
	cohort.Resource = strings.TrimSpace(cohort.Resource)
	cohort.KeyID = strings.TrimSpace(cohort.KeyID)
	cohort.Scope = strings.TrimSpace(cohort.Scope)
	cohort.Algorithm = strings.TrimSpace(cohort.Algorithm)
	if cohort.Algorithm == "" {
		cohort.Algorithm = CohortRankAlgorithm
	}
	if cohort.MaxRetainedBytes == 0 {
		cohort.MaxRetainedBytes = DefaultCohortMaxRetainedBytes
	}
	for i := range cohort.KeyPaths {
		cohort.KeyPaths[i] = strings.TrimSpace(cohort.KeyPaths[i])
	}
	sort.Strings(cohort.KeyPaths)
	sort.Slice(cohort.Predicates, func(i, j int) bool { return cohort.Predicates[i].Attribute < cohort.Predicates[j].Attribute })
}

func ruleSortKey(rule Rule) string {
	return string(rule.Service) + "\x00" + rule.Resource + "\x00" + string(rule.Target.Kind) + "\x00" + rule.Target.Path + "\x00" + rule.ID
}
