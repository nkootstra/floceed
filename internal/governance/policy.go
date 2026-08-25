package governance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
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
	profile        string
	rules          []Rule
	cohorts        []Cohort
	identity       string
	secretVerifier string
	secret         []byte
}

func NewEffectivePolicy(profile string, rules []Rule, cohorts []Cohort, secret []byte) (*EffectivePolicy, error) {
	p := &EffectivePolicy{profile: strings.TrimSpace(profile), rules: cloneRules(rules), cohorts: cloneCohorts(cohorts), secret: append([]byte(nil), secret...)}
	for i := range p.rules {
		normalizeRule(&p.rules[i])
	}
	for i := range p.cohorts {
		normalizeCohort(&p.cohorts[i])
	}
	for _, rule := range p.rules {
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
	for _, cohort := range p.cohorts {
		if cohort.Algorithm != CohortRankAlgorithm {
			return nil, fmt.Errorf("invalid cohort algorithm")
		}
	}
	sort.Slice(p.rules, func(i, j int) bool { return ruleSortKey(p.rules[i]) < ruleSortKey(p.rules[j]) })
	sort.Slice(p.cohorts, func(i, j int) bool { return p.cohorts[i].Resource < p.cohorts[j].Resource })
	payload, err := json.Marshal(struct {
		Profile string   `json:"profile"`
		Rules   []Rule   `json:"rules"`
		Cohorts []Cohort `json:"cohorts"`
	}{p.profile, p.rules, p.cohorts})
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
	p.identity = hex.EncodeToString(digest[:])
	return p, nil
}

func (p *EffectivePolicy) Profile() string {
	if p == nil {
		return ""
	}
	return p.profile
}
func (p *EffectivePolicy) Identity() string {
	if p == nil {
		return ""
	}
	return p.identity
}
func (p *EffectivePolicy) Rules() []Rule {
	if p == nil {
		return nil
	}
	return cloneRules(p.rules)
}
func (p *EffectivePolicy) Cohorts() []Cohort {
	if p == nil {
		return nil
	}
	return cloneCohorts(p.cohorts)
}

func (p EffectivePolicy) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Profile  string   `json:"profile"`
		Rules    []Rule   `json:"rules,omitempty"`
		Cohorts  []Cohort `json:"cohorts,omitempty"`
		Identity string   `json:"identity"`
	}{p.profile, p.rules, p.cohorts, p.identity})
}

func cloneRules(in []Rule) []Rule {
	out := append([]Rule(nil), in...)
	for i := range out {
		out[i].ContentTypes = append([]string(nil), in[i].ContentTypes...)
	}
	return out
}

func cloneCohorts(in []Cohort) []Cohort {
	out := append([]Cohort(nil), in...)
	for i := range out {
		out[i].KeyPaths = append([]string(nil), in[i].KeyPaths...)
		out[i].Predicates = append([]Predicate(nil), in[i].Predicates...)
		for j := range out[i].Predicates {
			out[i].Predicates[j].Value = cloneValue(in[i].Predicates[j].Value)
		}
	}
	return out
}

func cloneValue(value any) any {
	if value == nil {
		return nil
	}
	return cloneReflect(reflect.ValueOf(value)).Interface()
}

func cloneReflect(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := cloneReflect(value.Elem())
		wrapped := reflect.New(value.Type()).Elem()
		wrapped.Set(clone)
		return wrapped
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			clone.SetMapIndex(cloneReflect(iter.Key()), cloneReflect(iter.Value()))
		}
		return clone
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			clone.Index(i).Set(cloneReflect(value.Index(i)))
		}
		return clone
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		clone := reflect.New(value.Type().Elem())
		clone.Elem().Set(cloneReflect(value.Elem()))
		return clone
	default:
		return value
	}
}

// IdentityOf returns the stable identity of policy, or an empty identity when
// governance is disabled.
func IdentityOf(policy *EffectivePolicy) string {
	if policy == nil {
		return ""
	}
	return policy.Identity()
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
