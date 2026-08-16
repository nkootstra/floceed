package governance

import (
	"sort"
	"sync"
)

type CountBucket string

const (
	BucketZero                           CountBucket = "0"
	BucketOneToNine                      CountBucket = "1-9"
	BucketTenToNinetyNine                CountBucket = "10-99"
	BucketHundredToNineHundredNinetyNine CountBucket = "100-999"
	BucketThousandPlus                   CountBucket = "1000+"
)

func BucketCount(count int) CountBucket {
	switch {
	case count <= 0:
		return BucketZero
	case count < 10:
		return BucketOneToNine
	case count < 100:
		return BucketTenToNinetyNine
	case count < 1000:
		return BucketHundredToNineHundredNinetyNine
	default:
		return BucketThousandPlus
	}
}

type RuleAudit struct {
	RuleID string      `json:"rule_id"`
	Count  CountBucket `json:"count"`
}

type CohortAudit struct {
	ResourceIdentity string
	Eligible         CountBucket
	Retained         CountBucket
	Truncated        bool
}

type AuditSnapshot struct {
	Rules   []RuleAudit
	Cohorts []CohortAudit
}

// Audit retains exact counts only in memory and releases disclosure-bounded
// snapshots for persistence or display.
type Audit struct {
	mu      sync.Mutex
	counts  map[string]int
	cohorts map[string]cohortCounts
}

type cohortCounts struct {
	eligible  int
	retained  int
	truncated bool
}

func NewAudit() *Audit {
	return &Audit{counts: make(map[string]int), cohorts: make(map[string]cohortCounts)}
}

func (a *Audit) Record(ruleID string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts[ruleID]++
}

func (a *Audit) RecordCohort(resourceIdentity string, eligible, retained int, truncated bool) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cohorts[resourceIdentity] = cohortCounts{eligible: eligible, retained: retained, truncated: truncated}
}

// RestoreRuleCounts restores exact counters from a protected, policy-bound
// capture checkpoint. Exact values remain internal and are bucketed by Result.
func (a *Audit) RestoreRuleCounts(counts map[string]int) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for ruleID, count := range counts {
		if count > 0 {
			a.counts[ruleID] = count
		}
	}
}

func (a *Audit) RuleCounts() map[string]int {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	result := make(map[string]int, len(a.counts))
	for ruleID, count := range a.counts {
		result[ruleID] = count
	}
	return result
}

func (a *Audit) Snapshot() []RuleAudit { return a.Result().Rules }

func (a *Audit) Result() AuditSnapshot {
	if a == nil {
		return AuditSnapshot{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	ruleIDs := make([]string, 0, len(a.counts))
	for ruleID := range a.counts {
		ruleIDs = append(ruleIDs, ruleID)
	}
	sort.Strings(ruleIDs)
	result := AuditSnapshot{Rules: make([]RuleAudit, 0, len(ruleIDs))}
	for _, ruleID := range ruleIDs {
		result.Rules = append(result.Rules, RuleAudit{RuleID: ruleID, Count: BucketCount(a.counts[ruleID])})
	}
	identities := make([]string, 0, len(a.cohorts))
	for identity := range a.cohorts {
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	for _, identity := range identities {
		counts := a.cohorts[identity]
		result.Cohorts = append(result.Cohorts, CohortAudit{ResourceIdentity: identity, Eligible: BucketCount(counts.eligible), Retained: BucketCount(counts.retained), Truncated: counts.truncated})
	}
	return result
}
