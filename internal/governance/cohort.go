package governance

import (
	"bytes"
	"container/heap"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"sort"
	"strings"

	"golang.org/x/crypto/hkdf"
)

var ErrInvalidCohort = errors.New("invalid governance cohort")
var ErrCohortRetainedBytes = errors.New("cohort retained byte ceiling exceeded")

const DefaultCohortMaxRetainedBytes int64 = 64 << 20

type CohortRanker struct {
	key              []byte
	limit            int
	maxRetainedBytes int64
}

// CohortSelectionState is the bounded, durable state needed to resume cohort
// selection. Values have already passed governance transformations.
type CohortSelectionState struct {
	Rank  []byte `json:"rank"`
	Value []byte `json:"value"`
}

// CohortSelection retains the best candidates in a max-heap, so offering a
// candidate costs O(log(limit)) while memory remains O(limit).
type CohortSelection struct {
	ranker        *CohortRanker
	items         cohortMaxHeap
	retainedBytes int64
}

type cohortMaxHeap []CohortSelectionState

func (h cohortMaxHeap) Len() int      { return len(h) }
func (h cohortMaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h cohortMaxHeap) Less(i, j int) bool {
	return compareCohortState(h[i], h[j]) > 0
}
func (h *cohortMaxHeap) Push(value any) { *h = append(*h, value.(CohortSelectionState)) }
func (h *cohortMaxHeap) Pop() any {
	old := *h
	value := old[len(old)-1]
	*h = old[:len(old)-1]
	return value
}

func compareCohortState(a, b CohortSelectionState) int {
	if cmp := bytes.Compare(a.Rank, b.Rank); cmp != 0 {
		return cmp
	}
	return bytes.Compare(a.Value, b.Value)
}

func (r *CohortRanker) NewSelection(state []CohortSelectionState) *CohortSelection {
	s := &CohortSelection{ranker: r}
	for _, candidate := range state {
		s.offerRanked(candidate.Rank, candidate.Value)
		s.retainedBytes += int64(len(candidate.Rank) + len(candidate.Value))
	}
	return s
}

func (s *CohortSelection) Offer(keyValues [][]byte, value []byte) {
	_ = s.OfferChecked(keyValues, value)
}

func (s *CohortSelection) OfferChecked(keyValues [][]byte, value []byte) error {
	rank := s.ranker.Rank(keyValues)
	candidateBytes := int64(len(rank) + len(value))
	if len(s.items) < s.ranker.limit {
		if s.retainedBytes+candidateBytes > s.ranker.maxRetainedBytes {
			return ErrCohortRetainedBytes
		}
		s.offerRanked(rank, value)
		s.retainedBytes += candidateBytes
		return nil
	}
	if compareCohortState(CohortSelectionState{Rank: rank, Value: value}, s.items[0]) < 0 {
		next := s.retainedBytes - int64(len(s.items[0].Rank)+len(s.items[0].Value)) + candidateBytes
		if next > s.ranker.maxRetainedBytes {
			return ErrCohortRetainedBytes
		}
		s.offerRanked(rank, value)
		s.retainedBytes = next
	}
	return nil
}

func (s *CohortSelection) offerRanked(rank, value []byte) {
	candidate := CohortSelectionState{Rank: append([]byte(nil), rank...), Value: append([]byte(nil), value...)}
	if len(s.items) < s.ranker.limit {
		heap.Push(&s.items, candidate)
		return
	}
	if compareCohortState(candidate, s.items[0]) < 0 {
		s.items[0] = candidate
		heap.Fix(&s.items, 0)
	}
}

func (s *CohortSelection) Len() int { return len(s.items) }

func (s *CohortSelection) State() []CohortSelectionState {
	state := make([]CohortSelectionState, len(s.items))
	for i, candidate := range s.items {
		state[i] = CohortSelectionState{Rank: append([]byte(nil), candidate.Rank...), Value: append([]byte(nil), candidate.Value...)}
	}
	return state
}

// Visit exposes the retained state without copying it, for streaming protected
// checkpoint serialization. Callers must not retain or mutate the slices.
func (s *CohortSelection) Visit(visit func(rank, value []byte) error) error {
	for i := range s.items {
		if err := visit(s.items[i].Rank, s.items[i].Value); err != nil {
			return err
		}
	}
	return nil
}

// RestoreOwned restores authenticated checkpoint bytes and takes ownership of
// both slices, avoiding a second heap-sized copy during resume.
func (s *CohortSelection) RestoreOwned(rank, value []byte) error {
	if len(rank) != sha256.Size {
		return ErrInvalidCohort
	}
	n := int64(len(rank) + len(value))
	if len(s.items) >= s.ranker.limit || s.retainedBytes+n > s.ranker.maxRetainedBytes {
		return ErrInvalidCohort
	}
	heap.Push(&s.items, CohortSelectionState{Rank: rank, Value: value})
	s.retainedBytes += n
	return nil
}

func (s *CohortSelection) Values() [][]byte {
	values := make([][]byte, len(s.items))
	for i, candidate := range s.items {
		values[i] = append([]byte(nil), candidate.Value...)
	}
	sort.Slice(values, func(i, j int) bool { return bytes.Compare(values[i], values[j]) < 0 })
	return values
}

func NewCohortRanker(profile string, secret []byte, cohort Cohort) (*CohortRanker, error) {
	if len(secret) < 32 || cohort.Algorithm != CohortRankAlgorithm || cohort.KeyID == "" || cohort.Limit <= 0 {
		return nil, ErrInvalidCohort
	}
	fields := []string{CohortRankAlgorithm, cohort.KeyID, cohort.Scope, profile, string(ServiceDynamoDB), cohort.Resource}
	for _, field := range fields {
		if strings.IndexByte(field, 0) >= 0 {
			return nil, ErrInvalidCohort
		}
	}
	reader := hkdf.New(func() hash.Hash { return sha256.New() }, secret, []byte("floceed/governance/hkdf-sha256/v1\x00"+cohort.KeyID), encodeDomain(fields))
	key := make([]byte, sha256.Size)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, ErrInvalidCohort
	}
	maxBytes := cohort.MaxRetainedBytes
	if maxBytes == 0 {
		maxBytes = DefaultCohortMaxRetainedBytes
	}
	if maxBytes < sha256.Size {
		return nil, ErrInvalidCohort
	}
	return &CohortRanker{key: key, limit: cohort.Limit, maxRetainedBytes: maxBytes}, nil
}

func (r *CohortRanker) Rank(keyValues [][]byte) []byte {
	mac := hmac.New(sha256.New, r.key)
	for _, value := range keyValues {
		_, _ = mac.Write(encodeLength(value))
		_, _ = mac.Write(value)
	}
	return mac.Sum(nil)
}

func encodeLength(value []byte) []byte {
	var out [8]byte
	binary.BigEndian.PutUint64(out[:], uint64(len(value)))
	return out[:]
}
