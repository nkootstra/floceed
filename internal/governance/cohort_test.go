package governance

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"testing"
)

func TestCohortRankSelectsTheSameMembersRegardlessOfScanOrder(t *testing.T) {
	secret := bytes.Repeat([]byte{0x42}, 32)
	cohort := Cohort{Resource: "orders", KeyID: "cohort-2026", Scope: "project", Algorithm: CohortRankAlgorithm, KeyPaths: []string{"tenant", "id"}, Limit: 2}
	ranker, err := NewCohortRanker("developer-safe", secret, cohort)
	if err != nil {
		t.Fatal(err)
	}

	forward := []struct {
		keyValues [][]byte
		value     []byte
	}{
		{keyValues: [][]byte{[]byte("a"), []byte("1")}, value: []byte("a/1")},
		{keyValues: [][]byte{[]byte("a"), []byte("2")}, value: []byte("a/2")},
		{keyValues: [][]byte{[]byte("b"), []byte("1")}, value: []byte("b/1")},
	}
	reverse := []int{2, 1, 0}

	forwardSelection := ranker.NewSelection(nil)
	for _, candidate := range forward {
		forwardSelection.Offer(candidate.keyValues, candidate.value)
	}
	reverseSelection := ranker.NewSelection(nil)
	for _, index := range reverse {
		candidate := forward[index]
		reverseSelection.Offer(candidate.keyValues, candidate.value)
	}
	gotForward := forwardSelection.Values()
	gotReverse := reverseSelection.Values()
	sort.Slice(gotForward, func(i, j int) bool { return string(gotForward[i]) < string(gotForward[j]) })
	sort.Slice(gotReverse, func(i, j int) bool { return string(gotReverse[i]) < string(gotReverse[j]) })
	if !bytes.Equal(bytes.Join(gotForward, nil), bytes.Join(gotReverse, nil)) {
		t.Fatalf("forward = %q, reverse = %q", gotForward, gotReverse)
	}
}

func TestCohortSelectionRejectsRetainedBytesAboveConfiguredCeiling(t *testing.T) {
	ranker, err := NewCohortRanker("safe", bytes.Repeat([]byte{1}, 32), Cohort{Resource: "orders", KeyID: "key-1", Algorithm: CohortRankAlgorithm, Limit: 2, MaxRetainedBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	selection := ranker.NewSelection(nil)
	if err := selection.OfferChecked([][]byte{[]byte("a")}, bytes.Repeat([]byte("x"), 33)); !errors.Is(err, ErrCohortRetainedBytes) {
		t.Fatalf("offer error = %v", err)
	}
}

func TestCohortSelectionRetainsOnlyTheConfiguredLimit(t *testing.T) {
	ranker, err := NewCohortRanker("developer-safe", bytes.Repeat([]byte{0x42}, 32), Cohort{
		Resource: "orders", KeyID: "cohort-2026", Algorithm: CohortRankAlgorithm, Limit: 17,
	})
	if err != nil {
		t.Fatal(err)
	}
	selection := ranker.NewSelection(nil)
	for i := 0; i < 100_000; i++ {
		value := []byte(fmt.Sprintf("%09d", i))
		selection.Offer([][]byte{value}, value)
		if got := selection.Len(); got > 17 {
			t.Fatalf("retained candidates = %d, want at most 17", got)
		}
	}
	if got := len(selection.State()); got != 17 {
		t.Fatalf("checkpoint candidates = %d, want 17", got)
	}
}

func TestCohortSelectionResumeProducesTheSameCanonicalValues(t *testing.T) {
	ranker, err := NewCohortRanker("developer-safe", bytes.Repeat([]byte{0x42}, 32), Cohort{
		Resource: "orders", KeyID: "cohort-2026", Algorithm: CohortRankAlgorithm, Limit: 11,
	})
	if err != nil {
		t.Fatal(err)
	}
	continuous := ranker.NewSelection(nil)
	interrupted := ranker.NewSelection(nil)
	for i := 999; i >= 0; i-- {
		value := []byte(fmt.Sprintf("%04d", i))
		continuous.Offer([][]byte{value}, value)
		if i >= 500 {
			interrupted.Offer([][]byte{value}, value)
		}
	}
	resumed := ranker.NewSelection(interrupted.State())
	for i := 499; i >= 0; i-- {
		value := []byte(fmt.Sprintf("%04d", i))
		resumed.Offer([][]byte{value}, value)
	}
	if !bytes.Equal(bytes.Join(continuous.Values(), nil), bytes.Join(resumed.Values(), nil)) {
		t.Fatalf("resumed selection differs from uninterrupted selection")
	}
}
