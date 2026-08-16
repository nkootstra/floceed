package governance

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/crypto/hkdf"
)

var ErrInvalidTransformation = errors.New("invalid governance transformation")

// Engine applies compiled governance rules to canonical source bytes.
type Engine struct {
	profile string
	secret  []byte
	mu      sync.RWMutex
	keys    map[string][]byte
}

// Result is the transformed representation of a source value.
type Result struct {
	Value []byte
	Omit  bool
}

func NewEngine(profile string, secret []byte) *Engine {
	return &Engine{profile: profile, secret: append([]byte(nil), secret...), keys: make(map[string][]byte)}
}

func (e *Engine) Apply(rule Rule, source []byte) (Result, error) {
	var output bytes.Buffer
	omit, err := e.ApplyReader(rule, bytes.NewReader(source), &output)
	if err != nil {
		return Result{}, err
	}
	return Result{Value: output.Bytes(), Omit: omit}, nil
}

// ApplyReader transforms a source stream directly into destination. Hash and
// pseudonym actions consume the source incrementally; replace and omit never
// retain or copy source bytes.
func (e *Engine) ApplyReader(rule Rule, source io.Reader, destination io.Writer) (bool, error) {
	switch rule.Action {
	case ActionOmit:
		return true, nil
	case ActionReplace:
		_, err := io.WriteString(destination, rule.Replacement)
		return false, transformationIOError(err)
	case ActionHash:
		digest := sha256.New()
		if _, err := io.Copy(digest, source); err != nil {
			return false, transformationIOError(err)
		}
		_, err := io.WriteString(destination, HashAlgorithm+":"+hex.EncodeToString(digest.Sum(nil)))
		return false, transformationIOError(err)
	case ActionPseudonymize:
		if len(e.secret) < 32 || rule.Algorithm != "" && rule.Algorithm != PseudonymAlgorithm {
			return false, ErrInvalidTransformation
		}
		key, err := e.pseudonymKey(rule)
		if err != nil {
			return false, ErrInvalidTransformation
		}
		mac := hmac.New(sha256.New, key)
		if _, err := io.Copy(mac, source); err != nil {
			return false, transformationIOError(err)
		}
		_, err = io.WriteString(destination, PseudonymAlgorithm+":"+hex.EncodeToString(mac.Sum(nil)))
		return false, transformationIOError(err)
	default:
		return false, ErrInvalidTransformation
	}
}

func (e *Engine) pseudonymKey(rule Rule) ([]byte, error) {
	cacheKey := string(encodeDomain([]string{rule.KeyID, rule.Scope, string(rule.Service), rule.Resource, rule.ID, string(rule.Target.Kind), rule.Target.Path}))
	e.mu.RLock()
	key := e.keys[cacheKey]
	e.mu.RUnlock()
	if key != nil {
		return key, nil
	}
	derived, err := deriveKey(e.secret, PseudonymAlgorithm, rule.KeyID, e.profile, rule)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	if key = e.keys[cacheKey]; key == nil {
		e.keys[cacheKey] = derived
		key = derived
	}
	e.mu.Unlock()
	return key, nil
}

func transformationIOError(err error) error {
	if err == nil {
		return nil
	}
	return ErrInvalidTransformation
}

func deriveKey(secret []byte, purpose, keyID, profile string, rule Rule) ([]byte, error) {
	fields := []string{purpose, keyID, rule.Scope, profile, string(rule.Service), rule.Resource, rule.ID, string(rule.Target.Kind), rule.Target.Path}
	for _, field := range fields {
		if strings.IndexByte(field, 0) >= 0 {
			return nil, ErrInvalidTransformation
		}
	}
	salt := []byte("floceed/governance/hkdf-sha256/v1\x00" + keyID)
	reader := hkdf.New(func() hash.Hash { return sha256.New() }, secret, salt, encodeDomain(fields))
	key := make([]byte, sha256.Size)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, err
	}
	return key, nil
}

func encodeDomain(fields []string) []byte {
	var encoded strings.Builder
	encoded.WriteString("floceed/governance/domain/v1")
	for _, field := range fields {
		encoded.WriteByte(0)
		encoded.WriteString(strconv.Itoa(len(field)))
		encoded.WriteByte(':')
		encoded.WriteString(field)
	}
	return []byte(encoded.String())
}
