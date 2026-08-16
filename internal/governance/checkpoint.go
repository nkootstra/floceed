package governance

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"strconv"

	"golang.org/x/crypto/hkdf"
)

var ErrCheckpointProtection = errors.New("governance checkpoint protection failed")

// ProtectCheckpoint seals opaque resume state using a key separated from the
// pseudonym and cohort domains. Binding the policy and capture identities as
// authenticated data prevents ciphertext reuse across captures.
func (p *EffectivePolicy) ProtectCheckpoint(captureIdentity string, plaintext []byte) ([]byte, error) {
	aead, err := p.checkpointAEAD(captureIdentity)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, ErrCheckpointProtection
	}
	return aead.Seal(nonce, nonce, plaintext, []byte(captureIdentity)), nil
}

func (p *EffectivePolicy) UnprotectCheckpoint(captureIdentity string, protected []byte) ([]byte, error) {
	aead, err := p.checkpointAEAD(captureIdentity)
	if err != nil {
		return nil, err
	}
	if len(protected) < aead.NonceSize() {
		return nil, ErrCheckpointProtection
	}
	plaintext, err := aead.Open(nil, protected[:aead.NonceSize()], protected[aead.NonceSize():], []byte(captureIdentity))
	if err != nil {
		return nil, ErrCheckpointProtection
	}
	return plaintext, nil
}

// ProtectCheckpointRecord seals one record in a checkpoint state stream. The
// record kind and ordinal are authenticated so records cannot be substituted,
// reordered, or moved between captures.
func (p *EffectivePolicy) ProtectCheckpointRecord(captureIdentity, kind string, ordinal uint64, plaintext []byte) ([]byte, error) {
	return p.protectCheckpoint(captureIdentity, checkpointRecordIdentity(captureIdentity, kind, ordinal), plaintext)
}

// UnprotectCheckpointRecord opens a record sealed by ProtectCheckpointRecord.
func (p *EffectivePolicy) UnprotectCheckpointRecord(captureIdentity, kind string, ordinal uint64, protected []byte) ([]byte, error) {
	return p.unprotectCheckpoint(captureIdentity, checkpointRecordIdentity(captureIdentity, kind, ordinal), protected)
}

func checkpointRecordIdentity(captureIdentity, kind string, ordinal uint64) string {
	return captureIdentity + "\x00record\x00" + kind + "\x00" + strconv.FormatUint(ordinal, 10)
}

func (p *EffectivePolicy) protectCheckpoint(captureIdentity, authenticatedIdentity string, plaintext []byte) ([]byte, error) {
	aead, err := p.checkpointAEAD(captureIdentity)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, ErrCheckpointProtection
	}
	return aead.Seal(nonce, nonce, plaintext, []byte(authenticatedIdentity)), nil
}

func (p *EffectivePolicy) unprotectCheckpoint(captureIdentity, authenticatedIdentity string, protected []byte) ([]byte, error) {
	aead, err := p.checkpointAEAD(captureIdentity)
	if err != nil || len(protected) < aead.NonceSize() {
		return nil, ErrCheckpointProtection
	}
	plaintext, err := aead.Open(nil, protected[:aead.NonceSize()], protected[aead.NonceSize():], []byte(authenticatedIdentity))
	if err != nil {
		return nil, ErrCheckpointProtection
	}
	return plaintext, nil
}

func (p *EffectivePolicy) checkpointAEAD(captureIdentity string) (cipher.AEAD, error) {
	if p == nil || len(p.secret) < 32 || p.Identity == "" || captureIdentity == "" {
		return nil, ErrCheckpointProtection
	}
	reader := hkdf.New(func() hash.Hash { return sha256.New() }, p.secret, []byte("floceed/governance/checkpoint-aead/v1\x00"+p.Identity), []byte(captureIdentity))
	key := make([]byte, 32)
	if _, err := io.ReadFull(reader, key); err != nil {
		return nil, ErrCheckpointProtection
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrCheckpointProtection
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrCheckpointProtection
	}
	return aead, nil
}
