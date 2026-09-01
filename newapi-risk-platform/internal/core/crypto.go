package core

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
)

type Cipher struct {
	aead cipher.AEAD
}

func NewCipher(masterKey string) (*Cipher, error) {
	if masterKey == "" {
		return nil, fmt.Errorf("master key is empty")
	}
	key := sha256.Sum256([]byte(masterKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Cipher{aead: aead}, nil
}

func (c *Cipher) Encrypt(plain []byte) (string, error) {
	if c == nil || c.aead == nil {
		return "", fmt.Errorf("cipher is not configured")
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	sealed := c.aead.Seal(nonce, nonce, plain, nil)
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

func (c *Cipher) Decrypt(encoded string) ([]byte, error) {
	if c == nil || c.aead == nil {
		return nil, fmt.Errorf("cipher is not configured")
	}
	sealed, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	nonceSize := c.aead.NonceSize()
	if len(sealed) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	return c.aead.Open(nil, sealed[:nonceSize], sealed[nonceSize:], nil)
}
