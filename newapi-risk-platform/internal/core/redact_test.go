package core

import (
	"strings"
	"testing"
)

func TestRedactJSON(t *testing.T) {
	got := string(RedactJSON([]byte(`{"api_key":"secret","nested":{"password":"p"},"prompt":"hello"}`), 1024))
	if strings.Contains(got, "secret") || strings.Contains(got, `"p"`) {
		t.Fatalf("secret leaked: %s", got)
	}
	if !strings.Contains(got, "hello") {
		t.Fatalf("non-sensitive content lost: %s", got)
	}
}

func TestCipherRoundTrip(t *testing.T) {
	c, err := NewCipher("test-master-key")
	if err != nil {
		t.Fatal(err)
	}
	enc, err := c.Encrypt([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := c.Decrypt(enc)
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != "hello" {
		t.Fatalf("unexpected plaintext: %q", plain)
	}
}
