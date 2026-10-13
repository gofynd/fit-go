package mongo

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLegacyTLSKeepsConfigWhenCAHasNoCertificates(t *testing.T) {
	clearMongoTLSEnv(t)
	caPath, certPath, keyPath := writeMongoTLSMaterial(t)
	t.Setenv("MONGO_TEST_SSL_CA", caPath)
	t.Setenv("MONGO_TEST_SSL_CERT", certPath)
	t.Setenv("MONGO_TEST_SSL_KEY", keyPath)

	config, err := loadTLSConfig("MONGO", "TEST")
	if err == nil || config == nil || config.RootCAs == nil {
		t.Fatalf("strict loader = (%#v, %v), want usable config plus CA parse error", config, err)
	}
	legacy, err := buildDialOptions("TEST", "write", "test", ConnectionOptions{}, false, false, false)
	if err != nil || legacy.TLSConfig == nil {
		t.Fatalf("legacy TLS = (%#v, %v), want original-main fail-closed config", legacy, err)
	}
	if _, err := buildDialOptions("TEST", "write", "test", ConnectionOptions{}, false, false, true); err == nil {
		t.Fatal("strict TLS validation accepted an invalid CA")
	}
}

func writeMongoTLSMaterial(t *testing.T) (string, string, string) {
	t.Helper()
	directory := t.TempDir()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fit-go-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(directory, "ca.pem")
	certPath := filepath.Join(directory, "cert.pem")
	keyPath := filepath.Join(directory, "key.pem")
	for path, contents := range map[string][]byte{
		caPath:   []byte("not a certificate"),
		certPath: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER}),
		keyPath:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	} {
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return caPath, certPath, keyPath
}
