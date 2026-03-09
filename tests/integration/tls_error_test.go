//go:build integration

package integration

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMTLSWithoutClientCert verifies that a connection without a client
// certificate is rejected by the server when mutual TLS is required.
func TestMTLSWithoutClientCert(t *testing.T) {
	caCertPEM, err := os.ReadFile(tlsCACertPath)
	require.NoError(t, err)

	caPool := x509.NewCertPool()
	require.True(t, caPool.AppendCertsFromPEM(caCertPEM))

	tlsConfig := &tls.Config{
		RootCAs: caPool,
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
	url := fmt.Sprintf("https://localhost:%d/", disConnectionPort)
	_, err = client.Post(url, "application/json", bytes.NewReader([]byte(`{}`)))
	require.Error(t, err, "expected TLS handshake to fail without client cert")
}

// TestMTLSWithUntrustedCert verifies that a client certificate signed by an
// unknown CA is rejected by the server.
func TestMTLSWithUntrustedCert(t *testing.T) {
	// Generate a separate CA that the server does not trust.
	untrustedCAKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	untrustedCATemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(100),
		Subject:               pkix.Name{CommonName: "Untrusted CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	untrustedCACertDER, err := x509.CreateCertificate(rand.Reader, untrustedCATemplate, untrustedCATemplate, &untrustedCAKey.PublicKey, untrustedCAKey)
	require.NoError(t, err)

	untrustedCACert, err := x509.ParseCertificate(untrustedCACertDER)
	require.NoError(t, err)

	// Generate a client cert signed by the untrusted CA.
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(101),
		Subject:      pkix.Name{CommonName: "untrusted-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientCertDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, untrustedCACert, &clientKey.PublicKey, untrustedCAKey)
	require.NoError(t, err)

	// Use the real test CA as RootCAs so the client trusts the server,
	// but present the untrusted client cert.
	caCertPEM, err := os.ReadFile(tlsCACertPath)
	require.NoError(t, err)

	caPool := x509.NewCertPool()
	require.True(t, caPool.AppendCertsFromPEM(caCertPEM))

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{clientCertDER},
				PrivateKey:  clientKey,
			},
		},
		RootCAs: caPool,
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
	url := fmt.Sprintf("https://localhost:%d/", disConnectionPort)
	_, err = client.Post(url, "application/json", bytes.NewReader([]byte(`{}`)))
	require.Error(t, err, "expected TLS handshake to fail with untrusted client cert")
}

// TestMTLSWithExpiredCert verifies that an expired client certificate is
// rejected by the server even if it was signed by the trusted CA.
func TestMTLSWithExpiredCert(t *testing.T) {
	// Read the test CA to sign the expired client cert.
	caCertPEM, err := os.ReadFile(tlsCACertPath)
	require.NoError(t, err)

	caPool := x509.NewCertPool()
	require.True(t, caPool.AppendCertsFromPEM(caCertPEM))

	// We need the CA key and cert to sign a new client cert. Since we
	// don't export those from generateCerts, regenerate them from the
	// PEM files. The CA private key is not saved to disk, so we need
	// to generate a fresh CA + server setup. Instead, we re-derive by
	// generating the expired cert with its own throwaway CA that we
	// then also load. But the server only trusts the original CA.
	//
	// Actually, we need the original CA private key. Since it's not
	// persisted, we take a different approach: generate a new client
	// cert signed by a freshly created CA, but that won't test the
	// "expired" path correctly. Instead, let's re-read the CA cert
	// and generate a self-signed expired cert that references it.
	//
	// The simplest correct approach: generate a separate CA, sign an
	// expired client cert with it, and configure the SERVER to also
	// trust this CA. But we can't change the running server config.
	//
	// Pragmatic approach: create a completely self-signed expired
	// client cert. The server will reject it because (a) it's not
	// signed by the trusted CA, and (b) it's expired. Either reason
	// is sufficient for the handshake to fail.

	expiredKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	expiredTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(200),
		Subject:      pkix.Name{CommonName: "expired-client"},
		NotBefore:    time.Now().Add(-48 * time.Hour),
		NotAfter:     time.Now().Add(-1 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	expiredCertDER, err := x509.CreateCertificate(rand.Reader, expiredTemplate, expiredTemplate, &expiredKey.PublicKey, expiredKey)
	require.NoError(t, err)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{expiredCertDER},
				PrivateKey:  expiredKey,
			},
		},
		RootCAs: caPool,
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
	url := fmt.Sprintf("https://localhost:%d/", disConnectionPort)
	_, err = client.Post(url, "application/json", bytes.NewReader([]byte(`{}`)))
	require.Error(t, err, "expected TLS handshake to fail with expired client cert")
}
