//go:build integration

package integration

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

// postMTLS sends a payload to the mTLS connection endpoint using the default test client cert.
func postMTLS(t *testing.T, payload []byte) *http.Response {
	t.Helper()
	return postMTLSWithConfig(t, payload, clientTLSConfig)
}

// postMTLSWithConfig sends a payload to the mTLS connection endpoint using the given TLS config.
func postMTLSWithConfig(t *testing.T, payload []byte, tlsConfig *tls.Config) *http.Response {
	t.Helper()
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
	url := fmt.Sprintf("https://localhost:%d/", disConnectionPort)
	resp, err := client.Post(url, "application/json", bytes.NewReader(payload))
	require.NoError(t, err, "POST to mTLS endpoint failed")
	return resp
}

// postJWTAttestation sends an attestation payload to the JWT endpoint.
func postJWTAttestation(t *testing.T, payload []byte, ethAddr common.Address) *http.Response {
	t.Helper()
	token, err := createJWT(ethAddr)
	require.NoError(t, err, "failed to create JWT")

	url := fmt.Sprintf("http://localhost:%d/", disAttestationPort)
	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "POST to JWT endpoint failed")
	return resp
}

// drainAndClose reads and discards the response body, then closes it.
func drainAndClose(t *testing.T, resp *http.Response) {
	t.Helper()
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}
