//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
)

// TestTombstoneEndpoint posts an attestation, then a tombstone that voids it,
// and asserts both rows exist in ClickHouse with the tombstone row's voids_id
// pointing at the attestation's id.
func TestTombstoneEndpoint(t *testing.T) {
	clearMinIOObjects(t, "cloudevent/valid/")

	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:556"
	clearClickHouseForSubject(t, subject)

	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	ethAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	// ── 1. Post an attestation ─────────────────────────────────────────
	attestationData := map[string]any{
		"subject":  subject,
		"insured":  true,
		"provider": "Test Insurance",
	}
	attDataBytes, err := json.Marshal(attestationData)
	require.NoError(t, err)

	attHash := accounts.TextHash(attDataBytes)
	attSig, err := crypto.Sign(attHash, privateKey)
	require.NoError(t, err)
	attSig[64] += 27

	const attestationID = "test-attestation-tombstone-target"
	attPayload := map[string]any{
		"id":          attestationID,
		"subject":     subject,
		"source":      ethAddr.Hex(),
		"producer":    ethAddr.Hex(),
		"specversion": "1.0",
		"time":        time.Now().UTC().Format(time.RFC3339),
		"type":        "dimo.attestation",
		"signature":   "0x" + common.Bytes2Hex(attSig),
		"data":        attestationData,
	}
	attBytes, err := json.Marshal(attPayload)
	require.NoError(t, err)
	t.Logf("Attestation payload: %s", string(attBytes))

	resp := postJWTAttestation(t, attBytes, ethAddr)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode, "attestation POST should return 200")

	// ── 2. Post a tombstone for that attestation ────────────────────────
	tombstoneData := map[string]string{
		"voidsId": attestationID,
		"reason":  "uploaded by mistake",
	}
	tdBytes, err := json.Marshal(tombstoneData)
	require.NoError(t, err)

	tdHash := accounts.TextHash(tdBytes)
	tdSig, err := crypto.Sign(tdHash, privateKey)
	require.NoError(t, err)
	tdSig[64] += 27

	const tombstoneID = "test-tombstone-001"
	tdPayload := map[string]any{
		"id":          tombstoneID,
		"subject":     subject,
		"source":      ethAddr.Hex(),
		"producer":    ethAddr.Hex(),
		"specversion": "1.0",
		"time":        time.Now().UTC().Format(time.RFC3339),
		"type":        "dimo.tombstone",
		"signature":   "0x" + common.Bytes2Hex(tdSig),
		"data":        tombstoneData,
	}
	tdPayloadBytes, err := json.Marshal(tdPayload)
	require.NoError(t, err)
	t.Logf("Tombstone payload: %s", string(tdPayloadBytes))

	resp = postJWTAttestation(t, tdPayloadBytes, ethAddr)
	drainAndClose(t, resp)
	require.Equal(t, 200, resp.StatusCode, "tombstone POST should return 200")

	// Wait for parquet batch flush + ClickHouse insert.
	time.Sleep(2 * time.Second)

	// ── 3. Both rows should exist in cloud_event ─────────────────────
	rows := queryCloudEvents(t, subject)
	require.Len(t, rows, 2, "expected attestation + tombstone rows")

	var attRow, tdRow *CloudEventRow
	for i := range rows {
		switch rows[i].EventType {
		case "dimo.attestation":
			attRow = &rows[i]
		case "dimo.tombstone":
			tdRow = &rows[i]
		}
	}
	require.NotNil(t, attRow, "attestation row missing")
	require.NotNil(t, tdRow, "tombstone row missing")

	require.Equal(t, attestationID, attRow.ID)
	require.Empty(t, attRow.VoidsID, "non-tombstone events should have empty voids_id")

	require.Equal(t, tombstoneID, tdRow.ID)
	require.Equal(t, ethAddr.Hex(), tdRow.Source)
	require.Equal(t, attestationID, tdRow.VoidsID, "tombstone voids_id should equal target attestation id")
}

// TestTombstoneEndpoint_RejectsEmptyVoidsID confirms a tombstone with an
// empty voidsId in its data payload is rejected at the endpoint.
func TestTombstoneEndpoint_RejectsEmptyVoidsID(t *testing.T) {
	subject := "did:erc721:137:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF:557"

	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	ethAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	tombstoneData := map[string]string{"voidsId": ""}
	tdBytes, err := json.Marshal(tombstoneData)
	require.NoError(t, err)

	tdHash := accounts.TextHash(tdBytes)
	tdSig, err := crypto.Sign(tdHash, privateKey)
	require.NoError(t, err)
	tdSig[64] += 27

	tdPayload := map[string]any{
		"id":          "test-tombstone-empty-target",
		"subject":     subject,
		"source":      ethAddr.Hex(),
		"producer":    ethAddr.Hex(),
		"specversion": "1.0",
		"time":        time.Now().UTC().Format(time.RFC3339),
		"type":        "dimo.tombstone",
		"signature":   "0x" + common.Bytes2Hex(tdSig),
		"data":        tombstoneData,
	}
	tdPayloadBytes, err := json.Marshal(tdPayload)
	require.NoError(t, err)

	resp := postJWTAttestation(t, tdPayloadBytes, ethAddr)
	drainAndClose(t, resp)
	require.NotEqual(t, 200, resp.StatusCode, "tombstone with empty voidsId should be rejected")
}
