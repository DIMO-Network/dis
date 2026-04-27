package cloudeventconvert

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	checksumAddr = "0x06012c8cf97BEaD5deAe237070F9587f8E7A266d"
	lowerAddr    = "0x06012c8cf97bead5deae237070f9587f8e7a266d"
)

func TestIsValidConnectionHeader_ChecksumsAddresses(t *testing.T) {
	logger := service.MockResources().Logger()

	tests := []struct {
		name             string
		hdr              cloudevent.CloudEventHeader
		expectedSubject  string
		expectedProducer string
		expectedSource   string
	}{
		{
			name: "lowercased erc721 DID is rewritten to checksum",
			hdr: cloudevent.CloudEventHeader{
				Subject:  "did:erc721:1:" + lowerAddr + ":2",
				Producer: "did:erc721:1:" + lowerAddr + ":1",
				Source:   lowerAddr,
			},
			expectedSubject:  "did:erc721:1:" + checksumAddr + ":2",
			expectedProducer: "did:erc721:1:" + checksumAddr + ":1",
			expectedSource:   checksumAddr,
		},
		{
			name: "already-checksummed erc721 DID is unchanged",
			hdr: cloudevent.CloudEventHeader{
				Subject:  "did:erc721:1:" + checksumAddr + ":2",
				Producer: "did:erc721:1:" + checksumAddr + ":1",
				Source:   checksumAddr,
			},
			expectedSubject:  "did:erc721:1:" + checksumAddr + ":2",
			expectedProducer: "did:erc721:1:" + checksumAddr + ":1",
			expectedSource:   checksumAddr,
		},
		{
			name: "legacy nft DID with lowercased address is rewritten to checksummed erc721",
			hdr: cloudevent.CloudEventHeader{
				Subject:  "did:nft:1:" + lowerAddr + "_2",
				Producer: "did:nft:1:" + lowerAddr + "_1",
				Source:   lowerAddr,
			},
			expectedSubject:  "did:erc721:1:" + checksumAddr + ":2",
			expectedProducer: "did:erc721:1:" + checksumAddr + ":1",
			expectedSource:   checksumAddr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hdr := tt.hdr
			require.True(t, isValidConnectionHeader(&hdr, logger))
			assert.Equal(t, tt.expectedSubject, hdr.Subject)
			assert.Equal(t, tt.expectedProducer, hdr.Producer)
			assert.Equal(t, tt.expectedSource, hdr.Source)
		})
	}
}

func TestParseAndValidateAttestation_ChecksumsSubjectAndSource(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name            string
		event           cloudevent.CloudEventHeader
		callerSource    string
		expectedSubject string
		expectedSource  string
	}{
		{
			name: "lowercased erc721 subject is rewritten to checksum",
			event: cloudevent.CloudEventHeader{
				ID:          "id-1",
				SpecVersion: "1.0",
				Source:      lowerAddr,
				Producer:    lowerAddr,
				Subject:     "did:erc721:1:" + lowerAddr + ":1005",
				Type:        cloudevent.TypeAttestation,
				Time:        now,
			},
			callerSource:    lowerAddr,
			expectedSubject: "did:erc721:1:" + checksumAddr + ":1005",
			expectedSource:  checksumAddr,
		},
		{
			name: "lowercased ethr subject is rewritten to checksum",
			event: cloudevent.CloudEventHeader{
				ID:          "id-2",
				SpecVersion: "1.0",
				Source:      lowerAddr,
				Producer:    lowerAddr,
				Subject:     "did:ethr:1:" + lowerAddr,
				Type:        cloudevent.TypeAttestation,
				Time:        now,
			},
			callerSource:    lowerAddr,
			expectedSubject: "did:ethr:1:" + checksumAddr,
			expectedSource:  checksumAddr,
		},
		{
			name: "lowercased caller source falls back when payload source is empty",
			event: cloudevent.CloudEventHeader{
				ID:          "id-3",
				SpecVersion: "1.0",
				Producer:    lowerAddr,
				Subject:     "did:erc721:1:" + lowerAddr + ":1",
				Type:        cloudevent.TypeAttestation,
				Time:        now,
			},
			callerSource:    lowerAddr,
			expectedSubject: "did:erc721:1:" + checksumAddr + ":1",
			expectedSource:  checksumAddr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := cloudevent.RawEvent{CloudEventHeader: tt.event, Data: json.RawMessage(`{}`)}
			msgBytes, err := json.Marshal(raw)
			require.NoError(t, err)

			got, err := parseAndValidateAttestation(msgBytes, tt.callerSource)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedSubject, got.Subject)
			assert.Equal(t, tt.expectedSource, got.Source)
		})
	}
}
