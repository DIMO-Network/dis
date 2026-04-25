package cloudeventconvert

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/dis/internal/processors"
	"github.com/DIMO-Network/dis/internal/web3"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/segmentio/ksuid"
)

// processAttestationMsg is the entrypoint for attestation messages.
// It validates the message, verifies the signature, and sets the metadata.
func (c *cloudeventProcessor) processAttestationMsg(ctx context.Context, msg *service.Message, msgBytes []byte, source string) service.MessageBatch {
	event, err := parseAndValidateAttestation(msgBytes, source)
	if err != nil {
		processors.SetError(msg, processorName, "failed to process attestation", err)
		return service.MessageBatch{msg}
	}

	validSignature, err := c.verifySignature(event, common.HexToAddress(event.Source))
	if err != nil {
		processors.SetError(msg, processorName, "failed to check message signature", err)
		return service.MessageBatch{msg}
	}

	if !validSignature {
		processors.SetError(msg, processorName, "message signature invalid", nil)
		return service.MessageBatch{msg}
	}

	msg.MetaDelete("Authorization")
	setMetaData(&event.CloudEventHeader, msg)
	msg.MetaSetMut(processors.MessageContentKey, cloudEventValidContentType)

	msg.SetStructuredMut(event)

	return service.MessageBatch{msg}
}

func parseAndValidateAttestation(msgBytes []byte, source string) (*cloudevent.RawEvent, error) {
	var event cloudevent.RawEvent
	if err := json.Unmarshal(msgBytes, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal attestation cloud event: %w", err)
	}

	if processors.IsFutureTimestamp(event.Time) {
		return nil, fmt.Errorf("event timestamp %v exceeds valid range", event.Time)
	}

	if did, err := cloudevent.DecodeERC721DID(event.Subject); err == nil {
		event.Subject = did.String()
	} else if did, err := cloudevent.DecodeEthrDID(event.Subject); err == nil {
		event.Subject = did.String()
	} else {
		return nil, fmt.Errorf("invalid attestation subject format: %w", err)
	}

	// If the payload includes a source, use it (delegation support);
	// otherwise fall back to the JWT holder's address.
	resolvedSource := source
	if event.Source != "" {
		resolvedSource = event.Source
	}
	if !common.IsHexAddress(resolvedSource) {
		return nil, fmt.Errorf("invalid source address: %s", resolvedSource)
	}
	// Normalize to EIP-55 checksummed form for consistent storage.
	resolvedSource = common.HexToAddress(resolvedSource).Hex()

	if err := validateHeadersAndSetDefaults(&event.CloudEventHeader, resolvedSource, ksuid.New().String()); err != nil {
		return nil, fmt.Errorf("failed to validate headers: %w", err)
	}

	if event.Type == "" {
		event.Type = cloudevent.TypeAttestation
	}
	if !isValidAttestationType(event.Type) {
		return nil, fmt.Errorf("invalid attestation type %q: must be dimo.attestation, dimo.raw.*, or dimo.document.*", event.Type)
	}
	return &event, nil
}

func isValidAttestationType(t string) bool {
	switch {
	case t == cloudevent.TypeAttestation:
		return true
	case strings.HasPrefix(t, "dimo.raw.") && len(t) > len("dimo.raw."):
		return true
	case strings.HasPrefix(t, "dimo.document.") && len(t) > len("dimo.document."):
		return true
	default:
		return false
	}
}

// verifySignature attempts to verify the signed data.
// first check if the source is the signer
// if the source is not the signer, check whether the signature is from a dev license where the source is the contract addr
func (c *cloudeventProcessor) verifySignature(event *cloudevent.RawEvent, source common.Address) (bool, error) {
	signature := common.FromHex(event.Signature)

	msgHashWithPrfx := accounts.TextHash(event.Data)
	eoaSigner, errEoa := verifyEOASignature(signature, msgHashWithPrfx, source)
	if errEoa != nil || !eoaSigner {
		erc1271Signer, errErc := c.verifyERC1271Signature(signature, common.BytesToHash(msgHashWithPrfx), source)
		if errErc != nil {
			return false, errors.Join(errEoa, errErc)
		}

		return erc1271Signer, nil
	}

	return true, nil
}

func verifyEOASignature(signature []byte, msgHash []byte, source common.Address) (bool, error) {
	if len(signature) != 65 {
		return false, fmt.Errorf("signature has length %d != 65", len(signature))
	}

	sigCopy := make([]byte, len(signature))
	copy(sigCopy, signature)

	sigCopy[64] -= 27
	if sigCopy[64] != 0 && sigCopy[64] != 1 {
		return false, fmt.Errorf("invalid v byte: %d; accepted values 27 or 28", signature[64])
	}

	pubKey, err := crypto.SigToPub(msgHash, sigCopy)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal public key: %w", err)
	}
	recoveredAddress := crypto.PubkeyToAddress(*pubKey)
	return source == recoveredAddress, nil
}

func (c *cloudeventProcessor) verifyERC1271Signature(signature []byte, msgHash common.Hash, source common.Address) (bool, error) {
	contract, err := web3.NewErc1271(source, c.ethClient)
	if err != nil {
		return false, fmt.Errorf("failed to connect to address: %s: %w", source, err)
	}

	result, err := contract.IsValidSignature(nil, msgHash, signature)
	if err != nil {
		return false, fmt.Errorf("failed to validate signature with contract: %w", err)
	}

	return result == erc1271magicValue, nil
}
