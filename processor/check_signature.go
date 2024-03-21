package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/DIMO-Network/shared"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

var zeroAddr common.Address

const sigLen = 65

type signatureProcessor struct {
	logger *service.Logger
}

func init() {
	// Config spec is empty for now as we don't have any dynamic fields.
	configSpec := service.NewConfigSpec().Description("Validates the signature of a message.")
	constructor := func(_ *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		return newSignatureProcessor(mgr.Logger()), nil
	}
	err := service.RegisterProcessor("check_signature", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

func newSignatureProcessor(lgr *service.Logger) *signatureProcessor {
	// The logger will already be labelled with the
	// identifier of this component within a config.
	return &signatureProcessor{
		logger: lgr,
	}
}

type Event struct {
	shared.CloudEvent[json.RawMessage]
	Signature string `json:"signature"`
}

func (s *signatureProcessor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Extract the message payload as a byte slice.
	payload, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	var event Event
	err = json.Unmarshal(payload, &event)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	addr := common.HexToAddress(event.Subject)
	signature := common.FromHex(event.Signature)
	hash := crypto.Keccak256Hash(event.Data)

	if recAddr, err := Ecrecover(hash.Bytes(), signature); err != nil {
		return nil, fmt.Errorf("failed to recover an address: %w", err)
	} else if recAddr != addr {
		return nil, fmt.Errorf("recovered wrong address %s", recAddr)
	}

	msg = service.NewMessage([]byte("Signature is valid"))
	return []*service.Message{msg}, nil
}

func (p *signatureProcessor) Close(ctx context.Context) error {
	return nil
}

// This is exact copy of the function from internal/controllers/helpers/handlers.go
// Ecrecover mimics the ecrecover opcode, returning the address that signed
// hash with signature. sig must have length 65 and the last byte, the recovery
// byte usually denoted v, must be 27 or 28.
func Ecrecover(hash, sig []byte) (common.Address, error) {
	if len(sig) != sigLen {
		return zeroAddr, fmt.Errorf("signature has invalid length %d", len(sig))
	}

	// Defensive copy: the caller shouldn't have to worry about us modifying
	// the signature. We adjust because crypto.Ecrecover demands 0 <= v <= 4.
	fixedSig := make([]byte, sigLen)
	copy(fixedSig, sig)
	fixedSig[64] -= 27

	rawPk, err := crypto.Ecrecover(hash, fixedSig)
	if err != nil {
		return zeroAddr, err
	}

	pk, err := crypto.UnmarshalPubkey(rawPk)
	if err != nil {
		return zeroAddr, err
	}

	return crypto.PubkeyToAddress(*pk), nil
}
