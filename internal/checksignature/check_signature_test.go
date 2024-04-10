package checksignature

import (
	"context"
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestSignatureProcessorSuccess(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	revProc := newSignatureProcessor(nil)

	msg := `{
		"data": {"timestamp":1709656316768},
		"signature": "0xed107d9e947ce3208f2a349fe9de841295252ed3157a6a6c3725c5b2fd47ccaf1407ba498f09a872ce409d3434162512f7d0b08fbc0561f407015b39953bb6d61c",
		"subject": "0x318F53cC0775fdfcb95A378Fd3228Dc564A28c61"
	}`

	result, err := revProc.Process(context.Background(),
		service.NewMessage([]byte(msg)))
	require.NoError(t, err)
	require.Len(t, result, 1)
}

func TestSignatureProcessorFailure(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	revProc := newSignatureProcessor(nil)

	msg := `{
		"data": {"timestamp":1709656316768},
		"signature": "0xed107d9e947ce3208f2a349fe9de841295252ed3157a6a6c3725c5b2fd47ccaf1407ba498f09a872ce409d3434162512f7d0b08fbc0561f407015b39953bb6d61c",
		"subject": "0xDC1eE274BCA98b421293f3737D1b9E4563c60cb3"
	}`

	result, err := revProc.Process(context.Background(),
		service.NewMessage([]byte(msg)))
	require.Error(t, err)
	require.Equal(t, err.Error(), "recovered wrong address 0x318F53cC0775fdfcb95A378Fd3228Dc564A28c61")
	require.Len(t, result, 0)
}

func TestSignatureProcessorInvalidSignature(t *testing.T) {
	revProc := newSignatureProcessor(nil)

	msg := `{
		"data": {"timestamp":1709656316768},
		"signature": "5fb985f758c6224ab45630d055c7aca163329b88accfb8fd76a0dbb13b2ebcfe3c5bd8b801851f683f7a288c174a11ed8fc2631d95929c3b3cc85c75fb10ea001a",
		"subject": "0x06fF8E7A4A159EA388da7c133DC5F79727868d83"
	}`

	result, err := revProc.Process(context.Background(),
		service.NewMessage([]byte(msg)))
	require.Error(t, err)
	require.Equal(t, err.Error(), "failed to recover an address: invalid signature recovery id")
	require.Len(t, result, 0)
}
