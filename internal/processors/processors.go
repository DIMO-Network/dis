package processors

import "github.com/redpanda-data/benthos/v4/public/service"

// MessageContentKey is the key for the message content.
const MessageContentKey = "dimo_message_content"

// AppendError appends an error message to the batches.
func AppendError(batches []service.MessageBatch, msg *service.Message, err error) []service.MessageBatch {
	msg.SetError(err)
	return append(batches, service.MessageBatch{msg})
}
