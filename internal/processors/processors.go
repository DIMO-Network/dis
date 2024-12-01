package processors

import "github.com/redpanda-data/benthos/v4/public/service"

// AppendError appends an error message to the batches.
func AppendError(batches []service.MessageBatch, msg *service.Message, err error) []service.MessageBatch {
	msg.SetError(err)
	return append(batches, service.MessageBatch{msg})
}
