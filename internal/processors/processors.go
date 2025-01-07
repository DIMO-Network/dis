package processors

import (
	"os"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// MessageContentKey is the key for the message content.
const (
	MessageContentKey = "dimo_message_content"
	defaultSkew       = time.Minute * 5
)

var allowableTimeSkew = getSkew()

// AppendError appends an error message to the batches.
func AppendError(batches []service.MessageBatch, msg *service.Message, err error) []service.MessageBatch {
	msg.SetError(err)
	return append(batches, service.MessageBatch{msg})
}

// IsFutureTimestamp checks if a timestamp is in the future past the allowable time skew.
func IsFutureTimestamp(ts time.Time) bool {
	return ts.After(time.Now().Add(allowableTimeSkew))
}

func getSkew() time.Duration {
	skew := os.Getenv("ALLOWABLE_TIME_SKEW")
	if skew == "" {
		return defaultSkew
	}
	dur, err := time.ParseDuration(skew)
	if err != nil {
		return defaultSkew
	}
	return dur
}
