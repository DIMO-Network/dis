package processors

import (
	"fmt"
	"os"
	"time"

	"github.com/DIMO-Network/model-garage/pkg/cloudevent"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// MessageContentKey is the key for the message content.
const (
	MessageContentKey = "dimo_message_content"
	defaultSkew       = time.Minute * 5
)

var allowableTimeSkew = getSkew()

// AppendError appends an error message to the batches.
func AppendError(batches []service.MessageBatch, msg *service.Message, componentName string, err error) []service.MessageBatch {
	msg.SetError(err)
	msg.MetaSetMut("dimo_component", componentName)
	return append(batches, service.MessageBatch{msg})
}

// MsgToEvent converts a message to a cloudevent.
func MsgToEvent(msg *service.Message) (*cloudevent.RawEvent, error) {
	msgStruct, err := msg.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("failed to get msg as struct: %w", err)
	}
	rawEvent, ok := msgStruct.(*cloudevent.RawEvent)
	if !ok {
		return nil, fmt.Errorf("failed to cast to cloudevent.RawEvent")
	}
	return rawEvent, nil
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
