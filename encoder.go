package encoder

import (
	"context"
	"fmt"

	eh "github.com/looplab/eventhorizon"
)

type Encoder interface {
	Decode([]byte) (eh.Event, context.Context, error)
	Encode(context.Context, eh.Event) ([]byte, error)
	String() string
}

// Error is an async error containing the error and the event.
type Error struct {
	Err   error
	Ctx   context.Context
	Event eh.Event
}

// Error implements the Error method of the error interface.
func (e Error) Error() string {
	return fmt.Sprintf("%s: (%s)", e.Err, e.Event.String())
}
