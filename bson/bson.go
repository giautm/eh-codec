package bson

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/giautm/eh-encoder"
	"github.com/globalsign/mgo/bson"
	eh "github.com/looplab/eventhorizon"
)

func NewEncoder() encoder.Encoder {
	return &bsonEncoder{}
}

type bsonEncoder struct{}

func (bsonEncoder) Decode(rawData []byte) (eh.Event, context.Context, error) {
	// Manually decode the raw BSON event.
	data := bson.Raw{
		Kind: 3,
		Data: rawData,
	}

	var e evt
	if err := data.Unmarshal(&e); err != nil {
		return nil, nil, encoder.Error{
			Err: errors.New("could not unmarshal event: " + err.Error()),
		}
	}

	// Create an event of the correct type.
	if data, err := eh.CreateEventData(e.EventType); err == nil {
		// Manually decode the raw BSON event.
		if err := e.RawData.Unmarshal(data); err != nil {
			return nil, nil, encoder.Error{
				Err: errors.New("could not unmarshal event data: " + err.Error()),
			}
		}

		// Set concrete event and zero out the decoded event.
		e.data = data
		e.RawData = bson.Raw{}
	}

	event := event{evt: e}
	ctx := eh.UnmarshalContext(e.Context)
	return event, ctx, nil
}

func (bsonEncoder) Encode(ctx context.Context, event eh.Event) ([]byte, error) {
	e := evt{
		AggregateID:   event.AggregateID(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		Version:       event.Version(),
		Timestamp:     event.Timestamp(),
		Context:       eh.MarshalContext(ctx),
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		rawData, err := bson.Marshal(event.Data())
		if err != nil {
			return nil, encoder.Error{
				Err:   errors.New("could not marshal event data: " + err.Error()),
				Event: event,
				Ctx:   ctx,
			}
		}
		e.RawData = bson.Raw{Kind: 3, Data: rawData}
	}

	data, err := bson.Marshal(e)
	if err != nil {
		return nil, encoder.Error{
			Err:   errors.New("could not marshal event: " + err.Error()),
			Event: event,
			Ctx:   ctx,
		}
	}

	return data, nil
}

func (bsonEncoder) String() string {
	return "bson"
}

// evt is the internal event used on the wire only.
type evt struct {
	EventType     eh.EventType           `bson:"event_type"`
	RawData       bson.Raw               `bson:"data,omitempty"`
	Timestamp     time.Time              `bson:"timestamp"`
	AggregateType eh.AggregateType       `bson:"aggregate_type"`
	AggregateID   eh.UUID                `bson:"_id"`
	Version       int                    `bson:"version"`
	Context       map[string]interface{} `bson:"context"`
	data          eh.EventData           `bson:"-"`
}

// event is the private implementation of the eventhorizon.Event interface
// for a MongoDB event store.
type event struct {
	evt
}

// EventType implements the EventType method of the eventhorizon.Event interface.
func (e event) EventType() eh.EventType {
	return e.evt.EventType
}

// Data implements the Data method of the eventhorizon.Event interface.
func (e event) Data() eh.EventData {
	return e.evt.data
}

// Timestamp implements the Timestamp method of the eventhorizon.Event interface.
func (e event) Timestamp() time.Time {
	return e.evt.Timestamp
}

// AggregateType implements the AggregateType method of the eventhorizon.Event interface.
func (e event) AggregateType() eh.AggregateType {
	return e.evt.AggregateType
}

// AggrgateID implements the AggrgateID method of the eventhorizon.Event interface.
func (e event) AggregateID() eh.UUID {
	return e.evt.AggregateID
}

// Version implements the Version method of the eventhorizon.Event interface.
func (e event) Version() int {
	return e.evt.Version
}

// String implements the String method of the eventhorizon.Event interface.
func (e event) String() string {
	return fmt.Sprintf("%s@%d", e.evt.EventType, e.evt.Version)
}
