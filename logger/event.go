package logger

import "fmt"

type EventType byte

const (
	EventPut EventType = iota
	EventDel
)

type Event struct {
	Id    uint64
	Type  EventType
	Key   string
	Value string
}

func NewEventPut(key, value string) Event {
	return Event{Type: EventPut, Key: key, Value: value}
}

func NewEventDel(key string) Event {
	return Event{Type: EventDel, Key: key}
}

func Serialize(e Event) string {
	return fmt.Sprintf("%d\t%d\t%s\t%s", e.Id, e.Type, e.Key, e.Value)
}

func Deserialize(e *Event, s string) error {
	if _, err := fmt.Sscanf(
		s, "%d\t%d\t%s\t%s",
		&e.Id, &e.Type, &e.Key, &e.Value,
	); err != nil {
		return err
	}
	return nil
}
