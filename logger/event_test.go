package logger

import (
	"strings"
	"testing"
)

func TestEventSerialize(t *testing.T) {
	testCases := []struct {
		// inputs
		id        uint64
		eventType EventType
		key       string
		value     string

		// target outputs
		expected string
	}{
		{
			id:        1,
			eventType: EventPut,
			key:       "hello",
			value:     "world",
			expected:  "1\t0\thello\tworld",
		},
		{
			id:        2,
			eventType: EventPut,
			key:       "hello",
			value:     "",
			expected:  "2\t0\thello\t",
		},
		{
			id:        3,
			eventType: EventDel,
			key:       "key",
			expected:  "3\t1\tkey\t",
		},
		{
			id:        4,
			eventType: EventDel,
			key:       "key",
			value:     "unusedValue",
			expected:  "4\t1\tkey\tunusedValue",
		},
	}
	for i, testCase := range testCases {
		e := Event{
			Id:    testCase.id,
			Type:  testCase.eventType,
			Key:   testCase.key,
			Value: testCase.value,
		}
		if serialized := Serialize(e); serialized != testCase.expected {
			t.Errorf(
				"%d: expected %s, got %s",
				i, readable(testCase.expected), readable(serialized),
			)
		}
	}
}

func readable(s string) string {
	return strings.Replace(s, "\t", "\\t", -1)
}

func TestEventDeserialize(t *testing.T) {
	testCases := []struct {
		// inputs
		serialized string

		// target outputs
		id        uint64
		eventType EventType
		key       string
		value     string
	}{
		{
			serialized: "1\t0\thello\tworld",
			id:         1,
			eventType:  EventPut,
			key:        "hello",
			value:      "world",
		},
		{
			serialized: "2\t0\tabcd\t",
			id:         2,
			eventType:  EventPut,
			key:        "abcd",
			value:      "",
		},
		{
			serialized: "3\t0\t\t",
			id:         3,
			eventType:  EventPut,
			key:        "",
			value:      "",
		},
	}
	for i, testCase := range testCases {
		var e Event
		Deserialize(&e, testCase.serialized)
		if e.Id != testCase.id {
			t.Errorf(
				"%d: expected id %d, got %d",
				i, testCase.id, e.Id,
			)
		}
		if e.Type != testCase.eventType {
			t.Errorf(
				"%d: expected event type %d, got %d",
				i, testCase.eventType, e.Type,
			)
		}
		if e.Key != testCase.key {
			t.Errorf(
				"%d: expected key %s, got %s",
				i, testCase.key, e.Key,
			)
		}
		if e.Value != testCase.value {
			t.Errorf(
				"%d: expected value %s, got %s",
				i, testCase.value, e.Value,
			)
		}
	}
}
