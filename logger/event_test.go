package logger

import "testing"

func TestEventSerialize(t *testing.T) {
	testCases := []struct {
		// inputs
		id        uint64
		eventType EventType
		key       string
		value     string

		// target outputs
		serialized string
	}{
		{},
	}
}
