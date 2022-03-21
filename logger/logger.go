package logger

import (
	"bufio"
	"fmt"
	"os"
)

// Logger implements a file-based transaction log.
type Logger struct {
	file      *os.File
	eventChan chan Event
	errChan   chan error
	nextId    uint64
}

func NewLogger(filename string, bufSize int) (*Logger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	return &Logger{
		file:      file,
		eventChan: make(chan Event, bufSize),
		errChan:   make(chan error, 1),
	}, nil
}

func (l *Logger) Run() {
	go func() {
		for e := range l.eventChan {
			e.Id = l.nextId
			if _, err := fmt.Fprintf(
				l.file, "%d\t%d\t%s\t%s\n",
				e.Id, e.Type, e.Key, e.Value,
			); err != nil {
				l.errChan <- err
				return
			}
			l.nextId++
		}
	}()
}

func (l *Logger) WritePut(key, value string) {
	l.eventChan <- Event{Type: EventPut, Key: key, Value: value}
}

func (l *Logger) WriteDel(key string) {
	l.eventChan <- Event{Type: EventDel, Key: key}
}

func (l *Logger) Err() <-chan error {
	return l.errChan
}

func (l *Logger) Replay() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file)
	eventChan := make(chan Event)
	errChan := make(chan error, 1)
	go func() {
		defer func() {
			close(eventChan)
			close(errChan)
		}()
		var e Event
		for scanner.Scan() {
			line := scanner.Text()
			if _, err := fmt.Sscanf(
				line, "%d\t%d\t%s\t%s",
				&e.Id, &e.Type, &e.Key, &e.Value,
			); err != nil {
				errChan <- err
				return
			}
			if e.Id <= l.nextId {
				errChan <- fmt.Errorf("transaction ids out of order")
				return
			}
			l.nextId = e.Id
			eventChan <- e
		}
		if err := scanner.Err(); err != nil {
			errChan <- fmt.Errorf("failed logger replay: %w", err)
			return
		}
	}()
	return eventChan, errChan
}

func (l *Logger) Close() error {
	close(l.eventChan)
	close(l.errChan)
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("failed to close logger: %w", err)
	}
	return nil
}
