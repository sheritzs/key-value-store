package main

import (
	"bufio"
	"fmt"
	"os"
)

type EventType byte

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

type Event struct {
	Sequence  uint64    // Unique record ID
	EventType EventType // Action taken
	Key       string    // Key affected by the transaction
	Value     string    // Value of the transaction
}

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error

	ReadEvents() (<-chan Event, <-chan error)

	Run()
}

type FileTransactionLogger struct {
	events       chan<- Event // Write-only channel for sending events
	errors       <-chan error // Read-only channel for receiving errors
	lastSequence uint64       // Last used event sequence number
	file         *os.File     // Transaction log	location
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) { // construction function
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}

	return &FileTransactionLogger{file: file}, nil
}

func (l *FileTransactionLogger) Run() {
	events := make(chan Event, 16) // Create a buffered events channel
	l.events = events

	errors := make(chan error, 1) // Create a buffered errors channel;  val of 1 allows for sending of error in
	l.errors = errors             // nonblocking manner

	go func() { // goroutine to retrieve Event values
		for e := range events {

			l.lastSequence++ // Increment sequence number

			_, err := fmt.Fprintf( // Write event to the log
				l.file,
				"%d\t%d\t%s\t%s\n",
				l.lastSequence, e.EventType, e.Key, e.Value)

			if err != nil {
				errors <- err
				return
			}
		}
	}()

}

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file)
	outEvent := make(chan Event)    // An unbuffered Event channel
	outError := make(chan error, 1) // A buffered error channel

	go func() {
		var e Event

		defer close(outEvent) // Close the channels when the goroutine ends
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s",
				&e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {

				outError <- fmt.Errorf("input parse error: %w, err")
				return
			}

			// Sanity check to verify whether the sequence numbers are
			// in increasing order
			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}

			l.lastSequence = e.Sequence // Update last used sequence #

			outEvent <- e // Send the event along
		}
		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()

	return outEvent, outError
}
