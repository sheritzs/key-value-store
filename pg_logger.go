package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

type PostgresdDBParams struct {
	dbName   string
	host     string
	user     string
	password string
}

type PostgresTransactionLogger struct {
	events chan<- Event // Write-only channel for sending events
	errors <-chan error // Read-only channel for receiving errors
	db     *sql.DB      // Database access interface
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	const table = "transactions"
	var result string

	rows, err := l.db.Query(fmt.Sprintf("SELECT to_regclass('public.%s');", table))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() && result != table {
		rows.Scan(&result)
	}

	return result == table, rows.Err()

}

func (l *PostgresTransactionLogger) createTable() error {
	var err error

	query := `CREATE TABLE transactions (
			sequence 	BIGSERIAL PRIMARY KEY,
			event_type 	SMALLINT,
			key 		TEXT,
			value 		TEXT
			);`

	_, err = l.db.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func NewPostgresTransactionLogger(config PostgresdDBParams) (TransactionLogger, error) { // construction function

	connStr := fmt.Sprintf("host=%s dbname=%s, user=%s password=%s",
		config.host, config.dbName, config.user, config.password)

	db, err := sql.Open("pstgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	err = db.Ping() // Test the database connection
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	logger := &PostgresTransactionLogger{{db: db}}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}

	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed create table: %w", err)
		}
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		query := `INSERT INTO transactions 
						(event_type, key, value)
						VALUES ($1, $2, $3)`

		for e := range events {
			_, err := l.db.Exec(
				query,
				e.EventType, e.Key, e.Value)

			if err != nil {
				errors <- err
			}
		}

	}()
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)    // An unbuffered Event channel
	outError := make(chan error, 1) // A buffered error channel

	go func() {
		defer close(outEvent) // Close the channels when the goroutine ends
		defer close(outError)

		query := `SELECT sequence, event_type, key, value
				  FROM transactions
				  ORDER BY sequence`

		rows, err := l.db.Query(query)
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}

		defer rows.Close()

		e := Event{}

		for rows.Next() {

			err = rows.Scan(
				&e.Sequence, &e.EventType,
				&e.Key, &e.Value)

			if err != nil {
				outError <- fmt.Errorf("error readingrow: %w", err)
				return
			}

			outEvent <- e
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()

	return outEvent, outError
}
