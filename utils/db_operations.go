package utils

import (
	"context"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
)

func PGQuery[T any](ctx context.Context, conn *pgx.Conn, query string, args ...interface{}) ([]T, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []T
	for rows.Next() {
		var result T
		dest, err := getFieldPointers(&result)
		if err != nil {
			return nil, err
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}

		results = append(results, result)
	}

	return results, rows.Err()
}

func PGInsert(structure interface{}, topic string) error {
	producer, err := NewKafkaProducer("localhost:9092", "password_manager_producer")
	if err != nil {
		return err
	}
	defer producer.Close() // Commit and close the producer at the end of the program

	key, err := DeduceKeySchema(structure) // Returns *key that is a json marshalled map of a valid key
	if err != nil {
		return err
	}
	fmt.Printf("key: %s\n", string(key))

	message, err := DeduceValueSchema(structure)
	if err != nil {
		return err
	}

	fmt.Printf("message: %+v\n", message)

	err = producer.Produce(topic, key, message)
	if err != nil {
		return err
	}

	return producer.Commit()
}

func PGDelete(structure interface{}, topic string) error {
	producer, err := NewKafkaProducer("localhost:9092", "password_manager_producer")
	if err != nil {
		return err
	}
	defer producer.Close() // Commit and close the producer at the end of the program

	key, err := DeduceKeySchema(structure) // Returns *key that is a json marshalled map of a valid key
	if err != nil {
		return err
	}
	fmt.Printf("key: %s\n", string(key))

	producer.Produce(topic, key, nil)
	return producer.Commit()
}

// ------------------------- Local Utils -------------------------
func getFieldPointers[T any](ptr *T) ([]any, error) {
	val := reflect.ValueOf(ptr).Elem()
	numFields := val.NumField()
	fields := make([]any, numFields)
	for i := 0; i < numFields; i++ {
		fields[i] = val.Field(i).Addr().Interface()
	}
	return fields, nil
}
