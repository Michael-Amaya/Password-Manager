package utils

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type DBEntry struct {
	UID     uuid.UUID
	SQLData map[string]any
	Table   string
}

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

func PGInsert(structure interface{}, table string) error {

	return errors.New("not implemented")
}

func PGDelete(structure interface{}, topic string) error {
	return errors.New("not implemented")
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

func GenerateSQLStructure(obj interface{}) ([]*DBEntry, error) {
	var entries []*DBEntry
	convertToSQL(obj, map[uintptr]*DBEntry{}, &entries)
	return entries, nil
}

func convertToSQL(obj interface{}, visited map[uintptr]*DBEntry, entries *[]*DBEntry) (*DBEntry, error) {
	// Always ensure we're working with a pointer to a struct
	val := reflect.ValueOf(obj)
	if val.Kind() == reflect.Struct {
		valPtr := reflect.New(val.Type())
		valPtr.Elem().Set(val)
		val = valPtr
	} else if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return nil, errors.New("expected struct or pointer to struct")
	}

	ptr := val.Pointer()
	if existing, ok := visited[ptr]; ok {
		return existing, nil
	}

	elem := val.Elem()
	typ := elem.Type()
	table := strings.ToLower(typ.Name())

	entry := &DBEntry{
		UID:     uuid.New(),
		SQLData: make(map[string]any),
		Table:   table,
	}
	visited[ptr] = entry

	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		fieldType := typ.Field(i)
		col := strings.ToLower(fieldType.Name)

		if field.Kind() == reflect.Struct {
			if field.CanAddr() {
				childPtr := field.Addr().Interface()
				childEntry, err := convertToSQL(childPtr, visited, entries)
				if err != nil {
					return nil, err
				}
				entry.SQLData[col+"_ref"] = childEntry.UID
			}
		} else {
			entry.SQLData[col] = field.Interface()
		}
	}

	*entries = append(*entries, entry)
	return entry, nil
}
