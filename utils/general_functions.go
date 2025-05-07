package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

func DeduceKeySchema[T any](keyStruct T) ([]byte, error) {
	val := reflect.ValueOf(keyStruct)
	typ := reflect.TypeOf(keyStruct)

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("keyStruct must be a struct")
	}

	idField := val.FieldByName("Id")
	if !idField.IsValid() || (idField.Kind() != reflect.String && !idField.Type().Implements(reflect.TypeOf((*fmt.Stringer)(nil)).Elem())) {
		return nil, fmt.Errorf("keyStruct must have an Id field of type string or fmt.Stringer")
	}

	key := map[string]interface{}{
		"schema": map[string]interface{}{
			"type": "struct",
			"fields": []map[string]interface{}{
				{
					"field":    "id",
					"type":     "string",
					"optional": false,
				},
			},
			"optional": false,
			"version":  1,
		},
		"payload": map[string]interface{}{
			"id": idField.Interface(),
		},
	}

	fmt.Printf("Key: %+v\n", key)

	keyBytes, err := json.Marshal(key)
	if err != nil {
		return nil, err
	}

	return keyBytes, nil
}

func DeduceValueSchema[T any](valueStruct T) (map[string]interface{}, error) {
	val := reflect.ValueOf(valueStruct)
	typ := reflect.TypeOf(valueStruct)

	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("valueStruct must be a struct")
	}

	fields := []map[string]interface{}{}
	payload := map[string]interface{}{}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// name := field.Name
		jsonTag := field.Tag.Get("json")
		jsonFieldName := strings.Split(jsonTag, ",")[0]
		if jsonFieldName == "" || jsonFieldName == "-" {
			jsonFieldName = field.Name
		}

		name := jsonFieldName
		// Do not include id field in schema/payload!
		if name == "id" {
			continue
		}
		ftype := field.Type.Kind()

		var schemaType string
		switch ftype {
		case reflect.String:
			schemaType = "string"
		case reflect.Int, reflect.Int32, reflect.Int64:
			schemaType = "int64"
		case reflect.Bool:
			schemaType = "boolean"
		case reflect.Float64, reflect.Float32:
			schemaType = "float"
		default:
			return nil, fmt.Errorf("field %s not of supported type", name)
		}

		fields = append(fields, map[string]interface{}{
			"field":    name,
			"type":     schemaType,
			"optional": false,
		})
		payload[name] = val.Field(i).Interface()
	}

	valueSchema := map[string]interface{}{
		"schema": map[string]interface{}{
			"type":     "struct",
			"fields":   fields,
			"optional": false,
			"version":  1,
		},
		"payload": payload,
	}

	fmt.Printf("Value: %+v\n", valueSchema)
	return valueSchema, nil
	// valueBytes, err := json.Marshal(valueSchema)
	// if err != nil {
	// 	return nil, err
	// }

	// return valueBytes, nil
}
