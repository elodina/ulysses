/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package validation

import (
	"fmt"
	"github.com/elodina/go-avro"
)

func validate(writer avro.Schema, reader avro.Schema) error {
	writerType := writer.Type()
	readerType := reader.Type()

	if writerType == readerType {
		switch writerType {
		case avro.Null, avro.Boolean, avro.Int, avro.Long, avro.Float, avro.Double, avro.String, avro.Bytes:
			return nil
		case avro.Fixed:
			return validateFixed(writer, reader)
		case avro.Enum:
			return validateEnum(writer, reader)
		case avro.Array:
			return validateArray(writer, reader)
		case avro.Map:
			return validateMap(writer, reader)
		case avro.Record:
			return validateRecord(writer, reader)
		case avro.Union:
			return validateUnion(writer, reader)
		default:
			return fmt.Errorf("Unknown schema type: %d", writerType)
		}
	} else {
		if writerType == avro.Union {
			return validateUnion(writer, reader)
		}

		switch readerType {
		case avro.Long:
			{
				if writerType == avro.Int {
					return nil
				}
			}
		case avro.Float:
			{
				switch writerType {
				case avro.Int, avro.Long:
					return nil
				}
			}
		case avro.Double:
			{
				switch writerType {
				case avro.Int, avro.Long, avro.Float:
					return nil
				}
			}
		case avro.Bytes:
			{
				if writerType == avro.String {
					return nil
				}
			}
		case avro.String:
			{
				if writerType == avro.Bytes {
					return nil
				}
			}
		case avro.Union:
			{
				return validateUnion(reader, writer)
			}
		case avro.Null, avro.Boolean, avro.Int, avro.Enum, avro.Array, avro.Map, avro.Record, avro.Fixed:
		default:
			return fmt.Errorf("Unknown schema type: %d", readerType)
		}
	}

	return fmt.Errorf("Found %s, expecting %s", avro.GetFullName(writer), avro.GetFullName(reader))
}

func validateFixed(writer avro.Schema, reader avro.Schema) error {
	fixedWriter := writer.(*avro.FixedSchema)
	fixedReader := reader.(*avro.FixedSchema)
	if avro.GetFullName(writer) != avro.GetFullName(reader) {
		return fmt.Errorf("Different Fixed type names: writer %s, reader %s", writer.GetName(), reader.GetName())
	}

	if fixedWriter.Size != fixedReader.Size {
		return fmt.Errorf("Different Fixed type sizes: writer %d, reader %d", fixedWriter.Size, fixedReader.Size)
	}

	return nil
}

func validateEnum(writer avro.Schema, reader avro.Schema) error {
	enumWriter := writer.(*avro.EnumSchema)
	enumReader := reader.(*avro.EnumSchema)

	if avro.GetFullName(writer) != avro.GetFullName(reader) {
		return fmt.Errorf("Different Enum type names: writer %s, reader %s", writer.GetName(), reader.GetName())
	}

	readerSymbolsMap := make(map[string]struct{})
	for _, symbol := range enumReader.Symbols {
		readerSymbolsMap[symbol] = struct{}{}
	}

	for _, symbol := range enumWriter.Symbols {
		if _, ok := readerSymbolsMap[symbol]; !ok {
			return fmt.Errorf("Enum symbol %s does not exist for reader schema", symbol)
		}
	}

	return nil
}

func validateArray(writer avro.Schema, reader avro.Schema) error {
	arrayWriter := writer.(*avro.ArraySchema)
	arrayReader := reader.(*avro.ArraySchema)

	return validate(arrayWriter.Items, arrayReader.Items)
}

func validateMap(writer avro.Schema, reader avro.Schema) error {
	mapWriter := writer.(*avro.MapSchema)
	mapReader := reader.(*avro.MapSchema)

	return validate(mapWriter.Values, mapReader.Values)
}

func validateRecord(writer avro.Schema, reader avro.Schema) error {
	recordWriter := writer.(*avro.RecordSchema)
	recordReader := reader.(*avro.RecordSchema)

	if avro.GetFullName(writer) != avro.GetFullName(reader) {
		return fmt.Errorf("Different Record type names: writer %s, reader %s", writer.GetName(), reader.GetName())
	}

	writerFields := recordWriter.Fields
	readerFields := recordReader.Fields

	writerFieldsMap := make(map[string]*avro.SchemaField)
	for _, field := range writerFields {
		writerFieldsMap[field.Name] = field
	}

	readerFieldsMap := make(map[string]*avro.SchemaField)
	for _, field := range readerFields {
		readerFieldsMap[field.Name] = field
	}

	for _, readerField := range readerFields {
		if _, ok := writerFieldsMap[readerField.Name]; !ok && readerField.Default == nil {
			return fmt.Errorf("Introduced field %s does not have default value which is required.", readerField.Name)
		}
	}

	for _, writerField := range writerFields {
		if readerField, ok := readerFieldsMap[writerField.Name]; ok {
			err := validate(writerField.Type, readerField.Type)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func validateUnion(writer avro.Schema, reader avro.Schema) error {
	unionWriter := writer.(*avro.UnionSchema)

	for _, writerSchema := range unionWriter.Types {
		if validate(writerSchema, reader) == nil {
			return nil
		}
	}

	return fmt.Errorf("Writer schema %s cannot be read by %s", writer.String(), reader.String())
}
