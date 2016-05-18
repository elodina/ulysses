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
	"regexp"
	"testing"

	avro "github.com/elodina/go-avro"
	"gopkg.in/stretchr/testify.v1/assert"
	"gopkg.in/stretchr/testify.v1/require"
)

func TestValidatorNoReader(t *testing.T) {
	// shouldn't get any errors if reader schema slice is empty
	validator, err := NewBuilder().CanReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(avro.MustParseSchema(`{"type": "int"}`), nil)
	assert.Equal(t, nil, err)

	validator, err = NewBuilder().CanReadStrategy().ValidateAll()
	require.Equal(t, nil, err)
	err = validator.Validate(avro.MustParseSchema(`{"type": "int"}`), nil)
	assert.Equal(t, nil, err)
}

func TestValidatorCase1(t *testing.T) {
	// good case with introducing a new field with default value, fully compatible

	writer := avro.MustParseSchema(`{"namespace": "example.avro",
 "type": "record",
 "name": "user",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": "int"}
 ]
}`)
	reader := avro.MustParseSchema(`{"namespace": "example.avro",
 "type": "record",
 "name": "user",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": "int"},
     {"name": "favorite_color", "type": "string", "default": "green"}
 ]
}`)

	validator, err := NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)
}

func TestValidatorCase2(t *testing.T) {
	// case with introducing a new field without default value, one way compatible

	writer := avro.MustParseSchema(`{"namespace": "example.avro",
 "type": "record",
 "name": "user",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": "int"}
 ]
}`)
	reader := avro.MustParseSchema(`{"namespace": "example.avro",
 "type": "record",
 "name": "user",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": "int"},
     {"name": "favorite_color", "type": "string"}
 ]
}`) // no default

	validator, err := NewBuilder().CanReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)

	validator, err = NewBuilder().CanBeReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*does not have default value.*"), err)

	// also make sure we get the error if one of mutual read cases fails
	validator, err = NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*does not have default value.*"), err)
}

func TestValidatorCase3(t *testing.T) {
	// validate 2 reader schemas where last is incompatible

	writer := avro.MustParseSchema(`{"namespace": "example.avro",
 "type": "record",
 "name": "user",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": "int"}
 ]
}`)

	reader := avro.MustParseSchema(`{"namespace": "example.avro",
 "type": "record",
 "name": "user",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": "int"},
     {"name": "favorite_color", "type": "string", "default": "green"}
 ]
}`)

	reader2 := avro.MustParseSchema(`{"namespace": "example.avro",
 "type": "record",
 "name": "user",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": "int"},
     {"name": "favorite_color", "type": "string"}
 ]
}`) // no default

	validator, err := NewBuilder().CanBeReadStrategy().ValidateAll()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader, reader2})
	assert.Regexp(t, regexp.MustCompile(".*does not have default value.*"), err)
}

func TestValidatorDifferentRecordNames(t *testing.T) {
	writer := avro.MustParseSchema(`{"namespace": "foo.bar",
 "type": "record",
 "name": "user",
 "fields": [{"name": "name", "type": "string"}]
 }`)
	reader := avro.MustParseSchema(`{"namespace": "foo.bar.baz",
 "type": "record",
 "name": "user",
 "fields": [{"name": "name", "type": "string"}]
 }`)

	validator, err := NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Different Record type names.*"), err)
}

func TestValidatorFixed(t *testing.T) {
	// good case
	writer := avro.MustParseSchema(`{"type": "fixed", "size": 16, "name": "md5"}`)
	reader := avro.MustParseSchema(`{"type": "fixed", "size": 16, "name": "md5"}`)

	validator, err := NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)

	// other name
	reader = avro.MustParseSchema(`{"type": "fixed", "size": 16, "name": "new_md5"}`)

	validator, err = NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Different Fixed type names.*"), err)

	// other size
	reader = avro.MustParseSchema(`{"type": "fixed", "size": 17, "name": "md5"}`)

	validator, err = NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Different Fixed type sizes.*"), err)
}

func TestValidatorEnum(t *testing.T) {
	// good case
	writer := avro.MustParseSchema(`{"type": "enum", "name": "Suit", "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}`)
	reader := avro.MustParseSchema(`{"type": "enum", "name": "Suit", "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}`)

	validator, err := NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)

	// other name
	reader = avro.MustParseSchema(`{"type": "enum", "name": "Suits", "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]}`)

	validator, err = NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Different Enum type names.*"), err)

	// other symbols
	reader = avro.MustParseSchema(`{"type": "enum", "name": "Suit", "symbols": ["SPADES", "HEARTS", "DIAMONDS"]}`)

	validator, err = NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Enum symbol .* does not exist.*"), err)
}

func TestValidatorArray(t *testing.T) {
	// good case
	writer := avro.MustParseSchema(`{"type": "array", "items": "string"}`)
	reader := avro.MustParseSchema(`{"type": "array", "items": "string"}`)

	validator, err := NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)

	// other item type
	reader = avro.MustParseSchema(`{"type": "array", "items": "int"}`)

	validator, err = NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Found string, expecting int.*"), err)
}

func TestValidatorMap(t *testing.T) {
	// good case
	writer := avro.MustParseSchema(`{"type": "map", "values": "string"}`)
	reader := avro.MustParseSchema(`{"type": "map", "values": "string"}`)

	validator, err := NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)

	// other value type
	reader = avro.MustParseSchema(`{"type": "map", "values": "int"}`)

	validator, err = NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Found string, expecting int.*"), err)
}

func TestValidatorUnion(t *testing.T) {
	// good case
	writer := avro.MustParseSchema(`["null", "string"]`)
	reader := avro.MustParseSchema(`["null", "string"]`)

	validator, err := NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)

	// other union type
	reader = avro.MustParseSchema(`["int", "float"]`)

	validator, err = NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*cannot be read.*"), err)

	// string -> [null, string]
	reader = avro.MustParseSchema(`{"type": "string"}`)

	validator, err = NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)
}
