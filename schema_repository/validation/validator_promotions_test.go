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

func TestValidatorPromotionIntLong(t *testing.T) {
	// int -> long promotion

	writer := avro.MustParseSchema(`{
	"type": "int"
}`)
	reader := avro.MustParseSchema(`{
	"type": "long"
}`)

	validator, err := NewBuilder().CanReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Found long, expecting int.*"), err)

	validator, err = NewBuilder().CanBeReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)
}

func TestValidatorPromotionIntFloat(t *testing.T) {
	// int -> float promotion

	writer := avro.MustParseSchema(`{
	"type": "int"
}`)
	reader := avro.MustParseSchema(`{
	"type": "float"
}`)

	validator, err := NewBuilder().CanReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Found float, expecting int.*"), err)

	validator, err = NewBuilder().CanBeReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)
}

func TestValidatorPromotionIntDouble(t *testing.T) {
	// int -> double promotion

	writer := avro.MustParseSchema(`{
	"type": "int"
}`)
	reader := avro.MustParseSchema(`{
	"type": "double"
}`)

	validator, err := NewBuilder().CanReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Found double, expecting int.*"), err)

	validator, err = NewBuilder().CanBeReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)
}

func TestValidatorPromotionLongFloat(t *testing.T) {
	// long -> float promotion

	writer := avro.MustParseSchema(`{
	"type": "long"
}`)
	reader := avro.MustParseSchema(`{
	"type": "float"
}`)

	validator, err := NewBuilder().CanReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Found float, expecting long.*"), err)

	validator, err = NewBuilder().CanBeReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)
}

func TestValidatorPromotionLongDouble(t *testing.T) {
	// long -> double promotion

	writer := avro.MustParseSchema(`{
	"type": "long"
}`)
	reader := avro.MustParseSchema(`{
	"type": "double"
}`)

	validator, err := NewBuilder().CanReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Found double, expecting long.*"), err)

	validator, err = NewBuilder().CanBeReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)
}

func TestValidatorPromotionFloatDouble(t *testing.T) {
	// float -> double promotion

	writer := avro.MustParseSchema(`{
	"type": "float"
}`)
	reader := avro.MustParseSchema(`{
	"type": "double"
}`)

	validator, err := NewBuilder().CanReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Regexp(t, regexp.MustCompile(".*Found double, expecting float.*"), err)

	validator, err = NewBuilder().CanBeReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)
}

func TestValidatorPromotionStringBytes(t *testing.T) {
	// string -> bytes -> string promotion

	writer := avro.MustParseSchema(`{
	"type": "string"
}`)
	reader := avro.MustParseSchema(`{
	"type": "bytes"
}`)

	validator, err := NewBuilder().CanReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)

	validator, err = NewBuilder().CanBeReadStrategy().ValidateLatest()
	require.Equal(t, nil, err)
	err = validator.Validate(writer, []avro.Schema{reader})
	assert.Equal(t, nil, err)
}
