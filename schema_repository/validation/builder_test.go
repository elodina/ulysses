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

	"gopkg.in/stretchr/testify.v1/assert"
	"gopkg.in/stretchr/testify.v1/require"
)

func TestSchemaValidatorBuilder(t *testing.T) {
	_, err := NewBuilder().ValidateAll()
	require.NotEqual(t, err, nil)
	assert.Regexp(t, regexp.MustCompile("SchemaValidationStrategy not specified in builder"), err.Error())

	_, err = NewBuilder().ValidateLatest()
	require.NotEqual(t, err, nil)
	assert.Regexp(t, regexp.MustCompile("SchemaValidationStrategy not specified in builder"), err.Error())

	validator, err := NewBuilder().CanBeReadStrategy().ValidateAll()
	require.Equal(t, err, nil)
	assert.IsType(t, new(validateAll), validator)
	assert.IsType(t, new(validateCanBeRead), validator.(*validateAll).strategy)

	validator, err = NewBuilder().CanReadStrategy().ValidateAll()
	require.Equal(t, err, nil)
	assert.IsType(t, new(validateAll), validator)
	assert.IsType(t, new(validateCanRead), validator.(*validateAll).strategy)

	validator, err = NewBuilder().MutualReadStrategy().ValidateAll()
	require.Equal(t, err, nil)
	assert.IsType(t, new(validateAll), validator)
	assert.IsType(t, new(validateMutualRead), validator.(*validateAll).strategy)

	validator, err = NewBuilder().CanBeReadStrategy().ValidateLatest()
	require.Equal(t, err, nil)
	assert.IsType(t, new(validateLatest), validator)
	assert.IsType(t, new(validateCanBeRead), validator.(*validateLatest).strategy)

	validator, err = NewBuilder().CanReadStrategy().ValidateLatest()
	require.Equal(t, err, nil)
	assert.IsType(t, new(validateLatest), validator)
	assert.IsType(t, new(validateCanRead), validator.(*validateLatest).strategy)

	validator, err = NewBuilder().MutualReadStrategy().ValidateLatest()
	require.Equal(t, err, nil)
	assert.IsType(t, new(validateLatest), validator)
	assert.IsType(t, new(validateMutualRead), validator.(*validateLatest).strategy)
}
