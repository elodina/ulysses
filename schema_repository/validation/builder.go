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

import "errors"

type SchemaValidatorBuilder struct {
	strategy SchemaValidationStrategy
}

func NewBuilder() *SchemaValidatorBuilder {
	return new(SchemaValidatorBuilder)
}

func (svb *SchemaValidatorBuilder) CanReadStrategy() *SchemaValidatorBuilder {
	svb.strategy = new(validateCanRead)
	return svb
}

func (svb *SchemaValidatorBuilder) CanBeReadStrategy() *SchemaValidatorBuilder {
	svb.strategy = new(validateCanBeRead)
	return svb
}

func (svb *SchemaValidatorBuilder) MutualReadStrategy() *SchemaValidatorBuilder {
	svb.strategy = new(validateMutualRead)
	return svb
}

func (svb *SchemaValidatorBuilder) ValidateLatest() (SchemaValidator, error) {
	return newValidateLatest(svb.strategy), svb.valid()
}

func (svb *SchemaValidatorBuilder) ValidateAll() (SchemaValidator, error) {
	return newValidateAll(svb.strategy), svb.valid()
}

func (svb *SchemaValidatorBuilder) valid() error {
	if svb.strategy == nil {
		return errors.New("SchemaValidationStrategy not specified in builder")
	}

	return nil
}
