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

import "github.com/elodina/go-avro"

type SchemaValidator interface {
	Validate(toValidate avro.Schema, existing []avro.Schema) error
}

type validateAll struct {
	strategy SchemaValidationStrategy
}

func newValidateAll(strategy SchemaValidationStrategy) *validateAll {
	return &validateAll{
		strategy: strategy,
	}
}

func (va *validateAll) Validate(toValidate avro.Schema, schemasInOrder []avro.Schema) error {
	for _, existing := range schemasInOrder {
		err := va.strategy.Validate(toValidate, existing)
		if err != nil {
			return err
		}
	}

	return nil
}

type validateLatest struct {
	strategy SchemaValidationStrategy
}

func newValidateLatest(strategy SchemaValidationStrategy) *validateLatest {
	return &validateLatest{
		strategy: strategy,
	}
}

func (vl *validateLatest) Validate(toValidate avro.Schema, existing []avro.Schema) error {
	if len(existing) > 0 {
		return vl.strategy.Validate(toValidate, existing[0])
	}

	return nil
}
