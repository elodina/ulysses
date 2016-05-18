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

type CompatibilityChecker interface {
	Validate(toValidate avro.Schema, existing avro.Schema) error
}

type NoneCompatibility struct{}

func (nc *NoneCompatibility) Validate(toValidate avro.Schema, existing avro.Schema) error {
	return nil
}

type BackwardCompatibility struct {
	validator SchemaValidator
}

func NewBackwardCompatibility() *BackwardCompatibility {
	validator, err := NewBuilder().CanReadStrategy().ValidateLatest()
	if err != nil {
		panic(err)
	}

	return &BackwardCompatibility{
		validator: validator,
	}
}

func (bc *BackwardCompatibility) Validate(toValidate avro.Schema, existing avro.Schema) error {
	return bc.validator.Validate(toValidate, []avro.Schema{existing})
}

type ForwardCompatibility struct {
	validator SchemaValidator
}

func NewForwardCompatibility() *ForwardCompatibility {
	validator, err := NewBuilder().CanBeReadStrategy().ValidateLatest()
	if err != nil {
		panic(err)
	}

	return &ForwardCompatibility{
		validator: validator,
	}
}

func (fc *ForwardCompatibility) Validate(toValidate avro.Schema, existing avro.Schema) error {
	return fc.validator.Validate(toValidate, []avro.Schema{existing})
}

type FullCompatibility struct {
	validator SchemaValidator
}

func NewFullCompatibility() *FullCompatibility {
	validator, err := NewBuilder().MutualReadStrategy().ValidateLatest()
	if err != nil {
		panic(err)
	}

	return &FullCompatibility{
		validator: validator,
	}
}

func (fc *FullCompatibility) Validate(toValidate avro.Schema, existing avro.Schema) error {
	return fc.validator.Validate(toValidate, []avro.Schema{existing})
}
