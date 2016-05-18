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

package api

import (
	"encoding/json"
	"net/http"

	"github.com/elodina/ulysses/logging"
)

const (
	ErrSchemaNotFound       = "Schema not found"
	ErrInBackendStore       = "Error in the backend datastore"
	ErrAuthStore            = "Error in authorization backend"
	ErrEncoding             = "Error encoding response"
	ErrDecoding             = "Error decoding request"
	ErrSubjectNotFound      = "Subject not found"
	ErrInvalidSchema        = "Invalid Avro schema"
	ErrIncompatibleSchema   = "Incompatible Avro schema"
	ErrInvalidCompatibility = "Invalid compatibility level"
	ErrUnauthorized         = "Client authorization required"
	ErrUserExists           = "User already exists"
)

type ErrorMessage struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func registryError(w http.ResponseWriter, errorMessage string, code int, err error) {
	if err != nil {
		log.Error(err)
	}
	log.Warningf("Registry error: %s, %s", errorMessage, err)
	w.WriteHeader(code)
	mes := &ErrorMessage{
		ErrorCode: code,
		Message:   errorMessage,
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(mes)
	if err != nil {
		log.Errorf("Can't respond with error: %s\n", err)
	}
}
