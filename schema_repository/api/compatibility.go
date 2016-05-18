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
	"strconv"

	"github.com/julienschmidt/httprouter"
)

type CompatibilityMessage struct {
	IsCompatible bool `json:"is_compatible"`
}

func (as *ApiServer) CheckCompatibility(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	client := ps.ByName("client")
	subject := ps.ByName("subject")
	versionStr := ps.ByName("version")
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		registryError(w, ErrDecoding, http.StatusBadRequest, err)
		return
	}

	defer r.Body.Close()
	var schema SchemaMessage
	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(&schema)
	if err != nil || !schemaValid(schema.Schema) {
		registryError(w, ErrInvalidSchema, 422, err)
	}

	oldSchema, found, err := as.storage.GetSchema(client, subject, version)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}

	if !found {
		registryError(w, ErrSchemaNotFound, http.StatusNotFound, err)
		return
	}

	compatibility, found, _ := as.storage.GetSubjectConfig(client, subject)
	if !found {
		compatibility, _ = as.storage.GetGlobalConfig(client)
	}
	resp := CompatibilityMessage{
		IsCompatible: schemaCompatible(schema.Schema, oldSchema, compatibility), //TODO compatibility
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(resp)
	if err != nil {
		registryError(w, ErrEncoding, http.StatusInternalServerError, err)
	}
}
