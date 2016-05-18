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

	"github.com/elodina/ulysses/schema_repository/storage"
	"github.com/elodina/ulysses/schema_repository/validation"
	"github.com/julienschmidt/httprouter"
)

var compatibilityCheckers = map[string]validation.CompatibilityChecker{
	storage.CompatibilityNone:     new(validation.NoneCompatibility),
	storage.CompatibilityBackward: validation.NewBackwardCompatibility(),
	storage.CompatibilityForward:  validation.NewForwardCompatibility(),
	storage.CompatibilityFull:     validation.NewFullCompatibility(),
}

func (as *ApiServer) UpdateGlobalConfig(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	client := ps.ByName("client")
	defer r.Body.Close()
	var config storage.CompatibilityConfig
	decoder := json.NewDecoder(r.Body)
	decoder.Decode(&config)
	if !validCompatibilityLevel(config.Compatibility) {
		registryError(w, ErrInvalidCompatibility, 422, nil)
		return
	}
	err := as.storage.UpdateGlobalConfig(client, config)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(config)
	if err != nil {
		registryError(w, ErrEncoding, http.StatusInternalServerError, err)
		return
	}
}

func (as *ApiServer) GetGlobalConfig(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	client, err := as.clientFromRequest(r)
	if err != nil {
		registryError(w, ErrUnauthorized, http.StatusForbidden, err)
		return
	}
	config, err := as.storage.GetGlobalConfig(client)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(config)
	if err != nil {
		registryError(w, ErrEncoding, http.StatusInternalServerError, err)
		return
	}
}

func (as *ApiServer) UpdateSubjectConfig(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	client, err := as.clientFromRequest(r)
	if err != nil {
		registryError(w, ErrUnauthorized, http.StatusForbidden, err)
		return
	}
	subject := ps.ByName("subject")
	var config storage.CompatibilityConfig
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(&config)
	if err != nil {
		registryError(w, ErrDecoding, http.StatusBadRequest, err)
		return
	}
	if !validCompatibilityLevel(config.Compatibility) {
		registryError(w, ErrInvalidCompatibility, 422, err)
		return
	}
	err = as.storage.UpdateSubjectConfig(client, subject, config)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(config)
	if err != nil {
		registryError(w, ErrEncoding, http.StatusInternalServerError, err)
		return
	}
}

func (as *ApiServer) GetSubjectConfig(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	client, err := as.clientFromRequest(r)
	if err != nil {
		registryError(w, ErrUnauthorized, http.StatusForbidden, err)
		return
	}
	subject := ps.ByName("subject")
	config, found, err := as.storage.GetSubjectConfig(client, subject)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}
	if !found {
		registryError(w, ErrSubjectNotFound, http.StatusNotFound, err)
		return
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(config)
	if err != nil {
		registryError(w, ErrEncoding, http.StatusInternalServerError, err)
		return
	}
}

func validCompatibilityLevel(level string) bool {
	return level == storage.CompatibilityNone ||
		level == storage.CompatibilityFull ||
		level == storage.CompatibilityForward ||
		level == storage.CompatibilityBackward
}
