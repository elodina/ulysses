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
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

type VersionMessage struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

type Schema struct {
	Subject string `json:"subject"`
	ID      int    `json:"id"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

func (as *ApiServer) GetSubjects(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	client := ps.ByName("client")
	subjects, err := as.storage.GetSubjects(client)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(subjects)
	if err != nil {
		log.Printf("Can't encode subjects: %s\n", err)
		registryError(w, ErrEncoding, http.StatusInternalServerError, err)
	}
}

func (as *ApiServer) GetVersionList(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	client := ps.ByName("client")
	versions, found, err := as.storage.GetVersions(client, ps.ByName("subject"))
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}
	if !found {
		registryError(w, ErrSubjectNotFound, http.StatusNotFound, err)
		return
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(versions)
	if err != nil {
		registryError(w, ErrEncoding, http.StatusInternalServerError, err)
		return
	}
}

func (as *ApiServer) GetVersion(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	client := ps.ByName("client")
	subject := ps.ByName("subject")
	versionStr := ps.ByName("version")
	var version int
	var err error
	fmt.Printf("Requested version: %s\n", versionStr)
	if versionStr == "latest" {
		latestSchema, found, _ := as.storage.GetLatestSchema(client, subject)
		if !found {
			registryError(w, ErrSchemaNotFound, http.StatusNotFound, nil)
			return
		}
		version = latestSchema.Version
	} else {
		version, err = strconv.Atoi(versionStr)
		if err != nil {
			registryError(w, ErrDecoding, http.StatusBadRequest, err)
			return
		}
	}
	schema, found, err := as.storage.GetSchema(client, subject, version)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}
	if !found {
		registryError(w, ErrSchemaNotFound, http.StatusNotFound, err)
		return
	}
	resp := VersionMessage{
		Name:    subject,
		Version: version,
		Schema:  schema,
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(resp)
	if err != nil {
		registryError(w, ErrEncoding, http.StatusInternalServerError, err)
		return
	}
}

func (as *ApiServer) NewSchema(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	client := ps.ByName("client")
	subject := ps.ByName("subject")
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	var req SchemaMessage
	err := decoder.Decode(&req)
	if err != nil || !schemaValid(req.Schema) {
		registryError(w, ErrInvalidSchema, 422, err)
		return
	}
	oldSchema, found, err := as.storage.GetLatestSchema(client, subject)
	compatibility, found, _ := as.storage.GetSubjectConfig(client, subject)
	if !found {
		compatibility, _ = as.storage.GetGlobalConfig(client)
	}
	if found && !schemaCompatible(req.Schema, oldSchema.Schema, compatibility) {
		registryError(w, ErrIncompatibleSchema, http.StatusConflict, err)
		return
	}

	id := as.storage.GetID(client, req.Schema)
	if id != -1 {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"id": %d}`, id)))
		return
	}

	id, err = as.storage.StoreSchema(client, subject, req.Schema)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}
	err = as.storage.AddSchema(client, subject, id, req.Schema)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"id": %d}`, id)))
}

func (as *ApiServer) CheckRegistered(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	client := ps.ByName("client")
	subject := ps.ByName("subject")
	schema, found, err := as.storage.GetLatestSchema(client, subject)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}
	if !found {
		registryError(w, ErrSchemaNotFound, http.StatusNotFound, err)
		return
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(schema)
	if err != nil {
		registryError(w, ErrEncoding, http.StatusInternalServerError, err)
		return
	}
}
