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
	"net/http"

	"fmt"
	avro "github.com/elodina/go-avro"
	"github.com/elodina/ulysses/auth"
	"github.com/elodina/ulysses/schema_repository/storage"
	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
)

type Watcher interface {
	Watch(string)
}

type SchemaMessage struct {
	Schema string `json:"schema"`
}

type ApiServer struct {
	storage storage.Storage
	address string
	watcher Watcher

	multiuser bool
	topic     string
}

func NewApiServer(addr string, stor storage.Storage, watcher Watcher, multiuser bool, topic string) *ApiServer {
	server := &ApiServer{
		storage:   stor,
		address:   addr,
		watcher:   watcher,
		multiuser: multiuser,
		topic:     topic,
	}
	return server
}

func (as *ApiServer) Start() error {
	router := httprouter.New()
	router.GET("/schemas/ids/:id", as.auth(as.GetSchema))
	router.GET("/subjects", as.auth(as.GetSubjects))
	router.GET("/subjects/:subject/versions", as.auth(as.GetVersionList))
	router.GET("/subjects/:subject/versions/:version", as.auth(as.GetVersion))
	router.POST("/subjects/:subject/versions", as.auth(as.NewSchema))
	router.POST("/subjects/:subject", as.auth(as.CheckRegistered))
	router.POST("/compatibility/subjects/:subject/versions/:version", as.auth(as.CheckCompatibility))
	router.PUT("/config", as.auth(as.UpdateGlobalConfig))
	router.GET("/config", as.auth(as.GetGlobalConfig))
	router.PUT("/config/:subject", as.auth(as.UpdateSubjectConfig))
	router.GET("/config/:subject", as.auth(as.GetSubjectConfig))

	if as.multiuser {
		//router.POST("/users", as.admin(as.auth(as.CreateUser)))
	} else {
		as.watcher.Watch(as.topic)
	}
	fmt.Printf("Starting schema server at %s\n", as.address)
	return http.ListenAndServe(as.address, router)
}

func (as *ApiServer) auth(handler httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		var client string
		fmt.Println("URI:", r.RequestURI)
		if as.multiuser {
			name := r.Header.Get("X-Api-User")
			token := r.Header.Get("X-Api-Key")
			fmt.Println("Token:", token)
			fmt.Println("Name:", name)
			user, ok := as.storage.UserByName(name)
			if !ok || user.Token != token {
				authorized, err := auth.Authorize(name, token)
				if err != nil {
					registryError(w, ErrAuthStore, http.StatusInternalServerError, err)
					return
				}
				if !authorized {
					registryError(w, ErrUnauthorized, http.StatusForbidden, err)
					return
				}

				err = as.storage.AddUser(name, token, true)
				as.watcher.Watch(name)
				if err != nil {
					registryError(w, ErrAuthStore, http.StatusInternalServerError, err)
					return
				}

				as.storage.CreateUser(name, token, true)
			}
			client = name
		} else {
			client = as.topic
		}
		ps = append(ps, httprouter.Param{Key: "client", Value: client})
		handler(w, r, ps)
	}
}

func (as *ApiServer) admin(handler httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		name := r.Header.Get("X-Api-User")
		admin, err := auth.IsAdmin(name)
		if err != nil {
			registryError(w, ErrAuthStore, http.StatusInternalServerError, err)
		}
		if !admin {
			registryError(w, ErrUnauthorized, http.StatusForbidden, nil)
			return
		}
		handler(w, r, ps)
	}
}

func schemaValid(schema string) bool {
	glog.Info("Validating schema %s", schema)
	_, err := avro.ParseSchema(schema)
	if err != nil {
		glog.Info("Schema is invalid: %s", err)
		return false
	}

	return true
}

func schemaCompatible(toValidate string, existing string, compatibilityLevel string) bool {
	schemaToValidate := avro.MustParseSchema(toValidate)
	existingSchema := avro.MustParseSchema(existing)

	checker, ok := compatibilityCheckers[compatibilityLevel]
	if !ok {
		glog.Warning("Compatibility level %s does not exist", compatibilityLevel)
		return false
	}

	err := checker.Validate(schemaToValidate, existingSchema)
	if err != nil {
		glog.Info("Compatibility check for level %s did not pass: %s", compatibilityLevel, err)
		return false
	}

	return true
}
