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

package storage

import (
	"fmt"
	"sync"
)

type ClientSchemas map[int64]string
type ClientSubjects map[string]Versions
type Versions map[int]int64
type SubjectConfigs map[string]string

type InMemoryStorage struct {
	schemas      map[string]ClientSchemas
	subjects     map[string]ClientSubjects
	configs      map[string]SubjectConfigs
	globalConfig map[string]string
	users        map[string]*User

	empty bool
	mutex *sync.RWMutex
}

func NewInMemoryStorage() *InMemoryStorage {
	store := &InMemoryStorage{
		schemas:      make(map[string]ClientSchemas),
		subjects:     make(map[string]ClientSubjects),
		configs:      make(map[string]SubjectConfigs),
		globalConfig: make(map[string]string),
		users:        make(map[string]*User),
		empty:        true,
		mutex:        &sync.RWMutex{},
	}
	return store
}

func (ims *InMemoryStorage) Empty() bool {
	return ims.empty
}

func (ims *InMemoryStorage) UserByName(name string) (*User, bool) {
	ims.mutex.RLock()
	defer ims.mutex.RUnlock()
	for _, user := range ims.users {
		if user.Name == name {
			return user, true
		}
	}
	return nil, false
}

func (ims *InMemoryStorage) UserByToken(name string) (*User, bool) {
	ims.mutex.RLock()
	defer ims.mutex.RUnlock()
	if user, ok := ims.users[name]; ok {
		return user, true
	}
	return nil, false
}

func (ims *InMemoryStorage) AddUser(name string, token string, admin bool) error {
	ims.mutex.Lock()
	defer ims.mutex.Unlock()
	if ims.empty && !admin {
		return fmt.Errorf("First user should be an admin")
	}
	if ims.empty {
		ims.empty = false
	}
	ims.users[token] = &User{Name: name, Token: token, Admin: admin}
	return nil
}

func (ims *InMemoryStorage) GetID(client string, schema string) int64 {
	ims.mutex.RLock()
	defer ims.mutex.RUnlock()

	for id, clientSchema := range ims.schemas[client] {
		if clientSchema == schema {
			return id
		}
	}

	return -1
}

func (ims *InMemoryStorage) GetSchemaByID(client string, id int64) (string, bool, error) {
	ims.mutex.RLock()
	defer ims.mutex.RUnlock()
	fmt.Printf("GetSchemaByID(%s, %d)\n", client, id)
	fmt.Printf("schemas: %v\n", ims.schemas)

	if clientSchema, ok := ims.schemas[client]; ok {
		if schema, found := clientSchema[id]; found {
			return schema, true, nil
		} else {
			return "", false, nil
		}
	}
	return "", false, clientNotFoundError(client)
}

func (ims *InMemoryStorage) GetSubjects(client string) ([]string, error) {
	ims.mutex.RLock()
	defer ims.mutex.RUnlock()
	if clientSubjects, ok := ims.subjects[client]; ok {
		subjects := make([]string, 0, len(clientSubjects))
		for subject, _ := range clientSubjects {
			subjects = append(subjects, subject)
		}
		return subjects, nil
	}
	return nil, clientNotFoundError(client)
}

func (ims *InMemoryStorage) GetVersions(client string, subject string) ([]int, bool, error) {
	ims.mutex.RLock()
	defer ims.mutex.RUnlock()
	if clientSubjects, ok := ims.subjects[client]; ok {
		if clientVersions, found := clientSubjects[subject]; found {
			versions := make([]int, 0, len(clientVersions))
			for version, _ := range clientVersions {
				versions = append(versions, version)
			}
			return versions, true, nil
		}
		return nil, false, nil
	}
	return nil, false, clientNotFoundError(client)
}

func (ims *InMemoryStorage) GetSchema(client string, subject string, version int) (string, bool, error) {
	ims.mutex.RLock()
	defer ims.mutex.RUnlock()
	if clientSubjects, ok := ims.subjects[client]; ok {
		if clientVersions, subjectFound := clientSubjects[subject]; subjectFound {
			if id, idFound := clientVersions[version]; idFound {
				if schema, schemaFound := ims.schemas[client][id]; schemaFound { // TODO: check if client in schemas
					return schema, true, nil
				}
				return "", false, inconsistentSchemaError(id)
			}
			return "", false, nil
		}
		return "", false, nil
	}
	return "", false, clientNotFoundError(client)
}

func (ims *InMemoryStorage) GetLatestSchema(client string, subject string) (*Schema, bool, error) {
	ims.mutex.RLock()
	defer ims.mutex.RUnlock()
	if clientSubjects, ok := ims.subjects[client]; ok {
		if clientVersions, subjectFound := clientSubjects[subject]; subjectFound {
			if len(clientVersions) == 0 {
				return nil, false, nil
			}

			id, version := latestVersion(clientVersions)

			if schemaStr, schemaFound := ims.schemas[client][id]; schemaFound { // TODO: check if client in schemas
				schema := &Schema{
					Subject: subject,
					ID:      id,
					Version: version,
					Schema:  schemaStr,
				}
				return schema, true, nil
			}

			return nil, false, inconsistentSchemaError(id)
		}
		return nil, false, nil
	}
	return nil, false, clientNotFoundError(client)
}

func (ims *InMemoryStorage) GetGlobalConfig(client string) (string, error) {
	ims.mutex.RLock()
	defer ims.mutex.RUnlock()
	if clientConfig, ok := ims.globalConfig[client]; ok {
		return clientConfig, nil
	}
	return "", clientNotFoundError(client)
}

func (ims *InMemoryStorage) GetSubjectConfig(client string, subject string) (string, bool, error) {
	ims.mutex.RLock()
	defer ims.mutex.RUnlock()
	if clientConfigs, ok := ims.configs[client]; ok {
		if subjectConfig, found := clientConfigs[subject]; found {
			return subjectConfig, true, nil
		}
		return "", false, nil
	}
	return "", false, clientNotFoundError(client)
}

func (ims *InMemoryStorage) AddSchema(client string, subject string, id int64, schema string) error {
	ims.mutex.Lock()
	defer ims.mutex.Unlock()
	if _, ok := ims.schemas[client]; !ok {
		ims.schemas[client] = make(ClientSchemas)
	}
	if _, ok := ims.schemas[client][id]; ok {
		return nil
	}
	ims.schemas[client][id] = schema

	if _, ok := ims.subjects[client]; !ok {
		ims.subjects[client] = make(ClientSubjects)
	}
	if _, ok := ims.subjects[client][subject]; !ok {
		ims.subjects[client][subject] = make(Versions)
	}

	version := 0
	if len(ims.subjects[client][subject]) == 0 {
		version = 1
	} else {
		_, v := latestVersion(ims.subjects[client][subject])
		version = v + 1
	}
	ims.subjects[client][subject][version] = id

	return nil
}

func (ims *InMemoryStorage) SetGlobalConfig(client string, level string) error {
	ims.mutex.Lock()
	defer ims.mutex.Unlock()
	ims.globalConfig[client] = level
	return nil
}

func (ims *InMemoryStorage) SetSubjectConfig(client string, subject string, level string) error {
	ims.mutex.Lock()
	defer ims.mutex.Unlock()
	if _, ok := ims.configs[client]; !ok {
		ims.configs[client] = make(SubjectConfigs)
	}
	ims.configs[client][subject] = level
	return nil
}

func latestVersion(versions Versions) (int64, int) {
	var schemaId int64
	maxVersion := -1
	for version, id := range versions {
		if version > maxVersion {
			maxVersion = version
			schemaId = id
		}
	}
	return schemaId, maxVersion
}

func inconsistentSchemaError(id int64) error {
	return fmt.Errorf("Inconsistent schema id in subjects: %d", id)
}

func clientNotFoundError(client string) error {
	return fmt.Errorf("Client %s not found in storage", client)
}
