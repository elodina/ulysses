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

type StorageMultiwriter struct {
	kafkaWriter     StorageWriter
	cassandraWriter StorageStateWriter
}

func NewStorageMultiwriter(kafkaStorage StorageWriter, cassandraStorage StorageStateWriter) *StorageMultiwriter {
	return &StorageMultiwriter{
		kafkaWriter:     kafkaStorage,
		cassandraWriter: cassandraStorage,
	}
}

func (sm *StorageMultiwriter) StoreSchema(client string, subject string, schema string) (int64, error) {
	id, err := sm.kafkaWriter.StoreSchema(client, subject, schema)
	if err != nil {
		return -1, err
	}
	err = sm.cassandraWriter.AddSchema(client, subject, id, schema)
	return id, err
}

func (sm *StorageMultiwriter) UpdateGlobalConfig(client string, config CompatibilityConfig) error {
	err := sm.kafkaWriter.UpdateGlobalConfig(client, config)
	if err != nil {
		return err
	}
	return sm.cassandraWriter.SetGlobalConfig(client, config.Compatibility)
}

func (sm *StorageMultiwriter) UpdateSubjectConfig(client string, subject string, config CompatibilityConfig) error {
	err := sm.kafkaWriter.UpdateSubjectConfig(client, subject, config)
	if err != nil {
		return err
	}
	return sm.cassandraWriter.SetSubjectConfig(client, subject, config.Compatibility)
}

func (sm *StorageMultiwriter) CreateUser(name string, token string, admin bool) (string, error) {
	return sm.kafkaWriter.CreateUser(name, token, admin)
}
