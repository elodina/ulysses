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
	"strings"

	"github.com/elodina/ulysses/logging"
	"github.com/gocql/gocql"
)

type CassandraStorage struct {
	connection *gocql.Session
}

func NewCassandraStorage(urls string, protoVersion int, cqlVersion string) *CassandraStorage {
	nodes := strings.Split(urls, ",")
	cluster := gocql.NewCluster(nodes...)
	cluster.CQLVersion = cqlVersion
	cluster.ProtoVersion = protoVersion
	cluster.Keyspace = "avro"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	return &CassandraStorage{
		connection: session,
	}
}

// implement StorageStateReader interface
func (cs *CassandraStorage) Empty() bool {
	var count *int
	err := cs.connection.Query("SELECT COUNT(*) FROM schemas;").Scan(&count)
	if err != nil {
		log.Error(err)
		return false
	}
	return *count == 0
}

func (cs *CassandraStorage) GetID(client string, schema string) int64 {
	iter := cs.connection.Query("SELECT id, avro_schema FROM schemas WHERE client = ?", client).Iter()
	var id *int64
	var storedSchema *string
	for iter.Scan(&id, &storedSchema) {
		if schema == *storedSchema {
			return *id
		}
	}
	return -1
}

func (cs *CassandraStorage) GetSchemaByID(client string, id int64) (string, bool, error) {
	iter := cs.connection.Query("SELECT id, avro_schema FROM schemas WHERE client = ?", client).Iter()
	var storedId *int64
	var schema *string
	for iter.Scan(&storedId, &schema) {
		if id == *storedId {
			return *schema, true, nil
		}
	}
	if err := iter.Close(); err != nil {
		return "", false, err
	}
	return "", false, nil
}

func (cs *CassandraStorage) GetSubjects(client string) ([]string, error) {
	iter := cs.connection.Query("SELECT subject FROM schemas WHERE client = ?", client).Iter()
	subjects := make([]string, 0)
	var subject *string
	for iter.Scan(&subject) {
		subjects = append(subjects, *subject)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return subjects, nil
}

func (cs *CassandraStorage) GetVersions(client string, subject string) ([]int, bool, error) {
	iter := cs.connection.Query("SELECT version FROM schemas WHERE client = ? AND subject = ?", client, subject).Iter()
	versions := make([]int, 0)
	var version *int
	for iter.Scan(&version) {
		versions = append(versions, *version)
	}
	if err := iter.Close(); err != nil {
		return nil, false, err
	}
	return versions, len(versions) > 0, nil
}

func (cs *CassandraStorage) GetSchema(client string, subject string, version int) (string, bool, error) {
	var schema *string
	err := cs.connection.Query("SELECT avro_schema FROM schemas WHERE client = ? AND subject = ? AND version = ?",
		client, subject, version).Consistency(gocql.One).Scan(&schema)
	if err != nil {
		return "", false, err
	}
	return *schema, *schema != "", nil
}

func (cs *CassandraStorage) GetLatestSchema(client string, subject string) (*Schema, bool, error) {
	latest := &Schema{Subject: subject}
	found := false
	iter := cs.connection.Query("SELECT id, avro_schema, version FROM schemas WHERE client = ? AND subject = ?", client, subject).Iter()
	var id *int64
	var schema *string
	var version *int
	for iter.Scan(&id, &schema, &version) {
		found = true
		if *version > latest.Version {
			latest.ID = *id
			latest.Schema = *schema
			latest.Version = *version
		}
	}
	if err := iter.Close(); err != nil {
		return nil, false, err
	}
	return latest, found, nil
}

func (cs *CassandraStorage) GetGlobalConfig(client string) (string, error) {
	var level *string
	err := cs.connection.Query("SELECT level FROM configs WHERE client = ? AND global = true",
		client).Consistency(gocql.One).Scan(&level)
	if err != nil {
		return "", err
	}
	return *level, nil
}

func (cs *CassandraStorage) GetSubjectConfig(client string, subject string) (string, bool, error) {
	var level string
	err := cs.connection.Query("SELECT level FROM configs WHERE client = ? AND global = false AND subject = ?",
		client, subject).Consistency(gocql.One).Scan(&level)
	if err != nil {
		return "", false, err
	}
	return level, true, nil
}

func (cs *CassandraStorage) UserByName(name string) (*User, bool) {
	return nil, false
}

func (cs *CassandraStorage) UserByToken(token string) (*User, bool) {
	return nil, false
}

// implement StorageStateWriter interface
func (cs *CassandraStorage) AddSchema(client string, subject string, id int64, schema string) error {
	versions, found, err := cs.GetVersions(client, subject)
	if err != nil {
		return err
	}
	newVersion := 1
	if found {
		for _, version := range versions {
			if newVersion <= version {
				newVersion = version + 1
			}
		}
	}
	return cs.connection.Query("INSERT INTO schemas (client, subject, version, id, avro_schema) VALUES (?, ?, ?, ?, ?)",
		client, subject, newVersion, id, schema).Exec()
}

func (cs *CassandraStorage) SetGlobalConfig(client string, level string) error {
	return cs.connection.Query("INSERT INTO configs (client, global, subject, level) VALUES (?, true, '', ?)", client, level).Exec()
}

func (cs *CassandraStorage) SetSubjectConfig(client string, subject string, level string) error {
	return cs.connection.Query("INSERT INTO configs (client, global, subject, level) VALUES (?, false, ?, ?)", client, subject, level).Exec()
}

func (cs *CassandraStorage) AddUser(name string, token string, admin bool) error {
	return nil
}
