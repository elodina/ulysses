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

import "testing"

var testSchema = `{"type": "string"}`
var anotherSchema = `{"type": "enum"}`
var client = "snow"
var subject = "testsubject"

func TestNewInMemoryStorage(t *testing.T) {
	store := NewInMemoryStorage()
	if store == nil {
		t.Fail()
	}
}

func TestAddSchema(t *testing.T) {
	store := NewInMemoryStorage()
	store.AddSchema("snow", "testsubject", 0, testSchema)
	schema, found, err := store.GetSchemaByID("snow", 0)
	if err != nil {
		t.Fail()
	}
	if !found {
		t.Fail()
	}
	if schema != testSchema {
		t.Fail()
	}
}

func TestSetGlobalConfig(t *testing.T) {
	store := NewInMemoryStorage()
	store.SetGlobalConfig("snow", "FULL")
	level, err := store.GetGlobalConfig("snow")
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if level != "FULL" {
		t.Fail()
	}
}

func TestSetSubjectConfig(t *testing.T) {
	store := NewInMemoryStorage()
	store.SetSubjectConfig("snow", "testsubject", "FULL")
	level, found, err := store.GetSubjectConfig("snow", "testsubject")
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if !found {
		t.Log("subject not found")
		t.Fail()
	}
	if level != "FULL" {
		t.Fail()
	}
}

func TestGetSchemaByID(t *testing.T) {
	store := NewInMemoryStorage()
	_, _, err := store.GetSchemaByID("snow", 0)
	if err == nil {
		t.Log("Error expected")
		t.Fail()
	}
	store.AddSchema("snow", "testsubject", 0, testSchema)
	_, found, _ := store.GetSchemaByID("snow", 1)
	if found {
		t.Log("Not found expected")
		t.Fail()
	}
	schema, _, _ := store.GetSchemaByID("snow", 0)
	if schema != testSchema {
		t.Log("Schema don't match")
		t.Fail()
	}
}

func TestGetSubjects(t *testing.T) {
	store := NewInMemoryStorage()
	_, err := store.GetSubjects(client)
	if err == nil {
		t.Log("Error expected")
		t.Fail()
	}
	store.AddSchema(client, "testsubject1", 0, testSchema)
	subjects, err := store.GetSubjects(client)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if len(subjects) != 1 {
		t.Logf("Expeced 1 element, got %d: %v", len(subjects), subjects)
		t.Fail()
	}
	if subjects[0] != "testsubject1" {
		t.Logf("%s != %s", subjects[0], "testsubject1")
		t.Fail()
	}
	store.AddSchema(client, "testsubject2", 1, testSchema)
	subjects, err = store.GetSubjects(client)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if len(subjects) != 2 {
		t.Logf("Expected 2 elements, got %d", len(subjects))
		t.Fail()
	}
	if subjects[1] != "testsubject1" && subjects[1] != "testsubject2" || subjects[0] != "testsubject1" && subjects[0] != "testsubject2" {
		t.Logf("Subjects mismatch: %v", subjects)
		t.Fail()
	}
}

func TestGetVersions(t *testing.T) {
	store := NewInMemoryStorage()
	_, _, err := store.GetVersions(client, subject)
	if err == nil {
		t.Log("Error expected")
		t.Fail()
	}
	store.AddSchema(client, subject, 0, testSchema)
	_, found, _ := store.GetVersions(client, "another")
	if found {
		t.Log("not found expected")
		t.Fail()
	}
	versions, found, err := store.GetVersions(client, subject)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if !found {
		t.Log("subject is lost!")
		t.Fail()
	}
	if len(versions) != 1 {
		t.Logf("Expected 1 version, got %d", len(versions))
		t.Fail()
	}
	if versions[0] != 1 {
		t.Log("Expected first version")
		t.Fail()
	}
	store.AddSchema(client, subject, 1, testSchema)
	versions, found, err = store.GetVersions(client, subject)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if !found {
		t.Log("subject is lost!")
		t.Fail()
	}
	if len(versions) != 2 {
		t.Logf("Expected 1 version, got %d", len(versions))
		t.Fail()
	}
	if versions[0] != 1 && versions[0] != 2 || versions[1] != 1 && versions[1] != 2 {
		t.Log("Expected first version")
		t.Fail()
	}
}

func TestGetSchema(t *testing.T) {
	store := NewInMemoryStorage()
	_, _, err := store.GetSchema(client, subject, 1)
	if err == nil {
		t.Log("Expected error")
		t.Fail()
	}
	store.AddSchema(client, subject, 1, testSchema)
	_, found, _ := store.GetSchema(client, "another", 1)
	if found {
		t.Log("Expected not found")
		t.Fail()
	}
	_, found, _ = store.GetSchema(client, subject, 2)
	if found {
		t.Log("Expected not found")
		t.Fail()
	}
	schema, found, err := store.GetSchema(client, subject, 1)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if !found {
		t.Log("schema not found")
		t.Fail()
	}
	if schema != testSchema {
		t.Log("schema dont match")
		t.Fail()
	}
}

func TestGetLatestSchema(t *testing.T) {
	store := NewInMemoryStorage()
	_, _, err := store.GetLatestSchema(client, subject)
	if err == nil {
		t.Log("error expected")
		t.Fail()
	}
	store.AddSchema(client, subject, 0, testSchema)
	_, found, _ := store.GetLatestSchema(client, "anothersubject")
	if found {
		t.Log("not found expected")
	}
	schema, found, err := store.GetLatestSchema(client, subject)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if !found {
		t.Log("not found")
		t.Fail()
	}
	if schema.Schema != testSchema {
		t.Log("schema don't match")
		t.Fail()
	}
	store.AddSchema(client, subject, 1, anotherSchema)
	schema, _, _ = store.GetLatestSchema(client, subject)
	if schema.Schema != anotherSchema {
		t.Log("it's not the latest schema")
		t.Fail()
	}
	if schema.Version != 2 {
		t.Log("expected second version")
		t.Fail()
	}
}
