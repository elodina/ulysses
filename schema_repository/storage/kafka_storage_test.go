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
	"testing"

	"github.com/elodina/siesta"
	"github.com/elodina/siesta-producer"
)

var offset int64 = 1

type MockProducer struct{}

func (ms *MockProducer) Send(*producer.ProducerRecord) <-chan *producer.RecordMetadata {
	metadata := make(chan *producer.RecordMetadata, 1)
	metadata <- &producer.RecordMetadata{Offset: offset, Error: siesta.ErrNoError}
	fmt.Println("Message sent!")
	return metadata
}

func TestNewKafkaStorage(t *testing.T) {
	store := NewKafkaStorage(&MockProducer{})
	if store == nil {
		t.Log("Expected object, got nil")
		t.Fail()
	}
}

func TestStoreSchema(t *testing.T) {
	store := NewKafkaStorage(&MockProducer{})
	id, err := store.StoreSchema(client, subject, testSchema)
	if err != nil {
		t.Log(err)
		t.Fail()
	}
	if id != offset+1 {
		t.Log("expected id == offset + 1")
		t.Fail()
	}
}

func TestUpdateGlobalConfig(t *testing.T) {
	store := NewKafkaStorage(&MockProducer{})
	err := store.UpdateGlobalConfig(client, CompatibilityConfig{Compatibility: "FULL"})
	if err != nil {
		t.Log(err)
		t.Fail()
	}
}

func TestUpdateSubjectConfig(t *testing.T) {
	store := NewKafkaStorage(&MockProducer{})
	err := store.UpdateSubjectConfig(client, subject, CompatibilityConfig{Compatibility: "FULL"})
	if err != nil {
		t.Log(err)
		t.Fail()
	}
}
