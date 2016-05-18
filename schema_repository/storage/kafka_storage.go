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
	"encoding/json"
	"fmt"

	"github.com/elodina/siesta"
	"github.com/elodina/siesta-producer"
	"github.com/golang/glog"
)

type MessageType string

const (
	MessageSchema        MessageType = "schema"
	MessageGlobalConfig              = "global-config"
	MessageSubjectConfig             = "subject-config"
	MessageCreateUser                = "create-user"
)

type Sender interface {
	Send(*producer.ProducerRecord) <-chan *producer.RecordMetadata
}

type KafkaStorage struct {
	producer Sender
}

func NewMessageRecord(messageType MessageType, content map[string]string) (*producer.ProducerRecord, error) {
	val, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	return &producer.ProducerRecord{Topic: content["client"], Key: string(messageType), Value: val}, nil
}

func NewKafkaStorage(producer Sender) StorageWriter {
	store := &KafkaStorage{producer: producer}
	return store
}

func (ks *KafkaStorage) StoreSchema(client string, subject string, schema string) (int64, error) {
	glog.Info("StoreSchema invoked")
	content := map[string]string{
		"client":  client,
		"subject": subject,
		"schema":  schema,
	}
	metadata, err := ks.send(MessageSchema, content)
	if err != nil {
		return -1, err
	}
	return metadata.Offset + 1, nil
}

func (ks *KafkaStorage) UpdateGlobalConfig(client string, config CompatibilityConfig) error {
	content := map[string]string{
		"client":        client,
		"compatibility": config.Compatibility,
	}
	_, err := ks.send(MessageGlobalConfig, content)
	return err
}

func (ks *KafkaStorage) UpdateSubjectConfig(client string, subject string, config CompatibilityConfig) error {
	content := map[string]string{
		"client":        client,
		"subject":       subject,
		"compatibility": config.Compatibility,
	}
	_, err := ks.send(MessageSubjectConfig, content)
	return err
}

func (ks *KafkaStorage) CreateUser(name string, token string, admin bool) (string, error) {
	content := map[string]string{
		"client": "admin",
		"name":   name,
		"token":  token,
		"admin":  fmt.Sprintf("%t", true),
	}
	_, err := ks.send(MessageCreateUser, content)
	return token, err
}

func (ks *KafkaStorage) send(messageType MessageType, content map[string]string) (*producer.RecordMetadata, error) {
	glog.Info("Sending to Kafka")
	record, err := NewMessageRecord(messageType, content)
	if err != nil {
		return nil, err
	}
	metadata := <-ks.producer.Send(record)
	glog.Infof("METADATA: %v", *metadata)
	if metadata.Error != siesta.ErrNoError {
		return nil, metadata.Error
	}
	return metadata, nil
}
