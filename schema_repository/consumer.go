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

package schema_repository

import (
	"encoding/json"

	"github.com/elodina/gonzo"
	"github.com/elodina/ulysses/schema_repository/storage"
	"github.com/elodina/siesta"
	"github.com/elodina/ulysses/logging"
)

type Consumer struct {
	consumer gonzo.Consumer
	storage  storage.StorageStateWriter
}

func NewConsumer(brokerList []string, store storage.StorageStateWriter, multiuser bool) *Consumer {
	c := &Consumer{storage: store}
	config := siesta.NewConnectorConfig()
	config.FetchMinBytes = 1
	config.BrokerList = brokerList
	client, err := siesta.NewDefaultConnector(config)
	if err != nil {
		panic(err)
	}
	consumerConfig := gonzo.NewConsumerConfig()
	consumerConfig.Group = "ulysses-group"
	consumerConfig.AutoCommitEnable = false
	c.consumer = gonzo.NewConsumer(client, consumerConfig, c.consumerStrategy)
	if multiuser {
		c.consumer.Add("admin", 0)
	}
	return c
}

func (c *Consumer) Watch(topic string) {
	c.consumer.Add(topic, 0)
}

func (c *Consumer) Join() {
	c.consumer.Join()
}

func (c *Consumer) consumerStrategy(data *gonzo.FetchData, _ *gonzo.KafkaPartitionConsumer) {
	log.Info("Consumer strategy invoked")
	if data.Error != nil {
		log.Errorf("[Consumer] Fetch error: %s\n", data.Error)
	}

	for _, msg := range data.Messages {
		var err error
		log.Info(string(msg.Value))
		content := messageContent(msg.Value)
		switch storage.MessageType(msg.Key) {
		case storage.MessageSchema:
			err = c.storage.AddSchema(content["client"], content["subject"], msg.Offset+1, content["schema"])
		case storage.MessageGlobalConfig:
			err = c.storage.SetGlobalConfig(content["client"], content["compatibility"])
		case storage.MessageSubjectConfig:
			err = c.storage.SetSubjectConfig(content["client"], content["subject"], content["compatibility"])
		case storage.MessageCreateUser:
			err = c.storage.AddUser(content["name"], content["token"], content["admin"] == "true")
		default:
			log.Error("[Consumer] Unexpected message type")
		}
		if err != nil {
			log.Errorf("[Consumer] %s", err)
		}
	}
}

func messageContent(message []byte) map[string]string {
	var content map[string]string
	err := json.Unmarshal(message, &content)
	if err != nil {
		log.Fatal(err.Error())
	}
	return content
}
