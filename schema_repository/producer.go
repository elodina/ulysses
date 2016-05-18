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
	"github.com/elodina/siesta"
	"github.com/elodina/siesta-producer"
	"github.com/elodina/ulysses/logging"
)

func createProducer(brokerList []string) *producer.KafkaProducer {
	connector := createConnector(brokerList)
	producerConfig := producer.NewProducerConfig()
	producerConfig.BatchSize = 1
	producerConfig.ClientID = "ulysses"
	producerConfig.SendRoutines = 2
	producerConfig.ReceiveRoutines = 2
	return producer.NewKafkaProducer(producerConfig, producer.StringSerializer, producer.ByteSerializer, connector)
}

func createConnector(brokerList []string) *siesta.DefaultConnector {
	config := siesta.NewConnectorConfig()
	config.BrokerList = brokerList

	connector, err := siesta.NewDefaultConnector(config)
	if err != nil {
		log.Fatal(err.Error())
	}
	return connector
}
