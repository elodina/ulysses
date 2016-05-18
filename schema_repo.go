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

package main

import (
 "flag"
 "strings"

 "github.com/elodina/ulysses/schema_repository"
 "github.com/elodina/ulysses/logging"
)

var (
 brokers      = flag.String("brokers", "localhost:9092", "Kafka broker list")
 topic        = flag.String("topic", "schemas", "Kafka topic")
 port         = flag.Int("port", 8081, "HTTP port to listen")
 cassandra    = flag.String("cassandra", "", "Cassandra nodes")
 protoVersion = flag.Int("proto-version", 3, "Cassandra protocol version")
 cqlVersion   = flag.String("cql-version", "3.0.0", "Cassandra CQL version")
)

func main() {
 flag.Parse()
 registryConfig := schema_repository.DefaultRegistryConfig()
 registryConfig.Brokers = strings.Split(*brokers, ",")
 registryConfig.Cassandra = *cassandra
 registryConfig.Port = *port
 registryConfig.Topic = *topic
 app := schema_repository.NewApp(registryConfig)
 err := app.Start()
 if err != nil {
  log.Fatal(err)
 }
}