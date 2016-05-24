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
	"fmt"
	"sync"
	"testing"
	"time"

	avro "github.com/elodina/go-avro"
	kafro "github.com/elodina/go-kafka-avro"
	"github.com/elodina/ulysses/schema_repository"
	. "github.com/smartystreets/goconvey/convey"
)

func TestApp(t *testing.T) {
	Convey("Given a schema registry server", t, func() {
		app := schema_repository.NewApp(schema_repository.DefaultRegistryConfig())
		go app.Start()
		Convey("Client should work properly", func() {
			time.Sleep(2 * time.Second) // waiting server to start
			fmt.Println("Creating new client")
			client := kafro.NewCachedSchemaRegistryClient("http://localhost:8081")
			rawSchema := "{\"namespace\": \"net.elodina.kafka.metrics\",\"type\": \"record\",\"name\": \"Timings\",\"fields\": [{\"name\": \"id\", \"type\": \"long\"},{\"name\": \"timings\",  \"type\": {\"type\":\"array\", \"items\": \"long\"} }]}"
			schema, err := avro.ParseSchema(rawSchema)
			So(err, ShouldBeNil)
			id, err := client.Register("test1", schema)
			So(err, ShouldBeNil)
			So(id, ShouldNotEqual, 0)
		})
		SkipConvey("Load testing for race detection", func() {
			Convey("Server should accept load", func() {
				var wg = &sync.WaitGroup{}
				for i := 0; i < 100; i++ {
					wg.Add(1)
					go makeLoad(wg)
				}
				wg.Wait()
			})
		})
	})
}

func makeLoad(wg *sync.WaitGroup) {
	client := kafro.NewCachedSchemaRegistryClient("http://localhost:8081")
	rawSchema := "{\"namespace\": \"net.elodina.kafka.metrics\",\"type\": \"record\",\"name\": \"Timings\",\"fields\": [{\"name\": \"id\", \"type\": \"long\"},{\"name\": \"timings\",  \"type\": {\"type\":\"array\", \"items\": \"long\"} }]}"
	schema, err := avro.ParseSchema(rawSchema)
	_, err = client.Register("test1", schema)
	if err != nil {
		panic(err)
	}
	schema, err = client.GetByID(1)
	if err != nil {
		panic(err)
	}
	client.GetLatestSchemaMetadata("test1")
	client.GetVersion("test1", schema)
	wg.Done()
}
