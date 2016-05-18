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

package log

import (
	"log"
)

// AsyncWriter is asynchronous writer, writes in separate goroutine
type AsyncWriter struct {
	lines chan []byte
}

// NewAsyncWriter creates writer and starts writing routine
func NewAsyncWriter() *AsyncWriter {
	aw := &AsyncWriter{
		lines: make(chan []byte, 100),
	}
	go aw.writerLoop()
	return aw
}

func (aw *AsyncWriter) Write(v []byte) (int, error) {
	aw.lines <- v
	return 0, nil
}

func (aw *AsyncWriter) writerLoop() {
	for {
		line := <-aw.lines
		log.Println(string(line))
	}
}
