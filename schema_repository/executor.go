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
	"os"

	"github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/elodina/ulysses/logging"
)

type SchemaExecutor struct {
	app *App
}

func NewExecutor(config SchemaRegistryConfig) *SchemaExecutor {
	app := NewApp(config)
	return &SchemaExecutor{app: app}
}

func (se *SchemaExecutor) LaunchTask(driver executor.ExecutorDriver, task *mesos.TaskInfo) {
	log.Infof("[LaunchTask] %s", task)

	runStatus := &mesos.TaskStatus{
		TaskId: task.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}

	if _, err := driver.SendStatusUpdate(runStatus); err != nil {
		log.Errorf("Failed to send status update: %s", runStatus)
		os.Exit(1) //TODO not sure if we should exit in this case, but probably yes
	}

	go func() {
		log.Info("Starting schema application")
		se.app.Start()
		// finish task
		log.Infof("Finishing task %s", task.GetName())
		finStatus := &mesos.TaskStatus{
			TaskId: task.GetTaskId(),
			State:  mesos.TaskState_TASK_FINISHED.Enum(),
		}
		if _, err := driver.SendStatusUpdate(finStatus); err != nil {
			log.Errorf("Failed to send status update: %s", finStatus)
			os.Exit(1)
		}
		log.Infof("Task %s has finished", task.GetName())
	}()
}

func (*SchemaExecutor) Registered(executor.ExecutorDriver, *mesos.ExecutorInfo, *mesos.FrameworkInfo, *mesos.SlaveInfo) {
}
func (*SchemaExecutor) Disconnected(executor.ExecutorDriver)                   {}
func (*SchemaExecutor) Reregistered(executor.ExecutorDriver, *mesos.SlaveInfo) {}
func (se *SchemaExecutor) KillTask(executor.ExecutorDriver, *mesos.TaskID) {
	se.app.Stop()
}
func (*SchemaExecutor) FrameworkMessage(executor.ExecutorDriver, string) {}
func (se *SchemaExecutor) Shutdown(executor.ExecutorDriver) {
	se.app.Stop()
}
func (*SchemaExecutor) Error(executor.ExecutorDriver, string) {}
