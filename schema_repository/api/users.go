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

package api

import (
	"fmt"
	"net/http"

	"github.com/golang/glog"

)

type UserRequest struct {
	Name  string `json:"name"`
	Admin bool   `json:"admin"`
}

func (as *ApiServer) clientFromRequest(r *http.Request) (string, error) {
	if as.storage.Empty() {
		glog.Info("Storage is empty")
		return "admin", nil
	}
	token := r.Header.Get("X-Api-Key")
	if token == "" {
		return "", fmt.Errorf("Token required")
	}
	user, found := as.storage.UserByToken(token)
	if !found {
		return "", fmt.Errorf("User with token %s not found", token)
	}
	return user.Name, nil
}
