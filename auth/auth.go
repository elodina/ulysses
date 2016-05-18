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

package auth

import (
	uuid "github.com/satori/go.uuid"
	"github.com/elodina/ulysses/logging"
)

var Storage AuthStorage = NewAuthInMemoryStorage()

func InitStorage(vaultUrl string, vaultToken string) {
	if vaultUrl != "" && vaultToken != "" {
		Storage = NewAuthVaultStorage(vaultUrl)
	} else {
		log.Infof("Vault URL (%s) of VAULT_TOKEN (%s) is empty, continue with in memory storage", vaultUrl, vaultToken)
	}
}

func Authorize(name string, token string) (bool, error) {
	return Storage.Authorize(name, token)
}

func AddUser(name string, admin bool) (string, error) {
	return Storage.AddUser(name, admin)
}

func IsAdmin(name string) (bool, error) {
	return Storage.IsAdmin(name)
}

func RefreshToken(name string) (string, error) {
	return Storage.RefreshToken(name)
}

func generateApiKey() string {
	return uuid.NewV4().String()
}
