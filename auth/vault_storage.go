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
	"fmt"
	"os"

	vault "github.com/hashicorp/vault/api"
	"github.com/elodina/ulysses/logging"
)

type AuthVaultStorage struct {
	vault *vault.Logical
}

func NewAuthVaultStorage(url string) *AuthVaultStorage {
	avs := &AuthVaultStorage{}
	vaultConfig := vault.DefaultConfig()
	vaultConfig.Address = url

	client, err := vault.NewClient(vaultConfig)
	if err != nil {
		log.Fatalf("Error connecting to Vault: %s", err)
	}
	token := os.Getenv("VAULT_TOKEN")
	if token == "" {
		log.Fatal("VAULT_TOKEN should not be empty")
	}
	client.SetToken(token)
	avs.vault = client.Logical()
	return avs
}

func (avs *AuthVaultStorage) Authorize(name string, token string) (bool, error) {
	secret, err := avs.vault.Read(pathForUser(name))
	if err != nil || secret == nil {
		return false, fmt.Errorf("Can't get the secret for user %s: %s", name, err)
	}
	log.Infof("[AuthVaultStorage] Secret: %v", secret)
	log.Infof("[AuthVaultStorage] Data: %v", secret.Data)
	return secret.Data["token"] == token, nil
}

func (avs *AuthVaultStorage) AddUser(name string, admin bool) (string, error) {
	apiKey := generateApiKey()
	data := map[string]interface{}{"token": apiKey, "admin": admin}
	_, err := avs.vault.Write(pathForUser(name), data)
	if err != nil {
		log.Errorf("Can't create user %s: %s", name, err)
		return "", err
	}
	return apiKey, nil
}

func (avs *AuthVaultStorage) RefreshToken(name string) (string, error) {
	apiKey := generateApiKey()
	secret, err := avs.vault.Read(pathForUser(name))
	if err != nil {
		return "", err
	}
	secret.Data["token"] = apiKey
	_, err = avs.vault.Write(pathForUser(name), secret.Data)
	if err != nil {
		log.Errorf("Can't write new token for user %s: %s", name, err)
		return "", err
	}
	return apiKey, nil
}

func (avs *AuthVaultStorage) IsAdmin(name string) (bool, error) {
	secret, err := avs.vault.Read(pathForUser(name))
	if err != nil {
		return false, err
	}
	return secret.Data["admin"].(bool), nil
}

func pathForUser(username string) string {
	return fmt.Sprintf("secret/token/%s", username)
}
