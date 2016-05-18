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
	"sync"
)

type AuthInMemoryStorage struct {
	sync.RWMutex
	users map[string]*User
}

func NewAuthInMemoryStorage() *AuthInMemoryStorage {
	return &AuthInMemoryStorage{users: make(map[string]*User)}
}

func (ams *AuthInMemoryStorage) Authorize(name string, token string) (bool, error) {
	ams.RLock()
	defer ams.RUnlock()

	return ams.users[name].Token == token, nil
}

func (ams *AuthInMemoryStorage) AddUser(name string, admin bool) (string, error) {
	ams.Lock()
	defer ams.Unlock()

	token := generateApiKey()
	ams.users[name] = &User{Name: name, Token: token, Admin: admin}
	return token, nil
}

func (ams *AuthInMemoryStorage) RefreshToken(name string) (string, error) {
	ams.Lock()
	defer ams.Unlock()

	if ams.users[name] == nil {
		return "", fmt.Errorf("User %s does not exist", name)
	}
	ams.users[name].Token = generateApiKey()
	return ams.users[name].Token, nil
}

func (ams *AuthInMemoryStorage) IsAdmin(name string) (bool, error) {
	ams.RLock()
	defer ams.RUnlock()

	if ams.users[name] == nil {
		return false, fmt.Errorf("User %s does not exist", name)
	}
	return ams.users[name].Admin, nil
}
