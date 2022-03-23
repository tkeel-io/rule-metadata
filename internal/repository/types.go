/*
 * Copyright (C) 2019 Yunify, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this work except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file, or at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package repository

import (
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-metadata/internal/conf"
	"github.com/tkeel-io/rule-metadata/internal/repository/dao"
	"time"
)

var timeNow = time.Now

// SubscriptionRepository is use to manager subscription data to etcd
type Repository struct {
	dao  *dao.Dao
	file conf.Matedata
	slot *conf.SlotConfig
}

// NewSubscriptionRepository create new subscription repository
func NewRepository(dao *dao.Dao, slot *conf.SlotConfig, file conf.Matedata) (*Repository, error) {
	if dao == nil {
		log.Fatal("dao is nil")
	}
	return &Repository{
		dao:  dao,
		slot: slot,
		file: file,
	}, nil
}
