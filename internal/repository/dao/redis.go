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

package dao

import (
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/internal/conf"
	"github.com/go-redis/redis"
	"time"
)

// NewRedisDB create new redis db
func NewRedisDB(c *conf.RedisConfig) (*redis.Client, error) {

	option := redis.Options{
		Addr: c.Addr,
	}

	if c.MaxActive > 0 {
		option.PoolSize = int(c.MaxActive)
	}

	client := redis.NewClient(&redis.Options{
		Addr:         c.Addr,
		Password:     c.Auth,
		PoolSize:     c.MaxActive,
		DB:           c.DB,
		DialTimeout:  time.Duration(c.DialTimeout),
		ReadTimeout:  time.Duration(c.ReadTimeout),
		WriteTimeout: time.Duration(c.WriteTimeout),
		IdleTimeout:  time.Duration(c.IdleTimeout),
		PoolTimeout:  time.Duration(c.PoolTimeout),
	})
	_, err := client.Ping().Result()
	if err != nil {
		log.Error("connect to redis fail",
			logf.Any("addr", c.Addr),
			logf.Any("db", c.DB),
			logf.Error(err))
		log.Fatal("connect to redis fail")
	} else {
		//log.InfoWithFields("connect to redis success addr:%s,db:%d", log.Fields{"redis": pong})
		log.Info("connect to redis success",
			logf.Any("addr", c.Addr),
			logf.Any("db", c.DB))
	}

	return client, nil
}
