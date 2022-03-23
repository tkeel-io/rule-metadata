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
	"context"
	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-metadata/pkg/types"
)

func (d *Dao) PutEntity(ctx context.Context, entity *metapb.Entity) (err error) {
	var (
		key    []byte
		values map[string]interface{}
	)

	if key, err = d.entityCodec.KeyCodec().Marshal(entity); err != nil {
		return
	}

	values, err = makeStrMapFromEntity(d.entityCodec, values, entity)
	if err != nil {
		return types.Errorf("entity(%v) to map error:%v", entity, err)
	}

	if err = d.redis.HMSet(string(key), values).Err(); err != nil {
		return types.Errorf("redis hmset(%s, %v) error:%v", key, values, err)
	}
	return nil
}

func (d *Dao) GetEntity(ctx context.Context, entityID string) (entity *metapb.Entity, err error) {
	var (
		key    []byte
		values []interface{}
		maps   map[string]interface{}
	)

	if key, err = d.entityCodec.KeyCodec().Marshal(&metapb.Entity{
		Id: entityID,
	}); err != nil {
		return
	}

	fields := entityFields()
	values, err = d.redis.HMGet(string(key), fields...).Result()

	if len(values) != len(fields) {
		return nil, types.Errorf("len error(%v, %v)", fields, values)
	}

	maps = make(map[string]interface{})
	for i, name := range fields {
		if values[i] != nil {
			maps[name] = values[i]
		}
	}

	entity, err = makeEntityFromStrMap(d.entityCodec, entity, maps)
	if err != nil {
		return nil, types.Errorf("map(%v) to entity error:%v", maps, err)
	}

	return
}

func (d *Dao) DelEntity(ctx context.Context, entityID string) (has bool, err error) {
	var (
		key []byte
		ret int64
	)

	has = false
	if key, err = d.entityCodec.KeyCodec().Marshal(&metapb.Entity{
		Id: entityID,
	}); err != nil {
		return
	}

	ret, err = d.redis.Del(string(key)).Result()
	if ret == 1 {
		has = true
	}

	return
}
