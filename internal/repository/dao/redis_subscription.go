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
	"github.com/go-redis/redis"
	"time"
)

func (d *Dao) CountSubscriptionSubscriber(ctx context.Context, subscription *metapb.Subscription) (int64, error) {
	key, err := d.subscriptionCodec.KeyCodec().Marshal(subscription)
	if err != nil {
		return 0, err
	}
	size, err := d.redis.ZCard(string(key)).Result()
	if err != nil {
		return 0, err
	}
	return size, nil
}

//PutSubscriptionSubscriber
// 1. Update the entity's list of subscribed subscriptions
// 2. Update the subscription's list of subscriber
func (d *Dao) PutSubscriptionSubscriber(ctx context.Context, subscription *metapb.Subscription, entity *metapb.Entity) (err error) {
	var (
		subscriptionKey []byte
		entityKey       []byte
		resp            int64
		now             = time.Now().UnixNano() / 1000 / 1000
	)

	if entity == nil || entity.Id == "" {
		return types.Errorf("entity error:%v", err)
	}
	if subscription == nil || subscription.TopicFilter == "" {
		return types.Errorf("subscription error:%v", err)
	}

	subscriptionKey, err = d.subscriptionCodec.KeyCodec().Marshal(subscription)
	if err != nil {
		return err
	}
	entityKey, err = d.entityCodec.KeyCodec().Marshal(entity)
	if err != nil {
		return err
	}

	// Update the entity's list of subscribed subscriptions
	if _, err = d.redis.HSet(string(entityKey), string(subscriptionKey), now).Result(); err != nil {
		return types.Errorf("hset: (%v,%v) , error: %v", string(entityKey), string(subscriptionKey), err)
	}
	// Update the subscription's list of subscriber
	if resp, err = d.redis.ZAdd(string(subscriptionKey), redis.Z{
		Member: entity.Id,
		Score:  float64(now),
	}).Result(); err != nil {
		return types.Errorf("zadd resp: %v, error: %v", resp, err)
	}
	return nil
}

//DelSubscriptionSubscriber
// 1. Remove the subscription's list of subscriber
// 2. Remove the entity's list of subscribed subscriptions
func (d *Dao) DelSubscriptionSubscriber(ctx context.Context, subscription *metapb.Subscription, entity *metapb.Entity) (err error) {
	var (
		subscriptionKey, entityKey []byte
		resp                       int64
	)
	if entity == nil || entity.Id == "" {
		return types.Errorf("entity error:%v", err)
	}
	subscriptionKey, err = d.subscriptionCodec.KeyCodec().Marshal(subscription)
	if err != nil {
		return types.Errorf("marshal error:%v", err)
	}
	entityKey, err = d.entityCodec.KeyCodec().Marshal(entity)
	if err != nil {
		return err
	}
	// Remove the subscription's list of subscriber
	if resp, err = d.redis.ZRem(string(subscriptionKey), entity.Id).Result(); err != nil {
		if err == redis.Nil {
			return nil
		}
		return types.Errorf("zrem resp: %v, error: %v", resp, err)
	}

	// Remove the entity's list of subscribed subscriptions
	if resp, err = d.redis.HDel(string(entityKey), string(subscriptionKey)).Result(); err != nil {
		if err == redis.Nil {
			return nil
		}
		return types.Errorf("zrem resp: %v, error: %v", resp, err)
	}

	return nil
}

func (d *Dao) GetSubscriptionSubscribers(ctx context.Context, subscription *metapb.Subscription) (subscribers []*metapb.Entity, err error) {

	var (
		body []byte
		keys []string
	)

	if body, err = d.subscriptionCodec.KeyCodec().Marshal(subscription); err != nil {
		return
	}

	cursor := uint64(0)
	count := int64(200)
	key := string(body)
	subscribers = make([]*metapb.Entity, 0)
	for {
		keys, cursor, err = d.redis.ZScan(key, cursor, "", count).Result()
		if len(keys)%2 != 0 {
			return subscribers, types.Errorf("redis zscan fail")
		}
		for i := 0; i < len(keys); i = i + 2 {
			member := keys[i]
			//score := keys[i+1]
			entity := metapb.Entity{}
			if member == "" {
				continue
			}
			entity.Id = member
			subscribers = append(subscribers, &entity)
		}
		if cursor == 0 {
			break
		}
	}
	return
}

func (d *Dao) GetSubscriberSubscriptions(ctx context.Context, subscriber *metapb.Entity) (subscriptions []*metapb.Subscription, err error) {
	var (
		key  []byte
		keys []string
	)

	if subscriber == nil || subscriber.Id == "" {
		return nil, types.Errorf("entity error:%v", err)
	}

	if key, err = d.entityCodec.KeyCodec().Marshal(&metapb.Entity{
		Id: subscriber.Id,
	}); err != nil {
		return
	}

	cursor := uint64(0)
	count := int64(200)
	subscriptions = make([]*metapb.Subscription, 0)
	schema := d.subscriptionCodec.KeyCodec().String() + "*"
	for {
		keys, cursor, err = d.redis.HScan(string(key), cursor, schema, count).Result()
		if len(keys)%2 != 0 {
			return nil, types.Errorf("redis zscan fail")
		}
		for i := 0; i < len(keys); i = i + 2 {
			subscriptionID := keys[i]
			var subscription = new(metapb.Subscription)
			if err = d.subscriptionCodec.KeyCodec().Unmarshal([]byte(subscriptionID), subscription); err != nil {
				return nil, err
			}
			subscriptions = append(subscriptions, subscription)
		}
		if cursor == 0 {
			break
		}
	}

	return
}
