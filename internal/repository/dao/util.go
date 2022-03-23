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
	metapb "github.com/tkeel-io/rule-util/metadata/v1"
)

const (
	//Entity Info
	ENTITY_ID           = "id"
	ENTITY_STATUS       = "status"
	ENTITY_DEVICE_INFO  = "device_info"
	ENTITY_ENTITY_INFO = "entity_info"
	ENTITY_REFRESH_TIME = "refresh_time"
)

//type EtcdHandle interface {
//	MakeValue(kv *mvccpb.KeyValue)
//	MakeEvent(ev *clientv3.Event)
//}
//
//type EtcdValueHandle func(kv []*mvccpb.KeyValue) (error)
//type EtcdEventHandle func(ev []*clientv3.Event) (error)
//
//type subscriptionChannel struct {
//	cf       CodecFactory
//	respchan chan []*metapb.SubscriptionEvent
//	errchan  chan error
//}
//
//func NewSubscriptionUtil(cf CodecFactory, respchan chan []*metapb.SubscriptionValue, errchan chan error) EtcdHandle {
//	return &subscriptionChannel{
//		cf:       cf,
//		respchan: respchan,
//		errchan:  errchan,
//	}
//}
//
//func (subscriptionChannel) MakeValue(kv *mvccpb.KeyValue) {
//	panic("implement me")
//}
//
//func (subscriptionChannel) MakeEvent(ev *clientv3.Event) {
//	panic("implement me")
//}
//
//func SubscriptionHabdel(cf CodecFactory, respchan chan []*metapb.SubscriptionValue) EtcdValueHandle {
//	return func(kvs []*mvccpb.KeyValue) (error) {
//		rvs := make([]*metapb.SubscriptionValue, 0, len(kvs))
//		for _, kv := range kvs {
//			subscription, err := makeKvSubscriptionValue(d.subscriptionCodec, kv)
//			if err != nil {
//				log.Errorf("makeKvSubscriptionValue(%v) error : %v", kv, err)
//				continue
//			}
//			rvs = append(rvs, subscription)
//		}
//
//		select {
//		case <-ctx.Done(): // Cancel
//			return
//		case respchan <- rvs:
//		}
//	}
//}

//@TODO use proto generate
func entityFields() []string {
	return []string{
		ENTITY_ID,
		ENTITY_STATUS,
		ENTITY_DEVICE_INFO,
		ENTITY_ENTITY_INFO,
		ENTITY_REFRESH_TIME,
	}
}

func makeStrMapFromEntity(cf CodecFactory, values map[string]interface{}, et *metapb.Entity) (map[string]interface{}, error) {
	var ret interface{}
	var err error
	if values == nil {
		values = make(map[string]interface{})
	}

	if ret, err = cf.ValueCodec().Marshal(et.Id); err != nil {
	} else {
		values[ENTITY_ID] = ret
	}

	if ret, err = cf.ValueCodec().Marshal(et.Status); err != nil {
	} else {
		values[ENTITY_STATUS] = ret
	}

	if ret, err = cf.ValueCodec().Marshal(et.DeviceInfo); err != nil {
	} else {
		values[ENTITY_DEVICE_INFO] = ret
	}

	if ret, err = cf.ValueCodec().Marshal(et.EntityInfo); err != nil {
	} else {
		values[ENTITY_ENTITY_INFO] = ret
	}

	if ret, err = cf.ValueCodec().Marshal(et.RefreshTime); err != nil {
	} else {
		values[ENTITY_REFRESH_TIME] = ret
	}

	return values, nil
}

func makeEntityFromStrMap(cf CodecFactory, entity *metapb.Entity, values map[string]interface{}) (*metapb.Entity, error) {
	var err error
	if len(values) == 0 {
		return nil, nil
	}
	if entity == nil {
		entity = new(metapb.Entity)
	}

	for k, v := range values {
		if s, ok := v.(string); ok {
			e := []byte(s)
			switch k {
			case ENTITY_ID:
				if err = cf.ValueCodec().Unmarshal(e, &(entity.Id)); err != nil {
				}
			case ENTITY_STATUS:
				if err = cf.ValueCodec().Unmarshal(e, &(entity.Status)); err != nil {
				}
			case ENTITY_DEVICE_INFO:
				if err = cf.ValueCodec().Unmarshal(e, &(entity.DeviceInfo)); err == nil {
				}
			case ENTITY_ENTITY_INFO:
				if err = cf.ValueCodec().Unmarshal(e, &(entity.EntityInfo)); err == nil {
				}
			case ENTITY_REFRESH_TIME:
				if err = cf.ValueCodec().Unmarshal(e, &(entity.RefreshTime)); err != nil {
				}
			}
		}

	}

	return entity, nil
}
