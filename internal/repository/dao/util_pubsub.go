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
	"github.com/tkeel-io/rule-metadata/pkg/types"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func makeKvSubscriptionValue(cf CodecFactory, kv *mvccpb.KeyValue) (*metapb.SubscriptionValue, error) {
	subscription := new(metapb.Subscription)
	if err := cf.KeyCodec().Unmarshal(kv.Key, subscription); err != nil {
		return nil, err
	}
	if err := cf.ValueCodec().Unmarshal(kv.Value, subscription); err != nil {
		return nil, err
	}
	rv := &metapb.SubscriptionValue{
		Subscription:    subscription,
		ResourceVersion: kv.ModRevision,
	}
	return rv, nil
}

func makeEvSubscriptionEvent(cf CodecFactory, ev *clientv3.Event) (*metapb.SubscriptionEvent, error) {
	var err error
	var subscription = new(metapb.Subscription)
	var rtype metapb.EventType
	var kv = ev.Kv

	if kv.Key == nil {
		return nil, types.Errorf("event key is nil")
	}

	if err = cf.KeyCodec().Unmarshal(kv.Key, subscription); err != nil {
		return nil, err
	}

	switch ev.Type {
	case mvccpb.PUT:
		rtype = metapb.EventType_PUT
		if err = cf.ValueCodec().Unmarshal(kv.Value, subscription); err != nil {
			return nil, err
		}
	case mvccpb.DELETE:
		rtype = metapb.EventType_DELETE
	}

	re := &metapb.SubscriptionEvent{
		Type: rtype,
		Kv: &metapb.SubscriptionValue{
			Subscription:    subscription,
			ResourceVersion: kv.ModRevision,
		},
	}

	return re, nil
}
