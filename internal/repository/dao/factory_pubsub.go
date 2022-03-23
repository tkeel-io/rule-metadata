/*
 * Copyright (c) 2019. LuCongyao <6congyao@gmail.com> .
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
	"encoding/json"
	"github.com/tkeel-io/rule-metadata/internal/codec"
	"github.com/tkeel-io/rule-metadata/pkg/types"
	"net/url"
	"strings"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
)

const defaultEntityCodecFactory = "entityV1"
const defaultSubscriptionCodecFactory = "subV0.0.1"

func init() {
	registerEntity()
	registerSubscription()
}

func registerEntity() {
	err := Register(defaultEntityCodecFactory, &defaultEntityFactory{
		keyCodec:   &entityKeyCodec{},
		valueCodec: &entityValueCodec{},
	})
	if err != nil {

	}
}

func registerSubscription() {
	err := Register(defaultSubscriptionCodecFactory, &defaultSubscriptionFactory{
		keyCodec:   &subscriptionKeyCodec{},
		valueCodec: &subscriptionValueCodec{},
	})
	if err != nil {

	}
}

type defaultSubscriptionFactory struct {
	keyCodec   codec.Codec
	valueCodec codec.Codec
}

func (f *defaultSubscriptionFactory) KeyCodec() codec.Codec {
	return f.keyCodec
}

func (f *defaultSubscriptionFactory) ValueCodec() codec.Codec {
	return f.valueCodec
}

type subscriptionKeyCodec struct {
}

// Marshal
// + key由2部分组成，由 "/" 分隔
//  - Scheme: 前缀,subscriptionKeyCodec.String()获取
//  - URIEscape(topic_filter): 编码后的 topic_filter
//  - hash: Subscription 唯一标识
// + key的限制如下
//  - Scheme 不能包含 "/"
//  - Scheme 代表了如何解析 Key
// + 例如 Key MDMPv1/light%2Flight_001%2Fset/275073F374BAED0C_0
//  - MDMPv1                       #Scheme
//  - light/light_001/set          #Subscription 中的 topic_filter
//  - 175073F374BAED0C             #Subscription ID
// +
func (c *subscriptionKeyCodec) Marshal(v interface{}) ([]byte, error) {
	if subscription, ok := v.(*metapb.Subscription); ok {
		key := strings.Join([]string{
			c.String(),
			url.QueryEscape(subscription.UserId),
			url.QueryEscape(subscription.TopicFilter),
		}, c.Delimiter())
		return []byte(key), nil
	}
	return nil, types.ErrMarshalError
}

func (c *subscriptionKeyCodec) Unmarshal(data []byte, v interface{}) error {
	if subscription, ok := v.(*metapb.Subscription); ok {
		arr := strings.Split(string(data), c.Delimiter())
		if len(arr) != 3 {
			return types.ErrUnmarshalError
		}
		if arr[0] != c.String() {
			return types.ErrUnmarshalError
		}
		uid, err := url.QueryUnescape(arr[1])
		if err != nil {
			return types.ErrUnmarshalError
		}
		subscription.UserId = uid
		tfs, err := url.QueryUnescape(arr[2])
		if err != nil {
			return types.ErrUnmarshalError
		}
		subscription.TopicFilter = tfs

		return nil
	}
	return types.ErrUnmarshalError
}

func (c *subscriptionKeyCodec) String() string {
	return defaultSubscriptionCodecFactory
}

func (c *subscriptionKeyCodec) Delimiter() string {
	return "/"
}

type subscriptionValueCodec struct {
}

func (c *subscriptionValueCodec) Marshal(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	return b, err
}

func (c *subscriptionValueCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (c *subscriptionValueCodec) String() string {
	return "json"
}

type defaultEntityFactory struct {
	keyCodec   codec.Codec
	valueCodec codec.Codec
}

func (f *defaultEntityFactory) KeyCodec() codec.Codec {
	return f.keyCodec
}

func (f *defaultEntityFactory) ValueCodec() codec.Codec {
	return f.valueCodec
}

type entityKeyCodec struct {
}

// Marshal
// + Entity 数据采用Hset存储
// + Key 类似于 [scheme]:[entityID]
func (c *entityKeyCodec) Marshal(v interface{}) ([]byte, error) {
	if entity, ok := v.(*metapb.Entity); ok {
		key := strings.Join([]string{
			c.String(),
			url.QueryEscape(entity.Id),
		}, c.Delimiter())
		return []byte(key), nil
	}
	return nil, types.ErrMarshalError
}

func (c *entityKeyCodec) Unmarshal(data []byte, v interface{}) error {
	if entity, ok := v.(*metapb.Entity); ok {
		arr := strings.Split(string(data), c.Delimiter())
		if len(arr) != 2 {
			return types.ErrUnmarshalError
		}
		if arr[0] != c.String() {
			return types.ErrUnmarshalError
		}
		id, err := url.QueryUnescape(arr[1])
		if err != nil {
			return types.ErrUnmarshalError
		}
		entity.Id = id
		return nil
	}
	return types.ErrUnmarshalError
}

func (c *entityKeyCodec) String() string {
	return defaultEntityCodecFactory
}

func (c *entityKeyCodec) Delimiter() string {
	return ":"
}

type entityValueCodec struct {
}

// Marshal
// + Entity 数据采用Hset存储
// + Key 类似于 [scheme]:[entityID]
// + Value 存储前需要先处理
func (c *entityValueCodec) Marshal(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	return b, err
}

func (c *entityValueCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (c *entityValueCodec) String() string {
	return "json"
}
