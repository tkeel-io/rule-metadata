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
	"errors"
	"github.com/tkeel-io/rule-metadata/internal/codec"
	"github.com/tkeel-io/rule-metadata/pkg/types"
	"net/url"
	"strings"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
)

const defaultRuleCodecFactory = "ruleV0.0.2"

func init() {
	registerRule()
}

func registerRule() {
	err := Register(defaultRuleCodecFactory, &defaultRuleFactory{
		keyCodec:   &ruleKeyCodec{},
		valueCodec: &ruleValueCodec{},
	})
	if err != nil {

	}
}

type defaultRuleFactory struct {
	keyCodec   codec.Codec
	valueCodec codec.Codec
}

func (f *defaultRuleFactory) KeyCodec() codec.Codec {
	return f.keyCodec
}

func (f *defaultRuleFactory) ValueCodec() codec.Codec {
	return f.valueCodec
}

type ruleKeyCodec struct {
}

// Marshal
// + key由2部分组成，由 "/" 分隔
//  - Scheme: 前缀,ruleKeyCodec.String()获取
//  - hash: Rule 唯一标识
// + key的限制如下
//  - Scheme 不能包含 "/"
//  - Scheme 代表了如何解析 Key
// + 例如 Key MDMPv1/light%2Flight_001%2Fset/275073F374BAED0C_0
//  - MDMPv1                       #Scheme
//  - light/light_001/set          #Rule 中的 topic_filter
//  - 175073F374BAED0C             #Rule ID
// +
func (c *ruleKeyCodec) Marshal(v interface{}) ([]byte, error) {
	if rule, ok := v.(*metapb.RuleQL); ok {
		if rule.UserId == ""{
			return nil, errors.New("rule's userId is empty")
		}
		key := strings.Join([]string{
			c.String(),
			rule.UserId,
			//url.QueryEscape(rule.TopicFilter),
			rule.Id,
		}, c.Delimiter())
		return []byte(key), nil
	}
	return nil, types.ErrMarshalError
}

func (c *ruleKeyCodec) Unmarshal(data []byte, v interface{}) error {
	if rule, ok := v.(*metapb.RuleQL); ok {
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
		rule.UserId = uid
		//tfs, err := url.QueryUnescape(arr[2])
		//if err != nil {
		//	return errors.ErrUnmarshalError
		//}
		//rule.TopicFilter = tfs
		rule.Id = arr[2]

		return nil
	}
	return types.ErrUnmarshalError
}

func (c *ruleKeyCodec) String() string {
	return defaultRuleCodecFactory
}

func (c *ruleKeyCodec) Delimiter() string {
	return "/"
}

type ruleValueCodec struct {
}

func (c *ruleValueCodec) Marshal(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	return b, err
}

func (c *ruleValueCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (c *ruleValueCodec) String() string {
	return "json"
}
