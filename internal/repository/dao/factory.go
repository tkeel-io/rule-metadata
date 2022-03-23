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
	"fmt"
	"github.com/tkeel-io/rule-metadata/internal/codec"
	"github.com/tkeel-io/rule-metadata/pkg/types"
)

type CodecFactory interface {
	KeyCodec() codec.Codec
	ValueCodec() codec.Codec
}

var factories = map[string]CodecFactory{}

// Register registers an extension.
func Register(name string, factory CodecFactory) error {
	if _, ok := factories[name]; ok {
		return types.Errorf("%s entityCodec factory is already registered", name)
	}
	factories[name] = factory
	return nil
}

func getFactory(name string) CodecFactory {
	if factory, ok := factories[name]; ok {
		return factory
	}
	panic(fmt.Sprintf("factory (%v) not found", name))
}
