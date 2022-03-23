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

package endpoint

import (
	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-metadata/internal/utils"
)

var DEFAULT_USERID = "default"

type Endpoint struct {
}

func (s *Endpoint) Logger() log.Factory {
	return utils.Logger
}

func (s *Endpoint) EventType(eventType metapb.EventType) metapb.ResourceObject_EventType {
	if eventType == metapb.EventType_PUT {
		return metapb.ResourceObject_ADDED
	}
	if eventType == metapb.EventType_DELETE {
		return metapb.ResourceObject_DELETED
	}
	panic("unknown event type.")
}

//func (s *Endpoint) CheckAssert(ctx context.Context, assert *metapb.Assert) *metapb.Assert {
//	// userID update
//	userID := DEFAULT_USERID
//	if assert != nil && assert.UserId != "" {
//		userID = assert.UserId
//	} else {
//		assert = &metapb.Assert{
//			UserId: userID,
//		}
//	}
//	s.Logger().For(ctx).Debug("userid",
//		logf.String("userID", assert.UserId),
//	)
//	return assert
//}
