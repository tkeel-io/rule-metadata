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
	"context"
	"fmt"
	"time"
)

func (rp *Repository) InitRestore(ctx context.Context, in interface{}) (pubSubSource, pubSubSink, ruleSource string) {
	pubSubSource = rp.slot.PubSubSource
	pubSubSink = rp.slot.PubSubSink
	ruleSource = rp.slot.RuleSource
	return pubSubSource, pubSubSink, ruleSource
}

func (rp *Repository) TopicQueue(service string) string {
	return fmt.Sprintf(rp.slot.PubSubSource, service)
}

func (rp *Repository) PubSubQueue(service string) string {
	return fmt.Sprintf(rp.slot.PubSubSink, service)
}

func (rp *Repository) GetLastRevision(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
	defer cancel()
	return rp.dao.GetLastRevision(ctx)
}
