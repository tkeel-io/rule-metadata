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

package event

import (
	"context"
	"time"

	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/internal/conf"
	"github.com/tkeel-io/rule-util/stream"
	"github.com/tkeel-io/rule-util/stream/checkpoint"
)

const defaultLogPath = "./"

func New(ctx context.Context, c *conf.EventConfig) stream.Source {
	if c == nil || c.Source == "" {
		log.Fatal("cloud event creat error",
			logf.Any("cfg", c))
	}

	checkpoint.InitCoordinator(ctx, 2*time.Second)
	source, err := stream.OpenSource(c.Source)
	if err != nil {
		log.Fatal("cloud event creat error",
			logf.String("Source", c.Source),
			logf.Error(err))
	}
	return source
}
