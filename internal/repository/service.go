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
)

type Service struct {
	Entities []string
}
type Entity struct {
	Entity      string //实体
	UserId      string
	TopicFilter string
	Service     string //Service[Device<pipe>、IoTData、Metrics、Alarm]
}

func (rp *Repository) ServiceRange(ctx context.Context) ([]string, error) {
	cfg, err := rp.file.ResourceConfig()
	if err != nil {
		return nil, err
	}
	ret := make([]string, 0)
	for key, _ := range cfg.Services {
		ret = append(ret, key)
	}
	return ret, nil
}

func (rp *Repository) ServiceEntityRange(ctx context.Context, userId, service string) ([]Entity, error) {
	cfg, err := rp.file.ResourceConfig()
	if err != nil {
		return nil, err
	}
	ret := make([]Entity, 0)
	resource, ok := cfg.Resources[userId]
	if !ok {
		return ret, nil
	}
	for _, rt := range resource.Routes {
		ret = append(ret, Entity{
			Entity:      rt.Entity,
			UserId:      rt.UserId,
			TopicFilter: rt.TopicFilter,
			Service:     rt.Service,
		})
	}
	return ret, nil
}
