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
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/pkg/types"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
)

func (r *Repository) GetEntity(ctx context.Context, entityID string) (entity *metapb.Entity, err error) {
	entity, err = r.dao.GetEntity(ctx, entityID)
	if entity == nil && err == nil {
		// TODO Don't read Entity from file
		//if r.file != nil {
		//	entity, _ = r.file.Entity(ctx, entityID) //
		//}
	}
	return
}

func (r *Repository) Connect(ctx context.Context, entity *metapb.Entity) (err error) {
	log.For(ctx).Debug("Connect",
		logf.EntityID(entity.Id),
		logf.Any("entity", entity))
	if entity.Id == "" {
		return types.ErrEntityNilError
	}
	entity.Status = metapb.Entity_Online
	entity.RefreshTime = timeNow().UnixNano() / 1000000
	err = r.dao.PutEntity(ctx, entity)
	return
}

func (r *Repository) Disconnect(ctx context.Context, entity *metapb.Entity) (err error) {
	if entity.Id == "" {
		return types.ErrEntityNilError
	}
	entity.Status = metapb.Entity_Offline
	entity.RefreshTime = timeNow().UnixNano() / 1000000
	err = r.dao.PutEntity(ctx, entity)
	return
}
