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
	"time"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
)

func (rp *Repository) NewRoute(ctx context.Context, rule *metapb.Route) (err error) {
	if has, err := rp.dao.HasRoute(ctx, rule); !has || err == nil {
		err = rp.dao.PutRoute(ctx, rule)
	}
	return
}

func (rp *Repository) DelRoute(ctx context.Context, rule *metapb.Route) (err error) {
	err = rp.dao.DelRoute(ctx, rule)
	return
}

func (rp *Repository) GetRouteRevision(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
	defer cancel()
	return rp.dao.GetRouteRevision(ctx)
}

func (rp *Repository) RouteRange(ctx context.Context, rev int64) (<-chan []*metapb.RouteValue, chan error) {
	respchan := make(chan []*metapb.RouteValue, 1024)
	errchan := make(chan error, 1)

	// check
	go func(rp *Repository) {
		defer close(respchan)
		defer close(errchan)

		if rp.file != nil {
			rp.file.RangeRoute(ctx, respchan, errchan) // TODO remove
		}

		rp.dao.RangeRoute(ctx, rev, respchan, errchan)
	}(rp)

	return respchan, errchan
}

func (rp *Repository) RouteWatch(ctx context.Context, rev int64) (<-chan []*metapb.RouteEvent, chan error) {
	respchan := make(chan []*metapb.RouteEvent, 1024)
	errchan := make(chan error, 1)

	go func(rp *Repository) {
		defer close(respchan)
		defer close(errchan)

		rp.dao.WatchRoute(ctx, rev, respchan, errchan)
	}(rp)

	return respchan, errchan
}

func (rp *Repository) CountRoute(ctx context.Context) int {
	var count int
	respchan := make(chan []*metapb.RouteValue, 1024)
	errchan := make(chan error, 1)

	ctx, cancel := context.WithTimeout(ctx, 1000*time.Millisecond)
	defer cancel()
	rev, _ := rp.dao.GetRouteRevision(ctx)
	// check
	go func(rp *Repository) {
		defer close(respchan)
		defer close(errchan)

		if rp.file != nil {
			rp.file.RangeRoute(ctx, respchan, errchan) // TODO remove
		}

		rp.dao.RangeRoute(ctx, rev, respchan, errchan)
	}(rp)

	for res := range respchan {
		count += len(res)
	}
	return count
}
