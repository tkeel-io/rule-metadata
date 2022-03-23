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

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/pkg/types"
	"github.com/opentracing/opentracing-go"
	olog "github.com/opentracing/opentracing-go/log"
)

func (rp *Repository) Subscribe(ctx context.Context, entity *metapb.Entity, subscriptions []*metapb.Subscription) (err error) {
	for _, subscription := range subscriptions {
		err = rp.dao.PutSubscriptionSubscriber(ctx, subscription, entity)

		if has, err := rp.dao.HasSubscription(ctx, subscription); !has || err == nil {
			err = rp.dao.PutSubscription(ctx, subscription)
		}
	}
	return
}

func (rp *Repository) UnSubscribe(ctx context.Context, entity *metapb.Entity, subscriptions []*metapb.Subscription) (err error) {
	for _, subscription := range subscriptions {
		err = rp.dao.DelSubscriptionSubscriber(ctx, subscription, entity)
		n, _ := rp.dao.CountSubscriptionSubscriber(ctx, subscription)
		if err == nil && n == 0 {
			err = rp.dao.DelSubscription(ctx, subscription)
		}
	}
	return
}

func (rp *Repository) PubSubRestore(ctx context.Context, rev int64) (<-chan []*metapb.SubscriptionValue, chan error) {
	respchan := make(chan []*metapb.SubscriptionValue, 1024)
	errchan := make(chan error, 1)

	// check
	go func(rp *Repository) {
		defer close(respchan)
		defer close(errchan)

		rp.dao.RangeSubscription(ctx, rev, respchan, errchan)
	}(rp)

	return respchan, errchan
}

func (rp *Repository) PubSubWatch(ctx context.Context, rev int64) (<-chan []*metapb.SubscriptionEvent, chan error) {
	respchan := make(chan []*metapb.SubscriptionEvent, 1024)
	errchan := make(chan error, 1)

	go func(rp *Repository) {
		defer close(respchan)
		defer close(errchan)

		rp.dao.WatchSubscription(ctx, rev, respchan, errchan)
	}(rp)

	return respchan, errchan
}

func (rp *Repository) Entities(ctx context.Context, subscription *metapb.Subscription) (out []*metapb.Entity, err error) {

	if subscription == nil {
		return nil, types.ErrSubscriptionNilError
	}

	out, err = rp.dao.GetSubscriptionSubscribers(ctx, subscription)
	if len(out) == 0 {
		err = rp.dao.DelSubscription(ctx, subscription)
		if err != nil {
			return out, types.ErrSubscriptionNilError
		}
	}
	return out, err
}

func (r *Repository) CleanSubscribe(ctx context.Context, entity *metapb.Entity) error {
	if entity.Id == "" {
		return types.ErrEntityNilError
	}
	subscriptions, err := r.dao.GetSubscriberSubscriptions(ctx, entity)

	//tracing
	span, ctx := opentracing.StartSpanFromContext(ctx, "cleanSubscribe")
	defer span.Finish()
	span.LogFields(
		olog.String("entity", entity.String()),
		olog.String("subscriptions", fmt.Sprintf("%v", subscriptions)),
		olog.String("err", fmt.Sprintf("%v", err)),
	)

	if err != nil {
		return types.Errorf("get subscriber's subscriptions error:%v", err)
	}
	for _, subscription := range subscriptions {
		if err := r.UnSubscribe(ctx, entity, []*metapb.Subscription{subscription}); err != nil {
			log.For(ctx).Error("unsubscribe",
				logf.EntityID(entity.Id),
				logf.String("entityAddr", entity.DeviceInfo.Addr))
		} else {
			log.For(ctx).Info("unsubscribe",
				logf.EntityID(entity.Id),
				logf.Any("subscription", subscription))
		}
	}
	return nil
}

func (rp *Repository) CountPubsub(ctx context.Context) int {
	var count int
	respchan := make(chan []*metapb.SubscriptionValue, 1024)
	errchan := make(chan error, 1)

	// check
	go func(rp *Repository) {
		defer close(respchan)
		defer close(errchan)

		rp.dao.RangeSubscription(ctx, 0, respchan, errchan)
	}(rp)

	for res := range respchan {
		count += len(res)
	}
	return count
}
