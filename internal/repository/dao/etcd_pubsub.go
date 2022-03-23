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

package dao

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
)

func (d *Dao) PutSubscription(ctx context.Context, subscription *metapb.Subscription) (err error) {
	var (
		key, body []byte
		resp      *clientv3.PutResponse
	)

	key, err = d.subscriptionCodec.KeyCodec().Marshal(subscription)
	if err != nil {
		d.logger.For(ctx).Error("marshal error",
			logf.Any("resp", resp),
			logf.Error(err))
		return err
	}
	if body, err = d.subscriptionCodec.ValueCodec().Marshal(subscription); err != nil {
		d.logger.For(ctx).Error("marshal error",
			logf.Any("resp", resp),
			logf.Error(err))
		return err
	}

	resp, err = d.etcd.Put(ctx, string(key), string(body))
	if err != nil {
		d.logger.For(ctx).Error("put resp",
			logf.Any("resp", resp),
			logf.Error(err))
		return err
	}

	return nil
}

func (d *Dao) DelSubscription(ctx context.Context, subscription *metapb.Subscription) (err error) {
	var (
		key  []byte
		resp *clientv3.DeleteResponse
	)

	key, err = d.subscriptionCodec.KeyCodec().Marshal(subscription)
	if err != nil {
		d.logger.For(ctx).Error("marshal error",
			logf.Any("resp", resp),
			logf.Error(err))
		return err
	}

	resp, err = d.etcd.Delete(ctx, string(key))
	if err != nil {
		d.logger.For(ctx).Error("del resp",
			logf.Any("resp", resp),
			logf.Error(err))
		return err
	}

	return nil
}

func (d *Dao) HasSubscription(ctx context.Context, subscription *metapb.Subscription) (has bool, err error) {
	var (
		key  []byte
		resp *clientv3.GetResponse
	)

	key, err = d.subscriptionCodec.KeyCodec().Marshal(subscription)
	if err != nil {
		d.logger.For(ctx).Error("marshal error",
			logf.Any("resp", resp),
			logf.Error(err))
		return
	}

	resp, err = d.etcd.Get(ctx, string(key))
	if err != nil {
		d.logger.For(ctx).Error("del resp",
			logf.Any("resp", resp),
			logf.Error(err))
		return
	}

	has = len(resp.Kvs) > 0

	return has, nil
}

func (d *Dao) RangeSubscription(ctx context.Context, rev int64, respchan chan []*metapb.SubscriptionValue, errchan chan error) {
	//data scheme
	scheme := d.subscriptionCodec.KeyCodec().String()
	opts := make([]clientv3.OpOption, 0)
	opts = append(opts, clientv3.WithRange(clientv3.GetPrefixRangeEnd(scheme)))
	opts = append(opts, clientv3.WithRev(rev))
	for {
		d.logger.For(ctx).Info("start get etcd prefix",
			logf.Any("scheme", scheme))
		resp, err := d.etcd.Get(ctx, scheme, opts...)
		if err != nil {
			errchan <- err
			return
		}

		rvs := make([]*metapb.SubscriptionValue, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			subscription, err := makeKvSubscriptionValue(d.subscriptionCodec, kv)
			if err != nil {
				d.logger.For(ctx).Error("makeKvSubscriptionValue",
					logf.Any("kv", kv),
					logf.Error(err))
				continue
			}
			rvs = append(rvs, subscription)
		}

		select {
		case <-ctx.Done(): // Cancel
			return
		case respchan <- rvs:
		}

		if !resp.More {
			return
		}
		// move to next scheme
		scheme = string(append(resp.Kvs[len(resp.Kvs)-1].Key, 0))
	}
}

func (d *Dao) WatchSubscription(ctx context.Context, rev int64, respchan chan []*metapb.SubscriptionEvent, errchan chan error) {
	//data scheme
	scheme := d.subscriptionCodec.KeyCodec().String()

	opts := make([]clientv3.OpOption, 0)
	opts = append(opts, clientv3.WithPrefix())
	opts = append(opts, clientv3.WithRev(rev+1))
	d.logger.For(ctx).Info("start watch etcd",
		logf.String("prefix", scheme),
		logf.Int64("startrevision", rev+1))
	resp := d.etcd.Watch(ctx, scheme, opts...)

	for wr := range resp {
		if len(wr.Events) == 0 {
			//@TODO error handle
			d.logger.For(ctx).Info("compact revision from wr.compactrevision",
				logf.Int64("CompactRevision", wr.CompactRevision))
			return
		}

		rvs := make([]*metapb.SubscriptionEvent, 0, len(wr.Events))
		for _, ev := range wr.Events {
			evt, err := makeEvSubscriptionEvent(d.subscriptionCodec, ev)
			if err != nil {
				d.logger.For(ctx).Error("makeEvSubscriptionEvent",
					logf.Any("Event", ev),
					logf.Error(err))
				//errchan <- err
				//return
				continue
			}
			rvs = append(rvs, evt)
		}

		select {
		case <-ctx.Done(): // Cancel
			return
		case respchan <- rvs:
		}
	}
}
