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
	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"reflect"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestEtcdSubscription(t *testing.T) {
	var err error
	var rev, rev2 int64
	subscription := &metapb.Subscription{
		Id:          "XYZ1234567890",
		UserId:      "uid-001",
		TopicFilter: "XYZ1234567890/ABCDEFGHIJK1234/test",
	}
	Convey("Test Put & Del", t, func() {

		Convey("GetSubscriptionRevision", func() {
			rev, err = dao.GetSubscriptionRevision(ctx)
			So(err, ShouldBeNil)
			So(rev, ShouldBeGreaterThan, 0)

			err = dao.PutSubscription(ctx, subscription)
			So(err, ShouldBeNil)

			rev2, err = dao.GetSubscriptionRevision(ctx)
			So(err, ShouldBeNil)
			So(rev2, ShouldEqual, rev+1)
		})
		Convey("Put", func() {
			rev, err = dao.GetSubscriptionRevision(ctx)
			So(err, ShouldBeNil)
			So(rev, ShouldBeGreaterThan, 0)

			err = dao.PutSubscription(ctx, subscription)
			So(err, ShouldBeNil)

			rev2, err = dao.GetSubscriptionRevision(ctx)
			So(err, ShouldBeNil)
			So(rev2, ShouldEqual, rev+1)

			flag := false
			respchan := make(chan []*metapb.SubscriptionValue, 1024)
			errchan := make(chan error, 1)

			// check
			go func() {
				defer close(respchan)
				defer close(errchan)
				dao.RangeSubscription(ctx, 0, respchan, errchan)
			}()

			for kvs := range respchan {
				for _, kv := range kvs {
					if reflect.DeepEqual(subscription, kv.Subscription) {
						flag = true
						break
					}
				}
			}
			So(flag, ShouldBeTrue)
			So(err, ShouldBeNil)
		})

		Convey("Del", func() {

			err = dao.DelSubscription(ctx, subscription)
			So(err, ShouldBeNil)

			flag := false
			respchan := make(chan []*metapb.SubscriptionValue, 1024)
			errchan := make(chan error, 1)

			// check
			go func() {
				defer close(respchan)
				defer close(errchan)
				dao.RangeSubscription(ctx, 0, respchan, errchan)
			}()

			for kvs := range respchan {
				for _, kv := range kvs {
					if reflect.DeepEqual(subscription, kv.Subscription) {
						flag = true
						break
					}
				}
			}
			So(flag, ShouldBeFalse)
			So(err, ShouldBeNil)
		})
	})
}

func TestEtcdHasSubscription(t *testing.T) {
	var err error
	var has bool
	subscription := &metapb.Subscription{
		Id:          "XYZ1234567890",
		TopicFilter: "XYZ1234567890/ABCDEFGHIJK1234/test",
	}
	Convey("Test HasSubscription", t, func() {
		Convey("HasSubscription", func() {
			err = dao.DelSubscription(ctx, subscription)
			has, err = dao.HasSubscription(ctx, subscription)
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)

			err = dao.PutSubscription(ctx, subscription)
			has, err = dao.HasSubscription(ctx, subscription)
			So(err, ShouldBeNil)
			So(has, ShouldBeTrue)

			//clear
			err = dao.DelSubscription(ctx, subscription)
			has, err = dao.HasSubscription(ctx, subscription)
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)
		})
	})
}
