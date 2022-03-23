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
	"fmt"
	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"reflect"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestRedis(t *testing.T) {
	var err error
	var str string
	Convey("TestRedis", t, func() {
		So(dao, ShouldNotBeNil)
		So(dao.redis, ShouldNotBeNil)
		str, err = dao.redis.Ping().Result()
		t.Log("TestRedis str", str, dao.redis.String())
		So(err, ShouldBeNil)
	})
}

func TestCountSubscriptionSubscriber(t *testing.T) {
	ctx := context.Background()
	id := "ABCDEFGHIJK1234"
	entity := &metapb.Entity{
		Id: id,
	}
	subscription := &metapb.Subscription{
		Id:          "XYZ1234567890",
		TopicFilter: "XYZ1234567890/ABCDEFGHIJK1234/test",
	}
	Convey("CountSubscriptionSubscriber", t, func() {
		//err = dao.DelSubscriptionSubscriber(ctx, subscription, entity)
		//So(err, ShouldBeNil)
		c1, err := dao.CountSubscriptionSubscriber(ctx, subscription)
		So(err, ShouldBeNil)
		err = dao.PutSubscriptionSubscriber(ctx, subscription, entity)
		So(err, ShouldBeNil)
		c2, err := dao.CountSubscriptionSubscriber(ctx, subscription)
		So(err, ShouldBeNil)
		err = dao.DelSubscriptionSubscriber(ctx, subscription, entity)
		So(err, ShouldBeNil)
		c3, err := dao.CountSubscriptionSubscriber(ctx, subscription)
		So(err, ShouldBeNil)
		err = dao.DelSubscriptionSubscriber(ctx, subscription, entity)
		So(err, ShouldBeNil)
		err = dao.DelSubscriptionSubscriber(ctx, subscription, entity)
		So(err, ShouldBeNil)
		So(c2-1, ShouldEqual, c1)
		So(c3, ShouldEqual, c1)
	})
}

func TestPutSubscriptionSubscriber(t *testing.T) {
	var err error
	ctx := context.Background()
	id := "ABCDEFGHIJK1234"
	entity := &metapb.Entity{
		Id: id,
	}
	subscription := &metapb.Subscription{
		Id:          "XYZ1234567890",
		TopicFilter: "XYZ1234567890/ABCDEFGHIJK1234/test",
	}
	Convey("TestPutSubscriptionSubscriber", t, func() {
		err = dao.DelSubscriptionSubscriber(ctx, subscription, entity)
		t.Log("[+]", err)
		So(err, ShouldBeNil)
		c1, err := dao.CountSubscriptionSubscriber(ctx, subscription)
		So(err, ShouldBeNil)
		err = dao.PutSubscriptionSubscriber(ctx, subscription, entity)
		So(err, ShouldBeNil)
		rets, err := dao.GetSubscriptionSubscribers(ctx, subscription)
		So(err, ShouldBeNil)
		So(len(rets), ShouldEqual, c1+1)

		flag := false
		for _, e := range rets {
			if e.Id == id {
				flag = true
				break
			}
		}
		So(flag, ShouldBeTrue)

		//clear
		err = dao.DelSubscriptionSubscriber(ctx, subscription, entity)
		So(err, ShouldBeNil)
	})
}

func TestPutEntity(t *testing.T) {
	ctx := context.Background()
	id := "ABCDEFGHIJK1234"
	entity := &metapb.Entity{
		Id: id,
		DeviceInfo:&metapb.DeviceInfo{
			Addr: "grpc://127.0.0.1/vvvvv",
		},
	}
	Convey("TestPutEntity", t, func() {
		var err error
		_, err = dao.DelEntity(ctx, id)
		ret, err := dao.GetEntity(ctx, id)
		//t.Log(entity, ret)
		err = dao.PutEntity(ctx, entity)
		ret, err = dao.GetEntity(ctx, id)
		//t.Log(entity, ret)
		So(reflect.DeepEqual(ret, entity), ShouldBeTrue)
		So(err, ShouldBeNil)
		So(ret, ShouldNotBeNil)

		//clear
		_, err = dao.DelEntity(ctx, id)
	})
}

func BenchmarkPubsubListSubscriberFromMeta(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx := context.Background()
		ret, err := dao.GetSubscriptionSubscribers(ctx,
			&metapb.Subscription{
				UserId:      "admin",
				Id:          "aaa",
				TopicFilter: fmt.Sprintf("aaa-%d", i),
			})
		if err != nil {
			fmt.Println("err:", len(ret), err)
		}
	}
}

func BenchmarkRedisZScan(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dao.redis.ZScan("subV0.0.1/admin/aaa-1986", 0, "", 200).Result()
	}
}

func BenchmarkRedisZCard(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = dao.redis.ZCard("subV0.0.1/admin/aaa-1986")
	}
}

func TestRedisZScan(t *testing.T) {
	tt := time.Now()
	for i := 0; i < 1*1; i++ {
		keys, cursor, err := dao.redis.ZScan("subV0.0.1/admin/aaa-1986", 0, "", 200).Result()
		fmt.Println(keys, cursor, err, len(keys)%2 != 0)
	}
	fmt.Println(time.Now().After(tt))
}
