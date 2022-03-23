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
	"encoding/json"
	"fmt"
	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"reflect"
	"testing"

	"github.com/smartystreets/assertions/should"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/smartystreets/gunit"
)

func TestEtcdRuleUNIT(t *testing.T) {
	gunit.RunSequential(new(EtcdRuleUNIT), t)
	//gunit.Run(new(EtcdRuleUNIT), t)
}

type EtcdRuleUNIT struct {
	*gunit.Fixture
	rule *metapb.RuleQL
}

func (this *EtcdRuleUNIT) Setup() {
	this.So(nil, should.BeNil)
	this.rule = &metapb.RuleQL{
		Id:          "rule-1",
		UserId:      "*",
		TopicFilter: "/abcd/#",
		Body:        []byte(`select *, userid() as user_id, deviceid() as device_id from /abcd/#`),
		Actions: []*metapb.Action{
			&metapb.Action{
				Id:   "action-1",
				Type: "republish",
				Metadata: map[string]string{
					"option": `{
						"topic": "/aaabbbccc1/1-{{topic(1)}}/2-{{topic(2)}}/4444"
					}`,
				},
			},
			&metapb.Action{
				Id:   "action-2",
				Type: "republish",
				Metadata: map[string]string{
					"option": `{
						"topic": "/aaabbbccc2/1-{{topic(1)}}/2-{{topic(2)}}/4444"
					}`,
				},
			},
			&metapb.Action{
				Id:   "action-3",
				Type: "republish",
				Metadata: map[string]string{
					"option": `{
						"topic": "/aaabbbccc2/1-{{topic(1)}}/2-{{topic(2)}}/4444"
					}`,
				},
			},
			&metapb.Action{
				Id:   "action-3",
				Type: "republish",
				Body: []byte(`{
					"sink": "qmq://192.168.0.23:2379/mdmp-device-export/router.v0.0.1.prod",
				}`),
			},
		},
	}
}

func (this *EtcdRuleUNIT) TestCreatRule_revision() {
	rule := this.rule
	b, _ := json.Marshal(rule)
	fmt.Println(string(b))
	rev, err := dao.GetRuleRevision(ctx)
	this.So(rev, ShouldBeGreaterThan, 0)
	this.So(err, ShouldBeNil)

	err = dao.PutRule(ctx, rule)
	this.So(err, ShouldBeNil)

	oldRev := rev
	rev, err = dao.GetRuleRevision(ctx)
	this.So(err, ShouldBeNil)
	this.So(rev, ShouldEqual, oldRev+1)
}

func (this *EtcdRuleUNIT) TestCreatRule_range() {
	rule := this.rule
	b, _ := json.Marshal(rule)
	fmt.Println(string(b))
	rev, err := dao.GetRuleRevision(ctx)
	this.So(err, ShouldBeNil)
	this.So(rev, ShouldBeGreaterThan, 0)
	oldRev := rev

	//Put
	err = dao.PutRule(ctx, rule)
	this.So(err, ShouldBeNil)

	// check
	flag := this.checkRangeRule(rule)
	this.So(flag, ShouldBeTrue)

	//Del
	err = dao.DelRule(ctx, rule)
	this.So(err, ShouldBeNil)

	// check
	flag = this.checkRangeRule(rule)
	this.So(flag, ShouldBeFalse)

	rev, err = dao.GetRuleRevision(ctx)
	this.So(err, ShouldBeNil)
	this.So(rev, ShouldEqual, oldRev+2)

}

func (this *EtcdRuleUNIT) checkRangeRule(rule *metapb.RuleQL) bool {
	flag := false
	respchan := make(chan []*metapb.RuleValue, 1024)
	errchan := make(chan error, 1)
	go func() {
		defer close(respchan)
		defer close(errchan)
		dao.RangeRule(ctx, 0, respchan, errchan)
	}()
	for kvs := range respchan {
		for _, kv := range kvs {
			if reflect.DeepEqual(rule, kv.Rule) {
				flag = true
				break
			}
		}
	}
	return flag
}