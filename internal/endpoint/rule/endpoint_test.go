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

package rule

import (
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
	"testing"
)

func TestRuleQL(t *testing.T) {
	//gunit.RunSequential(new(RuleQL), t)
	gunit.Run(new(RuleQL), t)
}

type RuleQL struct {
	*gunit.Fixture
}

func (this *RuleQL) Setup() {
	this.So(nil, should.BeNil)
}

// replace github.com/tkeel-io/rule-util/ruleql => ../ruleql
func (this *RuleQL) Test() {
	Body := `select id as id, params.level as type, '设备认证成功' as name, 'low' as level,params.time as time, params.message as message, params.value as value, deviceid() as device_id, metadata.source as source, userid() as user_id, '' as notice_list from /sys/+/+/thing/event/subOffilne/post`
	expr,err := ruleql.Parse(string(Body))
	this.So(expr, should.NotBeNil)
	this.So(err, should.BeNil)
}
