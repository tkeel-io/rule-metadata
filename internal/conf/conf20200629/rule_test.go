package conf

import (
	"context"
	"fmt"
	"github.com/tkeel-io/rule-util/metadata/v1"
	"os"
	"testing"

	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
)

func TestRule(t *testing.T) {
	gunit.Run(new(RuleTest), t)
}

type RuleTest struct {
	*gunit.Fixture
	ruleConfPath string

	got *RuleConfig
	rvs *client
}

func (this *RuleTest) Setup() {
	pwd, _ := os.Getwd()
	rulePath := fmt.Sprintf("%s/../../conf/rule-example.json", pwd)
	got, err := NewClient(rulePath)
	this.So(err, should.BeNil)
	this.So(got, should.NotBeNil)
	this.got = got.RuleConf
	this.rvs = got
}
func (this *RuleTest) Teardown() {
}

func (this *RuleTest) TestLoadRuleTomlFile() {

}

func (this *RuleTest) TestRuleValue() {
	ctx := context.Background()
	respchan := make(chan []*v1.RuleValue, 1024)
	errchan := make(chan error, 1)
	this.rvs.RangeRule(ctx, respchan, errchan)
	rvs := <-respchan
	for _, rv := range rvs {
		this.Assert(rv != nil, fmt.Sprintf("%v", rv))
		this.Assert(len(rv.Rule.Actions) > 0, fmt.Sprintf("%v", rv))
		for key, ac := range rv.Rule.Actions {
			this.Assert(ac != nil, fmt.Sprintf("%v", ac))
			this.Assert(ac != nil, fmt.Sprintf("%v", ac))
			this.Assert(ac.Sink != "", fmt.Sprintf("%v:%v", key, ac))
		}
	}
}

func (this *RuleTest) TestRuleEntity() {
	for key, ac := range this.got.Entities {
		this.Assert(key != "", key)
		this.Assert(ac != nil, fmt.Sprintf("%v", ac))
	}
}
