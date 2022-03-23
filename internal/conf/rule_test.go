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

	got *ResourceConfig
	rvs *client
}

func (this *RuleTest) Setup() {
	pwd, _ := os.Getwd()
	rulePath := fmt.Sprintf("%s/../../conf/rule-test.json", pwd)
	got, err := NewClient(rulePath)
	this.So(err, should.BeNil)
	this.So(got, should.NotBeNil)
	this.got = got.ResourceConf
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
		for _, ac := range rv.Rule.Actions {
			this.Assert(ac != nil, fmt.Sprintf("%v", ac))
			this.Assert(ac != nil, fmt.Sprintf("%v", ac))
		}
	}
}
func (this *RuleTest) TestRouteValue() {
	ctx := context.Background()
	respchan := make(chan []*v1.RouteValue, 1024)
	errchan := make(chan error, 1)
	this.rvs.RangeRoute(ctx, respchan, errchan)
	rvs := <-respchan
	for _, rv := range rvs {
		this.Assert(rv != nil, fmt.Sprintf("%v", rv))
	}
}
