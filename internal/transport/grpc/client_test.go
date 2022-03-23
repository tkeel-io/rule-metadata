package grpc

import (
	"context"
	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/pkg/registry/etcd3"
	"io"
	"testing"
	"time"
)

import (
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/gunit"
)

const ERROR_VERSION = int64(-1)

func TestRule(t *testing.T) {
	log.InitLogger("IotHub", "rule")
	gunit.RunSequential(new(RuleAction), t)
	//gunit.Run(new(Rule), t)
}

type RuleAction struct {
	*gunit.Fixture
	discovery *etcdv3.Discovery
	cli       metapb.RuleActionClient
	evt       metapb.JobManagerClient
}

func (this *RuleAction) Setup() {
	this.So(nil, should.BeNil)
	this.discovery = NewDiscovery("127.0.0.1:2379")
	this.cli = NewRuleClient(this.discovery)
	this.evt = NewJobManagerClient(this.discovery)
	this.evt = NewRulexNodeActionClient(this.discovery)
}

func (this *RuleAction) Test() {
	ctx := context.Background()
	resp, err := this.cli.AddRouteTable(ctx, &metapb.RouteTableRequest{
		PacketId: 1235,
		Rule: &metapb.RuleQL{
			Id:     "rule_test_001",
			UserId: "usr-test001",
			Body:   []byte(`select *, userid() as user_id from /aaa`),
		},
		DstTopic: "/bb",
	})
	this.So(err, should.BeNil)
	this.So(resp.PacketId, should.Equal, 1235)
	this.So(this.Find(ctx, "rule_test_001"), should.BeTrue)

	resp, err = this.cli.DelRouteTable(ctx, &metapb.RouteTableRequest{
		PacketId: 1236,
		Rule: &metapb.RuleQL{
			Id:     "rule_test_001",
			UserId: "usr-test001",
			Body:   []byte(`select *, userid() as user_id from /aaa`),
		},
		DstTopic: "/bb",
	})
	this.So(err, should.BeNil)
	this.So(resp.PacketId, should.Equal, 1236)
	this.So(this.Find(ctx, "rule_test_001"), should.BeFalse)
}

func (this *RuleAction) Find(ctx context.Context, ID string) bool {
	sm, err := this.evt.Restore(ctx, &metapb.RestoreRequest{})
	this.So(err, should.BeNil)
	for {
		var rev = ERROR_VERSION
		resp, err := sm.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.For(ctx).Error("recv error",
				logf.Error(err))
			time.Sleep(time.Second)
			continue
		}
		switch body := resp.Body.(type) {
		case *metapb.RestoreResponse_InitConfig:
			continue
		case *metapb.RestoreResponse_Resource:
			continue
		case *metapb.RestoreResponse_Subscription:
			rsp := body.Subscription
			rev = rsp.StartRevision

		case *metapb.RestoreResponse_Rules:
			rsp := body.Rules
			rev = rsp.StartRevision
			for _, rule := range rsp.Rules {
				if rule.Rule.Id == ID {
					//log.Error("rsp", logf.Any("rule", rule.Rule.Id), logf.Any("rule", rule.Rule.Body))
					return true
				}
			}
		}
		if rev == ERROR_VERSION {
			log.For(ctx).Error(
				"recv error(unknown type)",
				logf.Any("resp", resp),
			)
			break
		}
	}
	return false
}
