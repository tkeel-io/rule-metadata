package dao

import (
	"reflect"
	"testing"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	. "github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestMakeKvSubscriptionValue(t *testing.T) {
	Convey("MakeKvSubscriptionValue", t, func() {
		kv := &mvccpb.KeyValue{
			Key:            []byte(defaultSubscriptionCodecFactory + "/ABCD1234/light%2Flight_001%2Fset"),
			CreateRevision: 1,
			ModRevision:    2,
			Version:        3,
			Value:          []byte("{}"),
		}
		rvt := &metapb.SubscriptionValue{
			CreateRevision: 1,
			ModRevision:    2,
			Version:        3,
			Subscription: &metapb.Subscription{
				UserId:      "ABCD1234",
				TopicFilter: "light/light_001/set",
			},
		}
		cf := getFactory(defaultSubscriptionCodecFactory)
		want, err := makeKvSubscriptionValue(cf, kv)
		So(err, ShouldBeNil)
		So(reflect.DeepEqual(want, rvt), ShouldBeTrue)
	})
}

func TestMakeEvSubscriptionEvent(t *testing.T) {
	Convey("MakeKvSubscriptionValue", t, func() {
		ev := &clientv3.Event{
			Type: mvccpb.DELETE,
			Kv: &mvccpb.KeyValue{
				Key:            []byte(defaultSubscriptionCodecFactory + "/ABCD1234/light%2Flight_001%2Fset"),
				CreateRevision: 1,
				ModRevision:    2,
				Version:        3,
				Value:          []byte("{}"),
			},
		}
		rvt := &metapb.SubscriptionEvent{
			Type: metapb.SubscriptionEvent_DELETE,
			Kv: &metapb.SubscriptionValue{
				CreateRevision: 1,
				ModRevision:    2,
				Version:        3,
				Subscription: &metapb.Subscription{
					UserId:      "ABCD1234",
					TopicFilter: "light/light_001/set",
				},
			},
		}
		cf := getFactory(defaultSubscriptionCodecFactory)
		want, err := makeEvSubscriptionEvent(cf, ev)
		So(err, ShouldBeNil)
		So(reflect.DeepEqual(want, rvt), ShouldBeTrue)
	})
}
