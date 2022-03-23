package dao

import (
	"reflect"
	"testing"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
)

func TestDefaultSubscriptionCodecFactoryMarshal(t *testing.T) {
	codec := defaultSubscriptionCodecFactory
	cf := getFactory(codec)
	tests := []struct {
		name   string
		Id     string
		UserId string
		Topic  string
		Ret    []byte
		Want   bool
	}{
		{"1",
			"275073F374BAED0C",
			"user1",
			"light/light_001/set",
			[]byte(defaultSubscriptionCodecFactory + "/user1/light%2Flight_001%2Fset"),
			true},
		{"2",
			"1111111",
			"user1",
			"/light/light_001/set",
			[]byte(defaultSubscriptionCodecFactory + "/user1/%2Flight%2Flight_001%2Fset"),
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscription := &metapb.Subscription{
				Id:          tt.Id,
				UserId:      tt.UserId,
				TopicFilter: tt.Topic,
			}
			ret, err := cf.KeyCodec().Marshal(subscription)
			if err != nil || reflect.DeepEqual(ret, tt.Ret) != tt.Want {
				t.Errorf("[%v]err  %v", tt.name, err)
				t.Errorf("ret = %v == %v, want %v", string(ret), string(tt.Ret), tt.Want)
			}
		})
	}
}

func TestDefaultSubscriptionCodecFactoryUnmarshal(t *testing.T) {
	codec := defaultSubscriptionCodecFactory
	cf := getFactory(codec)
	tests := []struct {
		name   string
		Id     string
		UserId string
		Topic  string
		Ret    []byte
		Want   bool
	}{
		{"1",
			"275073F374BAED0C",
			"user1",
			"light/light_001/set",
			[]byte(defaultSubscriptionCodecFactory + "/user1/light%2Flight_001%2Fset"),
			true},
		{"2",
			"1111",
			"user1",
			"/light/light_001/set",
			[]byte(defaultSubscriptionCodecFactory + "/user1/%2Flight%2Flight_001%2Fset"),
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := new(metapb.Subscription)
			err := cf.KeyCodec().Unmarshal(tt.Ret, s)
			if err != nil ||
				(s.TopicFilter == tt.Topic && s.UserId == tt.UserId) != tt.Want {
				t.Errorf("[%v]err  %v", tt.name, err)
				t.Errorf("ret = %v, want %v", s, tt.Want)
			}
		})
	}
}

func TestDefaultRuleCodecFactoryMarshal(t *testing.T) {
	codec := defaultRuleCodecFactory
	cf := getFactory(codec)
	tests := []struct {
		name   string
		Id     string
		UserId string
		Topic  string
		Ret    []byte
		Want   bool
	}{
		{"1",
			"275073F374BAED0C",
			"user1",
			"light/light_001/set",
			[]byte(defaultRuleCodecFactory + "/user1/275073F374BAED0C"),
			true},
		{"2",
			"1111111",
			"user1",
			"/light/light_001/set",
			[]byte(defaultRuleCodecFactory + "/user1/1111111"),
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscription := &metapb.RuleQL{
				Id:          tt.Id,
				UserId:      tt.UserId,
				TopicFilter: tt.Topic,
			}
			ret, err := cf.KeyCodec().Marshal(subscription)
			if err != nil || reflect.DeepEqual(ret, tt.Ret) != tt.Want {
				t.Errorf("[%v]err  %v", tt.name, err)
				t.Errorf("ret = %v == %v, want %v", string(ret), string(tt.Ret), tt.Want)
			}
		})
	}
}

func TestDefaultRuleCodecFactoryUnmarshal(t *testing.T) {
	codec := defaultRuleCodecFactory
	cf := getFactory(codec)
	tests := []struct {
		name   string
		Id     string
		UserId string
		Topic  string
		Ret    []byte
		Want   bool
	}{
		{"1",
			"275073F374BAED0C",
			"user1",
			"light/light_001/set",
			[]byte(defaultRuleCodecFactory + "/user1/275073F374BAED0C"),
			true},
		{"2",
			"1111111",
			"user1",
			"/light/light_001/set",
			[]byte(defaultRuleCodecFactory + "/user1/1111111"),
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := new(metapb.RuleQL)
			err := cf.KeyCodec().Unmarshal(tt.Ret, s)
			if err != nil ||
				(s.Id == tt.Id && s.UserId == tt.UserId && s.TopicFilter == "") != tt.Want {
				t.Errorf("[%v]err  %v", tt.name, err)
				t.Errorf("ret = %v, want %v", s, tt.Want)
			}
		})
	}
}
