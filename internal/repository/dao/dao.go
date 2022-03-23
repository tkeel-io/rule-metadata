package dao

import (
	"github.com/tkeel-io/rule-metadata/internal/utils"
	"github.com/tkeel-io/rule-metadata/pkg/types"
	goredis "github.com/go-redis/redis"
	goetcd "go.etcd.io/etcd/client/v3"

	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/internal/conf"
)

// Dao struct info of Dao.
type Dao struct {
	redis             *goredis.Client
	etcd              *goetcd.Client
	entityCodec       CodecFactory
	subscriptionCodec CodecFactory
	ruleCodec         CodecFactory
	routeCodec        CodecFactory
	logger            log.Factory
}

// New new a Dao and return.
func New(c *conf.Config) (*Dao, error) {
	var etcd *goetcd.Client
	var redis *goredis.Client
	var err error
	if etcd, err = NewEtcdDB(c.Etcd); err != nil {
		return nil, types.Errorf("etcd available:%v", err)
	}
	if redis, err = NewRedisDB(c.Redis); err != nil {
		return nil, types.Errorf("redis available:%v", err)
	}
	if _, err := redis.Ping().Result(); err != nil {
		return nil, types.Errorf("redis available: %v", err)
	}
	return &Dao{
		entityCodec:       getFactory(defaultEntityCodecFactory),
		subscriptionCodec: getFactory(defaultSubscriptionCodecFactory),
		ruleCodec:         getFactory(defaultRuleCodecFactory),
		routeCodec:        getFactory(defaultRouteCodecFactory),
		etcd:              etcd,
		redis:             redis,
		logger:            utils.Logger,
	}, nil
}

// Close close connections of etcd, redis.
func (d *Dao) Close() {
	if d.etcd != nil {
		if err := d.etcd.Close(); err != nil {
			d.logger.Bg().Error("etcd close error",
				logf.Error(err))
		}
	}
	if d.redis != nil {
		if err := d.redis.Close(); err != nil {
			d.logger.Bg().Error("redis close error",
				logf.Error(err))
		}
	}
}
