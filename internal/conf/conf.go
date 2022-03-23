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

package conf

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	api "github.com/tkeel-io/rule-util/metadata"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	xutils "github.com/tkeel-io/rule-util/pkg/utils"
	"github.com/BurntSushi/toml"

	"github.com/tkeel-io/rule-util/pkg/registry"

	"github.com/tkeel-io/rule-metadata/internal/utils"
	xtime "github.com/tkeel-io/rule-metadata/pkg/time"
)

func init() {
}

// Init init config.
func LoadTomlFile(confPath string) *Config {
	conf := Default()
	bs, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatal("load config failed, ",
			logf.String("dir", confPath),
			logf.Error(err))
	}
	_, err = toml.Decode(string(bs), &conf)
	if err != nil {
		dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		log.Fatal("load config failed, ",
			logf.String("config", string(bs)),
			logf.String("dir", dir),
			logf.Error(err))
	} else {
		log.Info("load config",
			logf.Any("config", conf),
		)
	}

	err = check(conf)
	return conf
}

func check(conf *Config) error {
	if conf == nil {
		log.Fatal("[check] config is nil",
			logf.Any("config", conf))
	}
	if conf.Event == nil || conf.Event.Source == "" {
		log.Fatal("[check] config event source is nil",
			logf.Any("config", conf))
	}
	if conf.Slot == nil || conf.Slot.PubSubSink == "" ||
		conf.Slot.PubSubSource == "" {
		log.Fatal("[check] config slot fail",
			logf.Any("config", conf))
	}
	if conf.Slot == nil || conf.Slot.PubSubSink == "" ||
		conf.Slot.PubSubSource == "" ||
		conf.Slot.RuleSource == "" {
		log.Fatal("[check] config slot fail",
			logf.Any("slot", conf.Slot))
	}
	return nil
}

// Default new a config with specified defualt value.
func Default() *Config {
	return &Config{
		RPCClient: &RPCClient{Dial: xtime.Duration(time.Second), Timeout: xtime.Duration(time.Second)},
		RPCServer: &RPCServer{
			APPID:                api.APPID,
			Timeout:              xtime.Duration(time.Second),
			IdleTimeout:          xtime.Duration(time.Second * 60),
			MaxLifeTime:          xtime.Duration(time.Hour * 2),
			ForceCloseWait:       xtime.Duration(time.Second * 20),
			KeepAliveInterval:    xtime.Duration(time.Second * 60),
			KeepAliveTimeout:     xtime.Duration(time.Second * 20),
			MaxMessageSize:       1024 * 1024,
			MaxConcurrentStreams: 1024,
		},
		Event: &EventConfig{},
		Slot:  &SlotConfig{},
		Base: &BaseConfig{
			LogLevel:       "info",
			WhiteList:      []string{},
			ProfEnable:     false,
			TracerEnable:   false,
			ProfPathPrefix: "debug",
			BaseConfig:     "",
		},
		Etcd: &EtcdConfig{
			Endpoints:   []string{"127.0.0.1:2379"},
			DialTimeout: xtime.Duration(4 * time.Second),
		},
		Redis: &RedisConfig{
			Addr:         "127.0.0.1:6379",
			Auth:         "",
			MaxActive:    200,
			DB:           0,
			DialTimeout:  xtime.Duration(4 * time.Second),
			ReadTimeout:  xtime.Duration(3 * time.Second),
			WriteTimeout: xtime.Duration(3 * time.Second),
			IdleTimeout:  xtime.Duration(30 * time.Second),
			PoolTimeout:  xtime.Duration(4 * time.Second),
		},
		Discovery: &registry.Config{
			Endpoints: []string{"127.0.0.1:2379"},
		},
		Prometheus: &Prometheus{
			Zone:   "staging",
			Node:   "unkown",
			Name:   "metadata" + xutils.GenerateUUID(),
			Host:   "en0",
			Module: "metadata",
		},
	}
}

// Config config.
type Config struct {
	Discovery  *registry.Config
	RPCClient  *RPCClient
	RPCServer  *RPCServer
	Redis      *RedisConfig
	Etcd       *EtcdConfig
	Event      *EventConfig
	Jaeger     *Jaeger
	Slot       *SlotConfig
	Base       *BaseConfig
	Prometheus *Prometheus
}

// RPCClient is RPC client config.
type RPCClient struct {
	Dial    xtime.Duration
	Timeout xtime.Duration
}

// RPCServer is RPC server config.
type RPCServer struct {
	APPID                string
	Addr                 string
	Timeout              xtime.Duration
	IdleTimeout          xtime.Duration
	MaxLifeTime          xtime.Duration
	ForceCloseWait       xtime.Duration
	KeepAliveInterval    xtime.Duration
	KeepAliveTimeout     xtime.Duration
	MaxMessageSize       int32
	MaxConcurrentStreams int32
	EnableOpenTracing    bool
}

// RedisConfig is redis server config.
type RedisConfig struct {
	Addr         string
	Auth         string
	MaxActive    int
	DB           int
	DialTimeout  xtime.Duration
	ReadTimeout  xtime.Duration
	WriteTimeout xtime.Duration
	IdleTimeout  xtime.Duration
	PoolTimeout  xtime.Duration
}

// EtcdConfig is etcd server config.
type EtcdConfig struct {
	Endpoints   []string
	DialTimeout xtime.Duration
}

type SlotConfig struct {
	RuleSource   string
	PubSubSource string
	PubSubSink   string
}

type EventConfig struct {
	Source string
}

// Jaeger config.
type Jaeger struct {
	Addr string
}

type BaseConfig struct {
	ProfPathPrefix string
	LogLevel       string
	ProfEnable     bool
	TracerEnable   bool
	WhiteList      []string
	Endpoints      []string
	BaseConfig     string
}

type Prometheus struct {
	Zone      string
	Node      string
	Name      string
	Host      string
	Module    string
	Endpoints []string
}

func (this *Prometheus) Address() string {

	var (
		err error
		ip  string
	)

	if ip, err = utils.GetIpByName(this.Host); nil != err {
		log.Error("parse addr failed.",
			logf.String("host", this.Host))
	}
	return ip
}
