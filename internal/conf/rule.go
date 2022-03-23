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
	"context"
	"encoding/json"
	"fmt"
	"github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/internal/utils"
	"github.com/tkeel-io/rule-metadata/pkg/types"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"io/ioutil"
)

var (
	ruleConfig   *ResourceConfig
	Initialized           = false
	_            Matedata = (*client)(nil)

	WantVersion = "20200220"
)

var ErrConfigNotFound = types.New("load rule config failed")

func init() {
	//flag.StringVar(&ruleConfPath, "rule", "rule-example.json", "default rule config path")
}

type Matedata interface {
	Entity(ctx context.Context, entityID string) (*v1.Entity, error)
	RangeRule(ctx context.Context, respchan chan []*v1.RuleValue, errchan chan error)
	RangeRoute(ctx context.Context, respchan chan []*v1.RouteValue, errchan chan error)
	ResourceConfig() (*ResourceConfig, error)
}

type client struct {
	ResourceConfPath string
	ResourceConf     *ResourceConfig
}

//func NewClient(ruleConfig string) (*client, error) {
func NewClient(rulePath string) (*client, error) {
	ruleConfig := ""
	if rulePath != "" {
		log.Info(fmt.Sprintf("use rule config(%s)", rulePath))
		ruleConfig = rulePath
	}
	if ruleConfig == "" {
		log.Error("rule local config filename empty")
		return nil, types.New("rule local config filename empty")
	}
	if cfg, err := LoadRuleTomlFile(ruleConfig); err != nil {
		return nil, err
	} else {
		cli := &client{
			ResourceConfPath: ruleConfig,
			ResourceConf:     cfg,
		}
		cli.check()
		return cli, nil
	}
}

// Init rule config.
func LoadRuleTomlFile(ruleConfPath string) (*ResourceConfig, error) {
	conf := DefaultRule()
	bs, err := ioutil.ReadFile(ruleConfPath)
	if err != nil {
		utils.Logger.Bg().Error("load rule config failed, ",
			logf.String("dir", ruleConfPath),
			logf.Error(err))
		return nil, err
	}
	err = json.Unmarshal(bs, conf)
	if err != nil {
		utils.Logger.Bg().Error("unmarshal failed, ",
			logf.String("dir", ruleConfPath),
			logf.Error(err))
		utils.Logger.Bg().Fatal("rule config error")
		return nil, err
	}
	if conf.Version != WantVersion {
		err = types.New("rule config version error")
		utils.Logger.Bg().Error("rule config version error",
			logf.String("wantVersion", WantVersion),
			logf.String("gotVersion", conf.Version),
			logf.String("configPath", ruleConfPath),
			logf.Error(err))
		utils.Logger.Bg().Fatal("rule config error")
		return nil, err
	}
	return conf, nil
}

// Default new a config with specified defualt value.
func DefaultRule() *ResourceConfig {
	return &ResourceConfig{
		Services:  map[string]*Service{},
		Resources: map[string]*Resource{},
	}
}

type ResourceConfig struct {
	Version   string
	Services  map[string]*Service
	Resources map[string]*Resource
}

type Service struct {
	Sink string
}

type Resource struct {
	Rules  map[string]*Rule
	Routes map[string]*Route
}

type Rule struct {
	UserId  string
	SQL     string
	Actions []Action
}

type Action struct {
	Type     string
	Metadata map[string]interface{}
	ErrFlag  bool
}

// alarm  /sys/+/+/thing/property/+/post
type Route struct {
	Entity      string //实体
	UserId      string
	TopicFilter string
	Service     string //Service[Device<pipe>、IoTData、Metrics、Alarm]
}

type Entity struct {
	Type     string
	Sink     string
	Metadata map[string]interface{}
	Body     []byte
}

func (c *client) RangeRule(ctx context.Context, respchan chan []*v1.RuleValue, errchan chan error) {
	if c == nil {
		return
	}
	for userID, resource := range c.ResourceConf.Resources {
		rvs := make([]*v1.RuleValue, 0, len(resource.Rules))
		for id, rule := range resource.Rules {
			rule := c.makeRule(userID, id, rule)
			rvs = append(rvs, &v1.RuleValue{
				Rule: rule,
			})
		}
		respchan <- rvs
	}
}

func (c *client) RangeRoute(ctx context.Context, respchan chan []*v1.RouteValue, errchan chan error) {
	if c == nil {
		return
	}
	for user, resource := range c.ResourceConf.Resources {
		rvs := make([]*v1.RouteValue, 0, len(resource.Routes))
		for id, rule := range resource.Routes {
			route := c.makeRoute(user, id, rule)
			rvs = append(rvs, &v1.RouteValue{
				Route: route,
			})
		}
		log.For(ctx).Debug("RangeRoute", logf.Any("rvs", rvs))
		respchan <- rvs
	}
}

func (c *client) ResourceConfig() (*ResourceConfig, error) {
	if c == nil || c.ResourceConf == nil || c.ResourceConf.Services == nil {
		err := types.New("Resource is nil.")
		return nil, err
	}
	return c.ResourceConf, nil
}

func (c *client) Entity(ctx context.Context, entityID string) (*v1.Entity, error) {
	if c == nil {
		return nil, types.Errorf("not found")
	}
	log.For(ctx).Debug("Entity", logf.EntityID(entityID))
	return c.GetEntity(entityID)
}

func (c *client) GetEntity(entityID string) (entities *v1.Entity, err error) {
	//ac, ok := c.ResourceConf.Entities[entityID]
	//if ok {
	//	return &v1.Entity{
	//		Id: entityID,
	//		EntityInfo: &v1.EntityInfo{
	//			EntityType: ac.Type,
	//			Metadata:   c.GetMetadata(ac.Metadata),
	//		},
	//	}, nil
	//}
	//return nil, errors.Errorf("not found")
	panic("GetEntity")
}

func (c *client) makeRule(userID string, ruleID string, rule *Rule) *v1.RuleQL {
	acts := make([]*v1.Action, 0)
	for idx, ac := range rule.Actions {
		acts = append(acts, &v1.Action{
			Id:        fmt.Sprintf("%s-%d", ruleID, idx),
			Type:      ac.Type,
			Metadata:  c.GetMetadata(ac.Metadata),
			ErrorFlag: ac.ErrFlag,
		})
	}

	// TODO error
	expr,_ := ruleql.Parse(string(rule.SQL))
	if topic, ok := ruleql.GetTopic(expr); ok {
		return &v1.RuleQL{
			Id:          ruleID,
			UserId:      userID,
			TopicFilter: topic,
			Body:        []byte(rule.SQL),
			Actions:     acts,
		}
	}
	return nil
}

func (c *client) makeRoute(userID string, routeID string, route *Route) *v1.Route {
	service, ok := c.ResourceConf.Services[route.Service]
	if !ok {
		return nil
	}
	return &v1.Route{
		Id:          routeID,
		UserId:      userID,
		TopicFilter: route.TopicFilter,
		Service: &v1.Service{
			Sink: &v1.Flow{URI: service.Sink},
		},
		Entity: route.Entity,
	}
}

func (c *client) GetMetadata(meta map[string]interface{}) (ret map[string]string) {
	ret = make(map[string]string, len(meta))
	for n, v := range meta {
		if byt, err := json.Marshal(v); err != nil {
			utils.Logger.Bg().Error(
				fmt.Sprintf("json marshal %v error", n),
				logf.Any("meta", meta))
		} else {
			ret[n] = string(byt)
		}
	}
	return
}

func (c *client) check() {
	if c.ResourceConf == nil {
		utils.Logger.Bg().Fatal("config nil")
	}
	for _, group := range c.ResourceConf.Resources {
		for rid, route := range group.Routes {
			service, ok := c.ResourceConf.Services[route.Service]
			if !ok {
				utils.Logger.Bg().Error("Service not found",
					logf.Any("ruleID", rid))
				utils.Logger.Bg().Fatal("Service not found")
			}
			sinkRef := service.Sink
			if sinkRef == "" {
				utils.Logger.Bg().Error("stream not found",
					logf.Any("sinkStream", sinkRef))
				utils.Logger.Bg().Fatal("stream not found")
			}
		}
	}
}
