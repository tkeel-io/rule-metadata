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
	//"github.com/BurntSushi/toml"
	"io/ioutil"
	//"os"
	//"path/filepath"
)

var (
	ruleConfPath string
	ruleConfig   *RuleConfig
	Initialized  = false
)

var ErrConfigNotFound = types.New("load rule config failed")

type RuleMatedata interface {
	Entity(ctx context.Context, entityID string) (*v1.Entity, error)
	RangeRule(ctx context.Context, respchan chan []*v1.RuleValue, errchan chan error)
}

type client struct {
	RuleConfPath string
	RuleConf     *RuleConfig
}

//func NewClient(ruleConfig string) (*client, error) {
func NewClient(rulePath string) (*client, error) {
	ruleConfig := ""
	if rulePath != "" {
		log.Info(fmt.Sprintf("use rule config(%s)", ruleConfPath))
		ruleConfig = rulePath
	}
	if ruleConfPath != "" {
		log.Info(fmt.Sprintf("use flag rule config(%s)", ruleConfPath))
		ruleConfig = ruleConfPath
	}
	if ruleConfig == "" {
		log.Error("rule local config filename empty")
		return nil, types.New("rule local config filename empty")
	}
	if cfg, err := LoadRuleTomlFile(ruleConfig); err != nil {
		return nil, err
	} else {
		cli := &client{
			RuleConfPath: ruleConfig,
			RuleConf:     cfg,
		}
		cli.check()
		return cli, nil
	}
}

// Init rule config.
func LoadRuleTomlFile(ruleConfPath string) (*RuleConfig, error) {
	conf := DefaultRule()
	//_, err := toml.DecodeFile(ruleConfPath, &conf)
	//if err != nil {
	//	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	//	log.GlobalLogger().Bg().Error("load rule config failed, ",
	//		logf.String("dir", dir),
	//		logf.  Error(err))
	//	return nil
	//}
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
	if conf.Version != "20200210" {
		err = types.New("rule config version error")
		utils.Logger.Bg().Error("rule config version error",
			logf.String("wantVersion", "20200208"),
			logf.String("gotVersion", conf.Version),
			logf.Error(err))
		utils.Logger.Bg().Fatal("rule config error")
		return nil, err
	}
	return conf, nil
}

// Default new a config with specified defualt value.
func DefaultRule() *RuleConfig {
	return &RuleConfig{
		"",
		map[string]string{},
		map[string]*Entity{},
		[]Group{},
	}
}

type RuleConfig struct {
	Version  string
	Sinks    map[string]string
	Entities map[string]*Entity
	Group    []Group
}
type Group struct {
	SinkRef string
	UserId  string
	GroupId string
	Rules   map[string]*Rule
}

type Rule struct {
	SQL      string
	Entities []string
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
	for _, group := range c.RuleConf.Group {
		rvs := make([]*v1.RuleValue, 0, len(group.Rules))
		for id, rule := range group.Rules {
			rule := c.makeRule(&group, id, rule)
			rvs = append(rvs, &v1.RuleValue{
				Rule: rule,
			})
		}
		respchan <- rvs
	}
}

func (c *client) Entity(ctx context.Context, entityID string) (*v1.Entity, error) {
	if c == nil {
		return nil, types.Errorf("not found")
	}
	log.For(ctx).Debug("Entity", logf.EntityID(entityID))
	return c.GetEntity(entityID)
}

func (c *client) GetEntity(entityID string) (entities *v1.Entity, err error) {
	ac, ok := c.RuleConf.Entities[entityID]
	if ok {
		return &v1.Entity{
			Id: entityID,
			EntityInfo: &v1.EntityInfo{
				EntityType: ac.Type,
				Metadata:   c.GetMetadata(ac.Metadata),
			},
		}, nil
	}
	return nil, types.Errorf("not found")
}

func (c *client) makeRule(group *Group, ruleID string, rule *Rule) *v1.RuleQL {
	acts := make([]*v1.Action, 0)
	for _, id := range rule.Entities {
		entity, ok := c.RuleConf.Entities[id]
		if ok {
			sinkRef := entity.Sink
			if sinkRef == "" {
				sinkRef = group.SinkRef
			}
			sink, ok := c.RuleConf.Sinks[sinkRef]
			if !ok && sink != "" {
				utils.Logger.Bg().Error("sink ref is empty",
					logf.Any("group", group),
					logf.Any("rule", rule),
					logf.EntityID(id),
					logf.Any("entity", entity))
				utils.Logger.Bg().Fatal("sink ref is empty")
			}
			acts = append(acts, &v1.Action{
				Id:       id,
				Type:     entity.Type,
				Metadata: c.GetMetadata(entity.Metadata),
				Sink:     sink,
			})
		} else {
			fmt.Println("error action", id)
		}
	}
	// TODO error
	expr, _ := ruleql.Parse(string(rule.SQL))
	if topic, ok := ruleql.GetTopic(expr); ok {
		return &v1.RuleQL{
			Id:          ruleID,
			UserId:      group.UserId,
			TopicFilter: topic,
			Body:        []byte(rule.SQL),
			Actions:     acts,
		}
	}
	return nil
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
	if c.RuleConf == nil {
		utils.Logger.Bg().Fatal("config nil")
	}
	for _, group := range c.RuleConf.Group {
		for rid, rule := range group.Rules {
			for _, eid := range rule.Entities {
				entity, ok := c.RuleConf.Entities[eid]
				if !ok {
					utils.Logger.Bg().Error("entity not found",
						logf.Any("ruleID", rid),
						logf.Any("EntityID", eid))
					utils.Logger.Bg().Fatal("entity not found")
				}
				sinkRef := entity.Sink
				if sinkRef == "" {
					sinkRef = group.SinkRef
				}
				_, ok = c.RuleConf.Sinks[sinkRef]
				if !ok {
					utils.Logger.Bg().Error("stream not found",
						logf.Any("sinkStream", sinkRef))
					utils.Logger.Bg().Fatal("stream not found")
				}
			}

			// TODO error
			expr, _ := ruleql.Parse(string(rule.SQL))
			topic, ok := ruleql.GetTopic(expr)
			if topic == "" {
				utils.Logger.Bg().Error("ruleql topic error",
					logf.Any("SQL", rule.SQL))
				utils.Logger.Bg().Fatal("ruleql topic error")
			}
			if !ok {
				utils.Logger.Bg().Error("ruleql error",
					logf.Any("SQL", rule.SQL))
				utils.Logger.Bg().Fatal("ruleql error")
			}
		}
	}
}
