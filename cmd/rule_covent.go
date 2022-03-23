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

package cmd

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/internal/conf"
	old "github.com/tkeel-io/rule-metadata/internal/conf/conf20200629"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
	"github.com/spf13/cobra"
	"strings"
)

var (
	oldFile string
	newFile string
)

func init() {
	ruleCoventCmd.Flags().StringVar(&oldFile, "old_conf", "rule-example.json", "Old rule file")
	ruleCoventCmd.Flags().StringVar(&newFile, "conf", "rule.json", "New rule file")
	RootCmd.AddCommand(ruleCoventCmd)
}

var ruleCoventCmd = &cobra.Command{
	Use:   "ruleCovent",
	Short: "convent old rule config.",
	Long:  `convent old rule config.`,
	Run: func(cmd *cobra.Command, args []string) {
		flag.Parse()
		defer log.Flush()
		covent(oldFile, newFile)
	},
}

func covent(oldFile, newConf string) {
	old, err := old.LoadRuleTomlFile(oldFile)
	if err != nil {
		log.Error(fmt.Sprintf("Load Rule Fail."), logf.Error(err))
		panic(err)
	}

	cfg := conf.ResourceConfig{
		Version:   "20200220",
		Services:  make(map[string]*conf.Service, 0),
		Resources: make(map[string]*conf.Resource, 0),
	}

	entities := old.Entities

	for name, sink := range old.Sinks {
		cfg.Services[name] = &conf.Service{
			Sink: sink,
		}
	}

	for _, group := range old.Group {
		userId := group.UserId
		_, ok := cfg.Resources[userId]
		if !ok {
			cfg.Resources[userId] = &conf.Resource{
				Rules:  make(map[string]*conf.Rule, 0),
				Routes: make(map[string]*conf.Route, 0),
			}
		}

		for ruleName, rule := range group.Rules {
			expr, _ := ruleql.Parse(rule.SQL)
			topic, _ := ruleql.GetTopic(expr)
			for idx, entityKey := range rule.Entities {
				entity, ok := entities[entityKey]
				//fmt.Println(entityKey, entity)
				if !ok {
					log.Error("entityKey not found.")
				}
				ruleName = strings.ReplaceAll(ruleName, "rule", "route")
				cfg.Resources[userId].Routes[fmt.Sprintf("%s-%d", ruleName, idx)] = &conf.Route{
					Entity:      entityKey,
					TopicFilter: topic,
					Service:     entity.Sink,
				}
			}
		}
	}

	bs, err := json.MarshalIndent(cfg, "", "  ")

	//fmt.Println("#Write to %s", newConf)
	//ioutil.WriteFile(newConf, bs, os.ModePerm)
	fmt.Println(string(bs))
}
