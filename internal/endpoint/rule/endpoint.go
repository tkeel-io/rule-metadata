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

package rule

import (
	"context"
	"fmt"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/metadata/v1error"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/internal/endpoint"
	xmetrics "github.com/tkeel-io/rule-metadata/internal/pkg/metrics"
	"github.com/tkeel-io/rule-metadata/internal/utils"
	"github.com/tkeel-io/rule-metadata/pkg/types"
	"github.com/tkeel-io/rule-util/ruleql/pkg/ruleql"
)

var _ metapb.RuleActionServer = &Endpoint{}

//Endpoint subject endpoint
type Endpoint struct {
	endpoint.Endpoint
	repo    RuleRepository
	logger  log.Factory
	context context.Context
}

type RuleRepository interface {
	NewRule(ctx context.Context, rule *metapb.RuleQL) (err error)
	DelRule(ctx context.Context, rule *metapb.RuleQL) (err error)
	GetRuleRevision(ctx context.Context) (int64, error)
}

//NewEndpoint creat endpoint from config
func NewEndpoint(ctx context.Context, repo RuleRepository) *Endpoint {
	return &Endpoint{
		context: ctx,
		repo:    repo,
		logger:  utils.Logger,
	}
}

func (s *Endpoint) AddRule(ctx context.Context, in *metapb.RuleRequest) (*metapb.RuleResponse, error) {
	s.logger.For(ctx).Info("AddRule",
		logf.Any("rule", in.Rule))

	rule, err := s.CheckRule(ctx, in.Rule)
	if err != nil {
		s.logger.For(ctx).Error("add route",
			logf.UserID(in.Rule.UserId),
			logf.StatusCode(v1error.RuleCreateFail),
			logf.Error(err))
		return &metapb.RuleResponse{
			Header:   &metapb.ResponseHeader{},
			PacketId: in.PacketId,
		}, err
	} else {
		s.logger.For(ctx).Info("add rule",
			logf.UserID(in.Rule.UserId),
			logf.StatusCode(v1error.RuleCreate))
	}

	err = s.repo.NewRule(ctx, rule)
	if err != nil {
		s.logger.For(ctx).Error("add rule",
			logf.UserID(in.Rule.UserId),
			logf.Any("rule", in.Rule),
			logf.StatusCode(v1error.RuleCreateFail),
			logf.Error(err))
		return &metapb.RuleResponse{
			Header:   &metapb.ResponseHeader{},
			PacketId: in.PacketId,
		}, err
	} else {
		xmetrics.RuleInc()
		s.logger.For(ctx).Info("add rule",
			logf.UserID(in.Rule.UserId),
			logf.Any("rule", in.Rule),
			logf.StatusCode(v1error.RuleCreate))
	}
	return &metapb.RuleResponse{
		Header:   &metapb.ResponseHeader{},
		PacketId: in.PacketId,
	}, nil
}

func (s *Endpoint) DelRule(ctx context.Context, in *metapb.RuleRequest) (*metapb.RuleResponse, error) {
	s.logger.For(ctx).Info("Unsubscribe",
		logf.Any("rule", in.Rule))
	err := s.repo.DelRule(ctx, in.Rule)
	if err != nil {
		s.logger.For(ctx).Error("del rule",
			logf.UserID(in.Rule.UserId),
			logf.StatusCode(v1error.RuleDeleteFail),
			logf.Error(err))
		return &metapb.RuleResponse{
			Header:   &metapb.ResponseHeader{},
			PacketId: in.PacketId,
		}, err
	} else {
		xmetrics.RuleDec()
		s.logger.For(ctx).Info("del rule",
			logf.UserID(in.Rule.UserId),
			logf.StatusCode(v1error.RuleDelete))
	}
	return &metapb.RuleResponse{
		Header:   &metapb.ResponseHeader{},
		PacketId: in.PacketId,
	}, nil
}

func (s *Endpoint) CheckRule(ctx context.Context, rule *metapb.RuleQL) (*metapb.RuleQL, error) {
	if rule == nil {
		return nil, types.ErrRuleNil
	}
	if rule.UserId == "" {
		return nil, types.ErrRuleUserIdEmpty
	}
	if rule.UserId == "*" {
		return nil, types.ErrRuleUserIdForbidden
	}

	expr, err := ruleql.Parse(string(rule.Body))
	if err != nil {
		return nil, types.ErrRuleParseError
	}
	if topic, ok := ruleql.GetTopic(expr); ok {
		rule.TopicFilter = topic
	} else {
		return nil, types.ErrRuleTopicNilError
	}
	if topic, ok := ruleql.GetTopic(expr); ok {
		rule.TopicFilter = topic
	} else {
		return nil, types.ErrRuleTopicNilError
	}

	return rule, nil
}

func (s *Endpoint) AddRouteTable(ctx context.Context, in *metapb.RouteTableRequest) (*metapb.RouteTableResponse, error) {
	s.logger.For(ctx).Info("Subscribe",
		logf.Any("rule", in.Rule))

	dstTopic := in.DstTopic
	if in.Rule.UserId == "" {
		return nil, types.ErrRuleUserIdForbidden
	}
	if in.Rule.UserId == "*" {
		return nil, types.ErrRuleUserIdForbidden
	}
	rule, err := s.CheckRouteTable(ctx, in.Rule, dstTopic)
	if err != nil {
		s.logger.For(ctx).Error("add route",
			logf.UserID(in.Rule.UserId),
			logf.StatusCode(v1error.RuleCreateFail),
			logf.Error(err))
	} else {
		s.logger.For(ctx).Info("add rule",
			logf.UserID(in.Rule.UserId),
			logf.StatusCode(v1error.RuleCreate))
	}

	err = s.repo.NewRule(ctx, rule)
	if err != nil {
		s.logger.For(ctx).Error("AddRouteTable",
			logf.Error(err))
		return nil, err
	} else {
		xmetrics.RuleInc()
	}

	return &metapb.RouteTableResponse{
		Header:   &metapb.ResponseHeader{},
		PacketId: in.PacketId,
		RuleQl:   rule,
	}, err
}

func (s *Endpoint) DelRouteTable(ctx context.Context, in *metapb.RouteTableRequest) (*metapb.RouteTableResponse, error) {
	s.logger.For(ctx).Info("Unsubscribe",
		logf.Any("rule", in.Rule))
	err := s.repo.DelRule(ctx, in.Rule)
	if err != nil {
		s.logger.For(ctx).Error("del rule",
			logf.UserID(in.Rule.UserId),
			logf.StatusCode(v1error.RuleDeleteFail),
			logf.Error(err))
	} else {
		xmetrics.RuleDec()
		s.logger.For(ctx).Info("del rule",
			logf.UserID(in.Rule.UserId),
			logf.StatusCode(v1error.RuleDelete))
	}
	return &metapb.RouteTableResponse{
		Header:   &metapb.ResponseHeader{},
		PacketId: in.PacketId,
		RuleQl:   in.Rule,
	}, err
}

func (s *Endpoint) CheckRouteTable(ctx context.Context, rule *metapb.RuleQL, dstTopic string) (*metapb.RuleQL, error) {
	ac := &metapb.Action{
		Id:   "republish",
		Type: "republish",
		Metadata: map[string]string{
			"option": fmt.Sprintf(`{"topic":"%s"}`, dstTopic),
		},
	}
	rule.Actions = []*metapb.Action{ac}
	expr, err := ruleql.Parse(string(rule.Body))
	if err != nil {
		return nil, types.ErrRuleParseError
	}
	if topic, ok := ruleql.GetTopic(expr); ok {
		rule.TopicFilter = topic
	} else {
		return nil, types.ErrRuleTopicNilError
	}
	return rule, nil
}

//func (s *Endpoint) AddRoute(ctx context.Context,in *metapb.RouteRequest) (*metapb.RouteResponse, error) {
//	s.logger.For(ctx).Info("Subscribe",
//		logf.Any("rule", in.Rule))
//
//	dstEntity := in.DstEntity
//	if in.Rule.UserId == "" {
//		return nil, errors.ErrRuleUserIdForbidden
//	}
//	if in.Rule.UserId == "*" {
//		return nil, errors.ErrRuleUserIdForbidden
//	}
//	rule, err := s.CheckRoute(ctx, in.Rule, dstEntity)
//	if err != nil {
//		s.logger.For(ctx).Error("add route",
//			logf.UserID(in.Rule.UserId),
//			logf.StatusCode(v1error.RuleCreateFail),
//			logf.Error(err))
//	} else {
//		s.logger.For(ctx).Info("add rule",
//			logf.UserID(in.Rule.UserId),
//			logf.StatusCode(v1error.RuleCreate))
//	}
//
//	err = s.repo.NewRule(ctx, rule)
//	if err != nil {
//		s.logger.For(ctx).Error("AddRouteTable",
//			logf.Error(err))
//		return nil, err
//	}
//
//	return &metapb.RouteResponse{
//		Header:   &metapb.ResponseHeader{},
//		PacketId: in.PacketId,
//		RuleQl:   rule,
//	}, err
//}
//
//func (s *Endpoint) DelRoute(ctx context.Context, in *metapb.RouteRequest) (*metapb.RouteResponse, error) {
//	s.logger.For(ctx).Info("Unsubscribe",
//		logf.Any("rule", in.Rule))
//	err := s.repo.DelRule(ctx, in.Rule)
//	if err != nil {
//		s.logger.For(ctx).Error("del rule",
//			logf.UserID(in.Rule.UserId),
//			logf.StatusCode(v1error.RuleDeleteFail),
//			logf.Error(err))
//	} else {
//		s.logger.For(ctx).Info("del rule",
//			logf.UserID(in.Rule.UserId),
//			logf.StatusCode(v1error.RuleDelete))
//	}
//	return &metapb.RouteResponse{
//		Header:   &metapb.ResponseHeader{},
//		PacketId: in.PacketId,
//		RuleQl:   in.Rule,
//	}, err
//}
//func (s *Endpoint) CheckRoute(ctx context.Context, rule *metapb.RuleQL, dstEntity string) (*metapb.RuleQL, error) {
//	ac := &metapb.Action{
//		Id:   "republish",
//		Type: "republish",
//	}
//	rule.Actions = []*metapb.Action{ac}
//	expr := ruleql.Parse(string(rule.Body))
//	if expr == nil {
//		return nil, errors.ErrRuleParseError
//	}
//	if topic, ok := ruleql.GetTopic(expr); ok {
//		rule.TopicFilter = topic
//	} else {
//		return nil, errors.ErrRuleTopicNilError
//	}
//	return rule, nil
//}
