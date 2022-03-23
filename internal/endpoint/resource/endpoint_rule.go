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

package resource

import (
	"context"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
	xmetrics "github.com/tkeel-io/rule-metadata/internal/pkg/metrics"
)

func (s *Endpoint) ruleRange(ctx context.Context, in *metapb.ResourcesRequest, stream metapb.ResourcesManager_ListAndWatchServer) error {
	rc, errc := s.repo.RuleRange(ctx, in.ResourceVersion)
	for r := range rc {
		body := make([]*metapb.ResourceObject, 0, len(r))
		for _, e := range r {
			body = append(body, &metapb.ResourceObject{
				Type:            metapb.ResourceObject_ADDED,
				ResourceVersion: e.ResourceVersion,
				Body: &metapb.ResourceObject_Rule{
					Rule: e.Rule,
				},
			})
		}
		if len(body) > 0 {
			err := stream.Send(&metapb.ResourcesResponse{
				Header: &metapb.ResponseHeader{UserId: "-"},
				Body:   body,
			})
			if err != nil {
				return err
			}
		}
		s.collectRule(len(r), len(body))
	}

	err := <-errc
	if err != nil {
		return err
	}

	return nil
}

func (s *Endpoint) ruleWatch(ctx context.Context, in *metapb.ResourcesRequest, stream metapb.ResourcesManager_ListAndWatchServer) error {
	rc, errc := s.repo.RuleWatch(ctx, in.ResourceVersion)
	for r := range rc {
		body := make([]*metapb.ResourceObject, 0, len(r))
		for _, e := range r {
			body = append(body, &metapb.ResourceObject{
				Type:            s.EventType(e.Type),
				ResourceVersion: e.Kv.ResourceVersion,
				Body: &metapb.ResourceObject_Rule{
					Rule: e.Kv.Rule,
				},
			})
		}
		err := stream.Send(&metapb.ResourcesResponse{
			Header: &metapb.ResponseHeader{},
			Body:   body,
		})
		if err != nil {
			return err
		}
		s.collectRule(len(r), len(body))
	}
	err := <-errc
	if err != nil {
		return err
	}
	return nil
}

func (s *Endpoint) collectRule(num, size int) {
	xmetrics.ResourceSync(xmetrics.ResourceTypeRule, num, size)
}

func (s *Endpoint) collectRoute(num, size int) {
	xmetrics.ResourceSync(xmetrics.ResourceTypeRoute, num, size)
}

func (s *Endpoint) collectRSubs(num, size int) {
	xmetrics.ResourceSync(xmetrics.ResourceTypeSubscription, num, size)
}
