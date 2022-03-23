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
)

func (s *Endpoint) pubSubRange(ctx context.Context, in *metapb.ResourcesRequest, stream metapb.ResourcesManager_ListAndWatchServer) error {
	rc, errc := s.repo.PubSubRestore(ctx, in.ResourceVersion)
	for r := range rc {
		body := make([]*metapb.ResourceObject, 0, len(r))
		for _, e := range r {
			body = append(body, &metapb.ResourceObject{
				Type:            metapb.ResourceObject_ADDED,
				ResourceVersion: e.ResourceVersion,
				Body: &metapb.ResourceObject_Subscription{
					Subscription: e.Subscription,
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
		s.collectRSubs(len(r), len(body))
	}

	err := <-errc
	if err != nil {
		return err
	}

	return nil
}

func (s *Endpoint) pubSubWatch(ctx context.Context, in *metapb.ResourcesRequest, stream metapb.ResourcesManager_ListAndWatchServer) error {
	rc, errc := s.repo.PubSubWatch(ctx, in.ResourceVersion)
	for r := range rc {
		body := make([]*metapb.ResourceObject, 0, len(r))
		for _, e := range r {
			body = append(body, &metapb.ResourceObject{
				Type:            s.EventType(e.Type),
				ResourceVersion: e.Kv.ResourceVersion,
				Body: &metapb.ResourceObject_Subscription{
					Subscription: e.Kv.Subscription,
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
		s.collectRSubs(len(r), len(body))
	}
	err := <-errc
	if err != nil {
		return err
	}
	return nil
}
