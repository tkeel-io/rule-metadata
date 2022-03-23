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
	"github.com/tkeel-io/rule-util/metadata/v1error"
	"github.com/tkeel-io/rule-util/pkg/log"
	"github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/internal/endpoint"
	"github.com/tkeel-io/rule-metadata/internal/repository"
	"github.com/tkeel-io/rule-metadata/internal/utils"
	"sync"

	metapb "github.com/tkeel-io/rule-util/metadata/v1"
)

var _ metapb.ResourcesManagerServer = (*Endpoint)(nil)

type ResourcesFunc func(*metapb.ResourcesResponse) error

//Endpoint subject endpoint
type Endpoint struct {
	endpoint.Endpoint
	repo    *repository.Repository
	logger  log.Factory
	context context.Context
}

func (s *Endpoint) Range(ctx context.Context, request *metapb.ResourceRangeRequest) (*metapb.ResourceRangeResponse, error) {
	panic("implement me")
}

//NewEndpoint creat endpoint from config
func NewEndpoint(ctx context.Context, repo *repository.Repository) *Endpoint {
	return &Endpoint{
		context: ctx,
		repo:    repo,
		logger:  utils.Logger,
	}
}

func (s *Endpoint) ListAndWatch(in *metapb.ResourcesRequest, stream metapb.ResourcesManager_ListAndWatchServer) error {
	var (
		err error
	)

	err = s.List(in, stream)
	if err != nil {
		return err
	}

	err = s.Watch(in, stream)
	if err != nil {
		return err
	}

	return nil
}

func (s *Endpoint) List(in *metapb.ResourcesRequest, stream metapb.ResourcesManager_ListServer) error {
	var (
		revision int64
		err      error
	)

	ctx, cancel := context.WithCancel(s.context)
	defer cancel()

	//we choose the most recent revision.
	revision, err = s.repo.GetLastRevision(ctx)
	if err != nil {
		s.logger.For(ctx).Error("sync restore",
			logf.StatusCode(v1error.SyncStartFailed),
			logf.Any("Revision", revision),
			logf.Error(err))
		return err
	}
	in.ResourceVersion = revision

	s.logger.For(ctx).Info("sync restore",
		logf.StatusCode(v1error.SyncStart),
		logf.Any("Revision", revision))
	defer s.logger.For(ctx).Info("sync restore stop	",
		logf.StatusCode(v1error.SyncStart),
		logf.Any("Revision", revision))

	//@TODO... add sink manager
	pubSubSource, pubSubSink, ruleSource := s.repo.InitRestore(ctx, in)

	// 1. init_config
	err = stream.Send(&metapb.ResourcesResponse{
		Header: &metapb.ResponseHeader{},
		Body: []*metapb.ResourceObject{
			{
				Type:            metapb.ResourceObject_ADDED,
				ResourceVersion: revision,
				Body: &metapb.ResourceObject_InitConfig{
					InitConfig: &metapb.Init{},
				},
			},
		},
	})
	if err != nil {
		s.logger.For(ctx).Error("sync restore initconfig",
			logf.StatusCode(v1error.SyncStartFailed),
			logf.Any("Revision", revision),
			logf.Error(err))
		return err
	}

	// 2. init resource
	err = stream.Send(&metapb.ResourcesResponse{
		Header: &metapb.ResponseHeader{},
		Body: []*metapb.ResourceObject{
			{
				Type:            metapb.ResourceObject_ADDED,
				ResourceVersion: revision,
				Body: &metapb.ResourceObject_Stream{
					Stream: &metapb.Stream{
						Source: []*metapb.Flow{
						},
						Sink: &metapb.Flow{
						},
						PubSubTopic: &metapb.Flow{
							URI: pubSubSource,
						},
						PubSubQueue: &metapb.Flow{
							URI: pubSubSink,
						},
						RuleTopic: &metapb.Flow{
							URI: ruleSource,
						},
					},
				},
			},
		},
	})
	if err != nil {
		s.logger.For(ctx).Error("sync restore resource",
			logf.StatusCode(v1error.SyncStartFailed),
			logf.Any("Revision", revision),
			logf.Error(err))
		return err
	}

	//Rule
	err = s.ruleRange(ctx, in, stream)
	if err != nil {
		s.logger.For(ctx).Error("sync rule range",
			logf.StatusCode(v1error.SyncStartFailed),
			logf.Any("Revision", revision),
			logf.Error(err))
		cancel()
	}

	//Route
	err = s.routeRange(ctx, in, stream)
	if err != nil {
		s.logger.For(ctx).Error("sync route range",
			logf.StatusCode(v1error.SyncStartFailed),
			logf.Any("Revision", revision),
			logf.Error(err))
		cancel()
	}

	//PubSub
	err = s.pubSubRange(ctx, in, stream)
	if err != nil {
		s.logger.For(ctx).Error("sync pubsubrange",
			logf.StatusCode(v1error.SyncStartFailed),
			logf.Any("Revision", revision),
			logf.Error(err))
		cancel()
	}

	return nil
}

func (s *Endpoint) Watch(in *metapb.ResourcesRequest, stream metapb.ResourcesManager_WatchServer) error {
	var (
		revision = in.ResourceVersion
	)
	var wg = sync.WaitGroup{}
	ctx, cancel := context.WithCancel(s.context)
	defer cancel()

	// catch cancel
	go func() {
		<-stream.Context().Done()
		s.logger.For(ctx).Warn("sync event",
			logf.StatusCode(v1error.SyncStartFailed),
			logf.Any("Revision", revision),
			logf.Error(stream.Context().Err()))
		cancel()
	}()

	s.logger.For(ctx).Info("sync event",
		logf.StatusCode(v1error.SyncStart),
		logf.Any("Revision", revision))
	defer s.logger.For(ctx).Info("sync event stop	",
		logf.StatusCode(v1error.SyncStop),
		logf.Any("Revision", revision))

	wg.Add(1)
	go func() {
		err := s.pubSubWatch(ctx, in, stream)
		if err != nil {
			s.logger.For(ctx).Error("pub/sub error",
				logf.Error(err))
			cancel()
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		err := s.ruleWatch(ctx, in, stream)
		if err != nil {
			s.logger.For(ctx).Error("rule error",
				logf.Error(err))
			cancel()
		}
		wg.Done()
	}()

	wg.Wait()

	return nil
}
