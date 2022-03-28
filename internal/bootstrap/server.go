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

package bootstrap

import (
	"context"
	"net"
	"os"
	"strings"

	"github.com/tkeel-io/rule-metadata/internal/conf"
	"github.com/tkeel-io/rule-metadata/internal/endpoint/resource"
	"github.com/tkeel-io/rule-metadata/internal/endpoint/rule"
	xmetrics "github.com/tkeel-io/rule-metadata/internal/pkg/metrics"
	"github.com/tkeel-io/rule-metadata/internal/repository"
	"github.com/tkeel-io/rule-metadata/internal/repository/dao"
	xgrpc "github.com/tkeel-io/rule-metadata/internal/transport/grpc"
	"github.com/tkeel-io/rule-metadata/internal/utils"
	"github.com/tkeel-io/rule-metadata/pkg/types"
	pb "github.com/tkeel-io/rule-util/metadata/v1"
	"github.com/tkeel-io/rule-util/metadata/v1error"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/pkg/registry"
	etcdv3 "github.com/tkeel-io/rule-util/pkg/registry/etcd3"
	"github.com/tkeel-io/rule-util/pkg/tracing"
	"github.com/tkeel-io/rule-util/stream"
	"google.golang.org/grpc"
)

type Server struct {
	shutdown chan error
	server   *grpc.Server
	listener net.Listener
	event    stream.Source
	handle   eventFunc
	conf     *conf.Config
	//	tracer   tracing.Tracer
	//	logger   log.Factory
	repo   *repository.Repository
	ctx    context.Context
	cancel context.CancelFunc
}

type listenFunc func(network string, address string) (net.Listener, error)

type eventFunc interface {
	Name(ctx context.Context) string
	Handle(ctx context.Context, message interface{}) error
}

type patchTable struct {
	listen listenFunc
	remove func(name string) error
}

func newPatchTable() *patchTable {
	return &patchTable{
		listen: net.Listen,
		remove: os.Remove,
	}
}

func NewServer(ctx context.Context, cfg *conf.Config, tracer tracing.Tracer, logger log.Factory) *Server {
	utils.SetLogger(logger)
	srv, err := newServer(ctx, cfg, newPatchTable(), tracer, logger)
	if err != nil {
		log.Fatal("creat server error",
			logf.Error(err))
	}
	return srv
}

func newServer(ctx context.Context, c *conf.Config, pt *patchTable, tracer tracing.Tracer, logger log.Factory) (*Server, error) {
	var err error
	ctx, cancel := context.WithCancel(ctx)
	srv := &Server{
		ctx:    ctx,
		cancel: cancel,
		conf:   c,
		server: xgrpc.New(c.RPCServer),
		//		event:  xevent.New(ctx, c.Event),
	}

	network, address := extractNetAddress(c.RPCServer.Addr)

	if network != "tcp" {
		return nil, types.Errorf("unsupported protocol %s: %v", address, err)
	}

	if err = register(c, srv); err != nil {
		log.Fatal("Register server error!")
	}

	if srv.listener, err = pt.listen(network, address); err != nil {
		return nil, types.Errorf("unable to listen: %v", err)
	}

	if c.Discovery != nil {
		dc, err := etcdv3.New(c.Discovery)
		if err != nil {
			log.Fatal("Register error!")
		}

		if err = dc.Register(ctx, &registry.Node{
			AppID: c.RPCServer.APPID,
			Addr:  srv.listener.Addr().String(),
		}, 1); err != nil {
			log.Fatal("Register error!")
		}

	}
	xmetrics.Init(c)
	xmetrics.GetIns().SetRule(srv.repo.CountRule(ctx))
	xmetrics.GetIns().SetRoute(srv.repo.CountRoute(ctx))
	xmetrics.GetIns().SetSubpub(srv.repo.CountPubsub(ctx))
	return srv, nil
}

func register(c *conf.Config, srv *Server) (err error) {
	// register endpoint
	//addr := ip.InternalIP()

	var db *dao.Dao
	var repo *repository.Repository

	ruleMate, err := conf.NewClient(c.Base.BaseConfig)
	if err != nil {
		log.Error("creat ruleMate repository error",
			logf.Error(err))
	}

	if db, err = dao.New(c); err != nil {
		log.Fatal("creat entity repository error",
			logf.Error(err))
	} else if repo, err = repository.NewRepository(db, c.Slot, ruleMate); err != nil {
		log.Fatal("creat entity repository error",
			logf.Error(err))
	}

	resourceHandle := resource.NewEndpoint(srv.ctx, repo)
	pb.RegisterResourcesManagerServer(srv.server, resourceHandle)

	ruleHandle := rule.NewEndpoint(srv.ctx, repo)
	pb.RegisterRuleActionServer(srv.server, ruleHandle)

	srv.repo = repo
	return err
}

// Run start to receiving gRPC requests.
func (srv *Server) Run() error {
	srv.shutdown = make(chan error, 1)
	go func() {
		log.Info("starting gRPC server",
			logf.String("Addr", srv.listener.Addr().String()))
		err := srv.server.Serve(srv.listener)
		if err != nil {
			log.Error("start gRPC server",
				logf.StatusCode(v1error.ServiceStartFailed),
				logf.Error(err))
			srv.errHandler(err)
		}
	}()
	if srv.event != nil {
		log.Debug("starting cloud event... ")
		go func(handle eventFunc) {
			err := srv.event.StartReceiver(srv.ctx, handle.Handle)
			if err != nil {
				log.Error("OpenStream",
					logf.StatusCode(v1error.StreamOpenFail),
					logf.Topic(srv.event.String()),
					logf.String("StreamName", handle.Name(srv.ctx)),
					logf.Error(err))
				srv.errHandler(err)
			}
		}(srv.handle)
		log.Info("OpenStream",
			logf.StatusCode(v1error.StreamOpen),
			logf.Topic(srv.event.String()),
			logf.String("Name", srv.handle.Name(srv.ctx)))
		log.Info("starting cloud event done. ")
	}

	//	go xmetrics.GetIns().Run()
	return nil
}

// Wait waits for the server to exit.
func (srv *Server) Wait() error {
	if srv.shutdown == nil {
		return types.Errorf("server(metadata) not running")
	}

	err := <-srv.shutdown
	srv.shutdown = nil

	return err
}

func (srv *Server) Close() error {
	srv.server.GracefulStop()
	if srv.shutdown != nil {
		srv.cancel()
		err := srv.event.Close(srv.ctx)
		if err != nil {
			log.Error("CloseStream",
				logf.StatusCode(v1error.StreamCloseFail),
				logf.Error(err))
		} else {
			log.Info("CloseStream",
				logf.StatusCode(v1error.StreamClose))
		}
		srv.shutdown <- nil
		_ = srv.Wait()
	}

	return nil
}

func extractNetAddress(apiAddress string) (string, string) {
	idx := strings.Index(apiAddress, "://")
	if idx < 0 {
		return "tcp", apiAddress
	}

	return apiAddress[:idx], apiAddress[idx+3:]
}

func (srv *Server) errHandler(err error) {
	select {
	case <-srv.shutdown:
		return
	default:
	}
	select {
	case <-srv.ctx.Done():
	case srv.shutdown <- err:
	}
}
