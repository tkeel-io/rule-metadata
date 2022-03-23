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
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tkeel-io/rule-util/metadata/v1error"
	"github.com/tkeel-io/rule-util/pkg/debug"
	"github.com/tkeel-io/rule-util/pkg/gops"
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-util/pkg/pprof"
	"github.com/tkeel-io/rule-util/pkg/tracing"
	"github.com/tkeel-io/rule-metadata/internal/bootstrap"
	"github.com/tkeel-io/rule-metadata/internal/conf"
	"github.com/tkeel-io/rule-metadata/pkg/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

var (
	confPath     string
	addr         string
	ruleConfPath string
)

func init() {
	serveCmd.PersistentFlags().StringVar(&confPath, "conf", "metadata-example.toml", "default config path")
	serveCmd.PersistentFlags().StringVar(&addr, "addr", "", "The metadata server addr")
	serveCmd.PersistentFlags().StringVar(&ruleConfPath, "rule", "", "default rule config path")
	RootCmd.AddCommand(serveCmd)
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "run metadata service.",
	Long:  `run metadata service.`,
	Run: func(cmd *cobra.Command, args []string) {
		version := fmt.Sprintln(
			fmt.Sprintf("Build Date:%s", version.BuildDate),
			fmt.Sprintf("Git Commit:%s", version.GitCommit),
			fmt.Sprintf("Version:%s", version.Version),
			fmt.Sprintf("Go Version:%s", version.GoVersion),
			fmt.Sprintf("OS / Arch:%s", version.OsArch),
		)
		log.Info("Start",
			logf.String("version", version),
			logf.StatusCode(v1error.ServiceIniting))
		flag.Parse()
		defer log.Flush()

		ctx, cancel := context.WithCancel(context.Background())

		go gops.Run(ctx)

		cfg := conf.LoadTomlFile(confPath)

		if ruleConfPath != "" {
			log.Info(fmt.Sprintf("cfg.Base.BaseConfig rule config(%s)", ruleConfPath))
			cfg.Base.BaseConfig = ruleConfPath
		}

		if cfg.Base.TracerEnable {
			tracer := tracing.Init("metadata", metricsFactory, log.GlobalLogger())
			tracing.SetGlobalTracer(tracer)

		}
		if cfg.Base.LogLevel != "" {
			level.SetLevel(log.GetLevel(cfg.Base.LogLevel))
		}
		if len(cfg.Base.Endpoints) > 0 {
			handles := []*debug.HandleFunc{}
			if cfg.Base.ProfEnable {
				handles = append(handles, pprof.HandleFunc()...)
			}
			handles = append(handles, []*debug.HandleFunc{
				{Pattern: "/log/level", Handler: level.ServeHTTP},
			}...)
			handles = append(handles, []*debug.HandleFunc{
				{Pattern: "/metrics", Handler: promhttp.Handler().ServeHTTP},
			}...)
			debug.Init(
				cfg.Base.Endpoints,
				handles...,
			)
		}

		srv := bootstrap.NewServer(
			ctx,
			cfg,
			tracing.GlobalTracer(),
			logger,
		)

		srv.Run()
		log.Info("Start",
			logf.StatusCode(v1error.ServiceStart))

		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

		select {
		case s := <-signalChan:
			err := srv.Close()
			if err != nil {
				log.Error("server close failure, ", logf.Error(err))
			}
			log.Info("Captured signal",
				logf.Any("signal", s))
			log.Info("Stop",
				logf.StatusCode(v1error.ServiceStop))
		}
		cancel()
	},
}
