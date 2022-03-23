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
	goflag "flag"
	"fmt"
	"github.com/tkeel-io/rule-util/metadata"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger-lib/metrics/expvar"
	prom "github.com/uber/jaeger-lib/metrics/prometheus"
	"os"
)

var (
	metricsBackend string
	metricsFactory metrics.Factory
)

func init() {
	RootCmd.PersistentFlags().StringVarP(&metricsBackend, "metrics", "m", "prometheus", "Metrics backend (expvar|prometheus)")
	cobra.OnInitialize(onInitialize)

	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
}

var RootCmd = &cobra.Command{
	Use:   "metadata",
	Short: "This is metadata service.",
	Long:  `This is metadata service.`,
}

//Execute execute the root command
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}


// onInitialize is called before the command is executed.
func onInitialize() {
	switch metricsBackend {
	case "expvar":
		metricsFactory = expvar.NewFactory(10) // 10 buckets for histograms
		logger.Bg().Debug("Using expvar as metrics backend")
	case "prometheus":
		metricsFactory = prom.New().Namespace(metrics.NSOptions{Name: metadata.APPID, Tags: nil})
		logger.Bg().Debug("Using Prometheus as metrics backend")
	default:
		logger.Bg().Fatal("unsupported metrics backend " + metricsBackend)
	}
}
