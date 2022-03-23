package metrics

import (
	"github.com/tkeel-io/rule-util/pkg/log"
	logf "github.com/tkeel-io/rule-util/pkg/logfield"
	"github.com/tkeel-io/rule-metadata/internal/conf"
	xutils "github.com/tkeel-io/rule-metadata/internal/utils"
	"github.com/prometheus/client_golang/prometheus"
	xmetrics "github.com/tkeel-io/rule-util/pkg/metrics/prometheus"
	"github.com/tkeel-io/rule-util/pkg/metrics/prometheus/option"
)

var metaMetric *MetaMetrics
var logger = xutils.Logger

type MetaMetrics struct {
	name      string
	node      string
	zone      string
	module    string
	address   string
	endpoints []string
	reg       *prometheus.Registry

	//rule增量统计
	ruleCounter prometheus.Gauge
	//route增量统计
	routeCounter prometheus.Gauge
	//subscription增量统计
	subscriptionCounter prometheus.Gauge
	//entity上线统计
	entityOnlineCounter prometheus.Counter
	//entity下线统计
	entityOfflineCounter prometheus.Counter
	//资源（rule,route,subscription）同步速度（单位：个）
	resourceSync *prometheus.GaugeVec
	//资源（rule,route,subscription）同步速度（单位：byte）
	resourceSyncSent *prometheus.GaugeVec
	//消息的输入输出统计
	msgInOut *prometheus.GaugeVec
	//接受消息的速率（B/s）
	msgReceivedByte prometheus.Gauge
	//消息发送的速率
	msgSentByte prometheus.Gauge
	//消息延迟处理时间分布
	msgDelaySum *prometheus.SummaryVec
}

func GetIns() *MetaMetrics {
	return metaMetric
}

func (this *MetaMetrics) register() {

	labels := option.WithBindLabels(
		option.NewBaseBindLabels(
			this.name,
			this.module,
			this.zone)...,
	)
	opts := []xmetrics.Option{labels.Append([]option.BindLabel{option.NewBindLabel("node", this.node)})}

	xmetrics.Setup(this.reg, opts...).Register(this.ruleCounter)
	xmetrics.Setup(this.reg, opts...).Register(this.routeCounter)
	xmetrics.Setup(this.reg, opts...).Register(this.subscriptionCounter)
	xmetrics.Setup(this.reg, opts...).Register(this.entityOnlineCounter)
	xmetrics.Setup(this.reg, opts...).Register(this.entityOfflineCounter)
	xmetrics.Setup(this.reg, opts...).Register(this.resourceSync)
	xmetrics.Setup(this.reg, opts...).Register(this.resourceSyncSent)

	xmetrics.Setup(this.reg, opts...).Register(this.msgInOut)
	xmetrics.Setup(this.reg, opts...).Register(this.msgReceivedByte)
	xmetrics.Setup(this.reg, opts...).Register(this.msgSentByte)
	xmetrics.Setup(this.reg, opts...).Register(this.msgDelaySum)
	xmetrics.Setup(this.reg, opts...).Register(prometheus.NewGoCollector())

}

func (this *MetaMetrics) exposed() {
	if err := xmetrics.ExposedMetrics(this.reg, &xmetrics.ExposedConf{
		Addr: this.address,
		Etcd: this.endpoints,
		KV:   NewmetadataFmt(this.name),
	}); nil != err {
		panic(err)
	}
}

func (this *MetaMetrics) SetRoute(num int) {
	this.routeCounter.Set(float64(num))
}

func (this *MetaMetrics) SetRule(num int) {
	this.ruleCounter.Set(float64(num))
}

func (this *MetaMetrics) SetSubpub(num int) {
	this.subscriptionCounter.Set(float64(num))
}

func (this *MetaMetrics) Run() {
	metaMetric.register()

	//expose
	metaMetric.exposed()
}

func Init(c *conf.Config) {
	metaMetric = &MetaMetrics{
		name:      c.Prometheus.Name,
		node:      c.Prometheus.Node,
		zone:      c.Prometheus.Zone,
		module:    c.RPCServer.APPID,
		address:   c.Prometheus.Address(),
		endpoints: c.Prometheus.Endpoints,
		reg:       prometheus.NewRegistry(),

		ruleCounter: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Name:      "rule",
			Help:      "count for rule.",
		}),

		routeCounter: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Name:      "route",
			Help:      "count for route.",
		}),

		subscriptionCounter: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Name:      "subscription",
			Help:      "count for subscription.",
		}),

		entityOnlineCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "mdmp",
			Name:      "online",
			Help:      "entity online.",
		}),

		entityOfflineCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "mdmp",
			//Subsystem: "entity",
			Name: "offline",
			Help: "entity offline.",
		}),

		resourceSync: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Name:      "sync",
			Help:      "count for sync resource.",
		}, resSyncLabels),

		resourceSyncSent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Name:      "sync_sent",
			Help:      "count for sync resource(bytes).",
		}, resSyncLabels),

		msgInOut: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Name:      "inout",
			Help:      "received messages and sent messages.",
		}, inoutLabels),

		msgReceivedByte: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Subsystem: "recv",
			Name:      "rate",
			Help:      "received message rate.",
		}),

		msgSentByte: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "mdmp",
			Subsystem: "sent",
			Name:      "rate",
			Help:      "send message rate.",
		}),

		msgDelaySum: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  "mdmp",
			Subsystem:  "msg",
			Name:       "delay_summary",
			Help:       "message delay summary.",
			Objectives: map[float64]float64{0.5: 0.01, 0.6: 0.01, 0.7: 0.01, 0.8: 0.01, 0.9: 0.01, 0.95: 0.001, 0.99: 0.001},
		}, []string{"status"}),
	}

	log.Info("init prometheus registry.",
		logf.String("name", c.Prometheus.Node),
		logf.String("zone", c.Prometheus.Zone),
		logf.String("module", c.RPCServer.APPID),
		logf.Any("address", c.Prometheus.Endpoints))
}
