package metrics

import "time"

func (this *MetaMetrics) RuleInc() {
	this.ruleCounter.Inc()
}

func (this *MetaMetrics) RuleDec() {
	this.ruleCounter.Dec()
}

func (this *MetaMetrics) RouteInc() {
	this.routeCounter.Inc()
}

func (this *MetaMetrics) RouteDec() {
	this.routeCounter.Dec()
}

func (this *MetaMetrics) SubscriptionInc() {
	this.subscriptionCounter.Inc()
}

func (this *MetaMetrics) SubscriptionDec() {
	this.subscriptionCounter.Dec()
}

func (this *MetaMetrics) SubscriptionAdd(num int) {
	this.subscriptionCounter.Add(float64(num))
}

func (this *MetaMetrics) SubscriptionSub(num int) {
	this.subscriptionCounter.Sub(float64(num))
}

func (this *MetaMetrics) EntityInc() { //设备上线，counter++
	this.entityOnlineCounter.Inc()
}

func (this *MetaMetrics) EntityDec() { //设备离线，离线counter++
	this.entityOfflineCounter.Inc()
}

func (this *MetaMetrics) ResourceSync(resource string, num, size int) {

	this.resourceSync.WithLabelValues(resource).Add(float64(num))
	this.resourceSyncSent.WithLabelValues(resource).Add(float64(size))
}

func (this *MetaMetrics) MsgTrace(inout, status string) {
	this.msgInOut.WithLabelValues(inout, status).Inc()
}

func (this *MetaMetrics) MsgRecvRate(size int) {
	this.msgReceivedByte.Add(float64(size))
}

func (this *MetaMetrics) MsgSentRate(size int) {
	this.msgSentByte.Add(float64(size))
}

func (this *MetaMetrics) MsgDelay(status string, delay float64) {
	this.msgDelaySum.WithLabelValues(status).Observe(delay)
}

func RuleInc()                                    { metaMetric.RuleInc() }
func RuleDec()                                    { metaMetric.RuleDec() }
func RouteInc()                                   { metaMetric.RouteInc() }
func RouteDec()                                   { metaMetric.RouteDec() }
func EntityInc()                                  { metaMetric.EntityInc() }
func EntityDec()                                  { metaMetric.EntityDec() }
func SubscriptionInc()                            { metaMetric.SubscriptionInc() }
func SubscriptionDec()                            { metaMetric.SubscriptionDec() }
func SubscriptionAdd(num int)                     { metaMetric.SubscriptionAdd(num) }
func SubscriptionSub(num int)                     { metaMetric.SubscriptionSub(num) }
func ResourceSync(resource string, num, size int) { metaMetric.ResourceSync(resource, num, size) }

func MsgReceived(num int) {
	metaMetric.MsgTrace(MsgInput, StatusAll)
	metaMetric.MsgRecvRate(num)
}

func MsgRecvFail() {
	metaMetric.MsgTrace(MsgInput, StatusFailure)
}

func MsgSent(status string, num int) {
	//status = {"success" | "failure"}
	metaMetric.MsgTrace(MsgOutput, status)
	metaMetric.MsgSentRate(num)
}

type msgContext struct {
	start_t time.Time
}

func NewMsgContext() *msgContext {
	return &msgContext{
		start_t: time.Now(),
	}
}

func (this *msgContext) Observe(err error) {
	status := StatusSuccess
	if err != nil {
		status = StatusFailure
	}
	metaMetric.MsgDelay(status, float64(time.Since(this.start_t).Microseconds())/1000.0)
}
