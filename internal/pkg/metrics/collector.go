package metrics

// import "github.com/prometheus/client_golang/prometheus"

// type rateGauge struct {
// 	prometheus.Gauge
// 	desc *prometheus.Desc
// }

// func NewrateGauge(desc *prometheus.Desc) rateGauge {
// 	return rateGauge{
// 		desc: desc,
// 	}
// }

// func (this rateGauge) Describe(ch chan<- *prometheus.Desc) {
// 	ch <- this.desc
// }

// func (this rateGauge) Collect(ch chan<- prometheus.Metric) {
// 	ch <- this
// 	this.reset()
// }

// func (this rateGauge) reset() {
// 	this.Set(0)
// }
