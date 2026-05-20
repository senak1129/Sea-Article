package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var (
	UserApiInterceptCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "",
		Subsystem: "user_api",
		Name:      "intercept_total",
		Help:      "Total number of API frontline interceptions",
	}, []string{"path", "result"})
)

func InitMetrics() {
	prometheus.Unregister(collectors.NewGoCollector())
	prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	prometheus.Register(UserApiInterceptCount)
}
