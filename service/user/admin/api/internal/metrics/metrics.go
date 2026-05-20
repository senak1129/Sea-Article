package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var (
	AdminApiInterceptCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "",
		Subsystem: "admin_api",
		Name:      "intercept_total",
		Help:      "Total number of Admin API interceptions",
	}, []string{"path", "result"})
)

func InitMetrics() {
	prometheus.Unregister(collectors.NewGoCollector())
	prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	prometheus.Register(AdminApiInterceptCount)
}
