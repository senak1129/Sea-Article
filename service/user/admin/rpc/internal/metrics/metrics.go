package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var (
	AdminLoginCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "",
		Subsystem: "admin_rpc",
		Name:      "login_total",
		Help:      "Admin login attempts and results",
	}, []string{"result"})

	AdminActionCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "",
		Subsystem: "admin_rpc",
		Name:      "high_risk_action_logal",
		Help:      "Total number of high-risk admin actions",
	}, []string{"action", "result"})
)

func InitMetrics() {
	prometheus.Unregister(collectors.NewGoCollector())
	prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	prometheus.Register(AdminLoginCount)
	prometheus.Register(AdminActionCount)
}
