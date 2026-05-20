package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

var (
	UserRegisterCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "",
		Subsystem: "user_rpc",
		Name:      "register_total",
		Help:      "Total number of user registerations",
	}, []string{"result"})

	UserLoginCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "",
		Subsystem: "user_rpc",
		Name:      "login_total",
		Help:      "Total number of user logins",
	}, []string{"result"})
)

func InitMetrics() {
	prometheus.Unregister(collectors.NewGoCollector())
	prometheus.Unregister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	prometheus.Register(UserRegisterCount)
	prometheus.Register(UserLoginCount)

}
