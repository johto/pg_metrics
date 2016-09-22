package main

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"os"
	"strings"
)

type PGMetricsCollector struct {
	elog *log.Logger
	dbh *sql.DB
	schemaName string

	fetchQuery string
	metrics map[string]PGMetric
	statsMetrics []*prometheus.Desc

	refreshMetricListRequest chan<- struct{}
}

type PGMetric struct {
	// possibly nil
	Desc *prometheus.Desc

	Name string
	Type string

	// counters
	CounterValue int64
}

const (
	POPULATE_DESCS bool = true
	SKIP_DESCS = false
)

func (c *PGMetricsCollector) fetchMetrics(populateDescs bool) map[string]PGMetric {
	metrics := make(map[string]PGMetric)

	rows, err := c.dbh.Query(c.fetchQuery)
	if err != nil {
		c.elog.Fatalf("ERROR:  %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		var metric PGMetric

		err = rows.Scan(&metric.Name, &metric.Type, &metric.CounterValue)
		if err != nil {
			c.elog.Fatalf("ERROR:  %s", err)
		}
		if populateDescs {
			metric.Desc = prometheus.NewDesc(
				metric.Name,
				metric.Name + " " + strings.ToLower(metric.Type),
				nil,
				nil,
			)
		}
		metrics[metric.Name] = metric
	}
	if rows.Err() != nil {
		c.elog.Fatalf("ERROR:  %s", rows.Err())
	}
	return metrics
}

func (c *PGMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.statsMetrics {
		ch <- desc
	}
	for _, metric := range c.metrics {
		ch <- metric.Desc
	}
}

func (c *PGMetricsCollector) fetchStats() (maxMetrics int32, numMetrics int32) {
	statsQuery := fmt.Sprintf(`SELECT max_metrics, num_metrics FROM %s.metrics_stats()`, pq.QuoteIdentifier(c.schemaName))
	err := c.dbh.QueryRow(statsQuery).Scan(&maxMetrics, &numMetrics)
	if err != nil {
		c.elog.Fatalf("ERROR:  %s", err)
	}
	return maxMetrics, numMetrics
}


func (c *PGMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	maxMetrics, numMetrics := c.fetchStats()
	ch <- prometheus.MustNewConstMetric(c.statsMetrics[0], prometheus.GaugeValue, float64(maxMetrics))
	ch <- prometheus.MustNewConstMetric(c.statsMetrics[1], prometheus.GaugeValue, float64(numMetrics))

	metrics := c.fetchMetrics(SKIP_DESCS)
	needRefresh := false
	for _, metric := range metrics {
		x, exists := c.metrics[metric.Name]
		if !exists {
			needRefresh = true
			continue
		}
		desc := x.Desc

		switch metric.Type {
		case "COUNTER":
			ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(metric.CounterValue))
		default:
			panic(fmt.Sprintf("unexpected metric type %s", metric.Type))
		}
	}

	if needRefresh {
		select {
			case c.refreshMetricListRequest <- struct{}{}:
			default:
		}
	}
}

func newPGMetricsCollector(elog *log.Logger, dbh *sql.DB, schemaName string, refreshMetricListRequest chan<- struct{}) *PGMetricsCollector {
	fetchQuery := fmt.Sprintf(
		`SELECT ` +
		`metric_name, metric_type, counter_value ` +
		`FROM %s.metrics()`,
		pq.QuoteIdentifier(schemaName),
	)
	c := &PGMetricsCollector{
		elog: elog,
		dbh: dbh,
		schemaName: schemaName,
		fetchQuery: fetchQuery,
		refreshMetricListRequest: refreshMetricListRequest,
	}
	c.metrics = c.fetchMetrics(POPULATE_DESCS)
	c.statsMetrics = []*prometheus.Desc{
		prometheus.NewDesc(
			"max_metrics",
			"tu-turu",
			nil,
			nil,
		),
		prometheus.NewDesc(
			"num_metrics",
			"tu-turu",
			nil,
			nil,
		),
	}

	return c
}

func metricsListUpdaterLoop(elog *log.Logger, dbh *sql.DB, schemaName string, registry *prometheus.Registry) {
	for {
		refreshMetricListRequest := make(chan struct{}, 1)
		collector := newPGMetricsCollector(elog, dbh, schemaName, refreshMetricListRequest)
		err := registry.Register(collector)
		if err != nil {
			elog.Fatalf("ERROR:  %s", err)
		}
		<-refreshMetricListRequest
		elog.Printf("Refreshing the list of metrics")
		registry.Unregister(collector)
	}
}

func main() {
	elog := log.New(os.Stderr, "", log.LstdFlags)
	schemaName := "metrics"

	dbh, err := sql.Open("postgres", "")
	if err != nil {
		elog.Fatal(err)
	}
	dbh.SetMaxOpenConns(1)
	dbh.SetMaxIdleConns(1)
	err = dbh.Ping()
	if err != nil {
		elog.Fatal(err)
	}

	registry := prometheus.NewPedanticRegistry()
	httpHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		ErrorLog: elog,
	})
	http.Handle("/metrics", httpHandler)
	go func() {
		elog.Fatal(http.ListenAndServe(":8080", nil))
	}()

	metricsListUpdaterLoop(elog, dbh, schemaName, registry)
}
