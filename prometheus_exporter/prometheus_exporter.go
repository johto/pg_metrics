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
	"time"
)

type PGMetricsCollector struct {
	elog *log.Logger
	dbh *sql.DB
	schemaName string

	fetchQuery string
	metrics map[string]PGMetric
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
		c.elog.Fatal(err)
	}
	for rows.Next() {
		var metric PGMetric

		err = rows.Scan(&metric.Name, &metric.Type, &metric.CounterValue)
		if err != nil {
			c.elog.Fatal(err)
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
		c.elog.Fatal(rows.Err())
	}
	return metrics
}

func (c *PGMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.metrics {
		ch <- metric.Desc
	}
}

func (c *PGMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	metrics := c.fetchMetrics(SKIP_DESCS)
	for _, metric := range metrics {
		x, exists := c.metrics[metric.Name]
		if !exists {
			//
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
}

func newPGMetricsCollector(elog *log.Logger, dbh *sql.DB, schemaName string) *PGMetricsCollector {
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
	}
	c.metrics = c.fetchMetrics(POPULATE_DESCS)
	return c
}

// Maintains the list of 
func metricsCollectorCollector(elog *log.Logger, dbh *sql.DB, schemaName string, registry *prometheus.Registry) {
	currentMetrics := make(map[string]string)
	currentCollector := prometheus.Collector(nil)
	collectQuery := fmt.Sprintf(`SELECT metric_name, metric_type FROM %s.metrics()`, pq.QuoteIdentifier(schemaName))
	for {
		newMetrics := make(map[string]string)

		rows, err := dbh.Query(collectQuery)
		if err != nil {
			elog.Fatal(err)
		}
		for rows.Next() {
			var name string
			var typ string

			err = rows.Scan(&name, &typ)
			if err != nil {
				elog.Fatal(err)
			}
			newMetrics[name] = typ
		}
		if rows.Err() != nil {
			elog.Fatal(rows.Err())
		}

		haveNewMetrics := false
		if currentCollector == nil {
			haveNewMetrics = true
		} else {
			for key := range newMetrics {
				_, exists := currentMetrics[key]
				if !exists {
					haveNewMetrics = true
					break
				}
			}
		}
		if haveNewMetrics {
			if currentCollector != nil {
				registry.Unregister(currentCollector)
			}
			currentCollector = newPGMetricsCollector(elog, dbh, schemaName)
			registry.Register(currentCollector)
		}
		time.Sleep(47 * time.Second)
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

	go metricsCollectorCollector(elog, dbh, schemaName, registry)
	//go metricsQuerier(dbh, schemaName)

	select{}
}
