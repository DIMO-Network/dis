package processors

// Metric name constants shared across dis processors.
const (
	MetricSignalsPerReport = "dis_signals_per_report"
	MetricEventsPerReport  = "dis_events_per_report"
	MetricCHInserts        = "dis_clickhouse_rows_total"
	MetricCHInsertErrors   = "dis_clickhouse_insert_errors_total"
)

// Table label values for ClickHouse metrics.
const (
	TableSignal = "signal"
	TableEvent  = "event"
)
