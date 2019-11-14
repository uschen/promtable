package promtable

import (
	"flag"
	"time"
)

// BT -
type BT struct {
	TablePrefix string
	Instance    string
	ProjectID   string
	KeyPath     string
	Expiration  time.Duration
}

// Config -
type Config struct {
	EnsureTables bool

	HashMetricName bool

	Bigtable BT

	EnableLongtermStorage bool
	LongtermBigtable      BT

	Web struct {
		Listen string
	}

	Metric struct {
		Enable bool
		Listen string
	}
}

// ParseFlags -
func ParseFlags() *Config {
	var cfg = new(Config)

	flag.StringVar(&cfg.Bigtable.Instance, "bigtable.instance", "", "The Cloud Bigtable Instance")
	flag.StringVar(&cfg.Bigtable.ProjectID, "bigtable.project_id", "", "The Cloud Bigtable Project ID")
	flag.StringVar(&cfg.Bigtable.KeyPath, "bigtable.keypath", "", "Google Cloud JSON key file path (optional)")
	flag.DurationVar(&cfg.Bigtable.Expiration, "bigtable.expiration", 0, "how long bigtable should keep the metrics, default is infinite")
	flag.StringVar(&cfg.Bigtable.TablePrefix, "bigtable.table_prefix", "", "bigtable table prefix for metrics and meta")

	flag.BoolVar(&cfg.EnableLongtermStorage, "enable-longterm-storage", false, "if true, will enable longterm bigtable storage")

	flag.StringVar(&cfg.LongtermBigtable.Instance, "longterm_bigtable.instance", "", "The Cloud Bigtable Instance")
	flag.StringVar(&cfg.LongtermBigtable.ProjectID, "longterm_bigtable.project_id", "", "The Cloud Bigtable Project ID")
	flag.StringVar(&cfg.LongtermBigtable.KeyPath, "longterm_bigtable.keypath", "", "Google Cloud JSON key file path (optional)")
	flag.DurationVar(&cfg.LongtermBigtable.Expiration, "longterm_bigtable.expiration", 0, "how long bigtable should keep the metrics, default is infinite")
	flag.StringVar(&cfg.LongtermBigtable.TablePrefix, "longterm_bigtable.table_prefix", "", "bigtable table prefix for metrics and meta")

	flag.StringVar(&cfg.Web.Listen, "web.listen", ":9202", "Address to listen on for web endpoints.")

	flag.BoolVar(&cfg.EnsureTables, "ensure-tables", false, "if true, will ensure bigtable tables on startup")
	flag.BoolVar(&cfg.Metric.Enable, "metric.enable", true, "if true, will expose server metric")
	flag.BoolVar(&cfg.HashMetricName, "hash-metric-name", true, "if true, will store metric name hashed")
	flag.StringVar(&cfg.Metric.Listen, "metric.listen", ":9100", "the addr the prometheus metric will be listen on")
	flag.Parse()

	return cfg
}
