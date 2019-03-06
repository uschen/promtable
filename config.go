package promtable

import (
	"flag"
)

// Config -
type Config struct {
	EnsureTables bool

	HashMetricName bool

	Bigtable struct {
		TablePrefix string
		Instance    string
		ProjectID   string
		KeyPath     string
	}

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
	flag.StringVar(&cfg.Web.Listen, "web.listen", ":9202", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.Bigtable.TablePrefix, "bigtable.table_prefix", "", "bigtable table prefix for metrics and meta")
	flag.BoolVar(&cfg.EnsureTables, "ensure-tables", false, "if true, will ensure bigtable tables on startup")
	flag.BoolVar(&cfg.Metric.Enable, "metric.enable", true, "if true, will expose server metric")
	flag.BoolVar(&cfg.HashMetricName, "hash-metric-name", true, "if true, will store metric name hashed")
	flag.StringVar(&cfg.Metric.Listen, "metric.listen", ":9100", "the addr the prometheus metric will be listen on")
	flag.Parse()

	return cfg
}
