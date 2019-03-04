## Data Model
Each row will store maxmium 4 billion metric values.

### RowKey
```
<namespace>#<metric_name>#<label_name>=<label_value>,<label_name2>=<label_value2>#<base_value>
```

### Column
* Family: `p` (point)
* Qualifier: value delta

### Read
* Need to support query: <metric_name> + <time_range> + <label_name>=<label_value>...

e.g. :
* metric: cpu_usage
* labels: 
  * instance,zone,region
* for two metrics:
  * cpu_usage{instance=a,zone=b,region=c}
  * cpu_usage{instance=b,zone=b,region=c}
  * cpu_usage{instance=c,zone=b}
* if labels are ordered
  * cpu_usage{instance=a,region=c,zone=b}
  * cpu_usage{region=c,zone=b}
  * cpu_usage{instance=c,zone=b}
* and the query is cpu_usage{region=c} from ts:1000 to ts:2000
  * prefix: `cpu_usage{`
