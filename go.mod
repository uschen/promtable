module github.com/uschen/promtable

go 1.12

require (
	cloud.google.com/go/bigtable v1.1.0
	github.com/gogo/protobuf v1.2.2-0.20190611061853-dadb62585089
	github.com/golang/snappy v0.0.1
	github.com/prometheus/client_golang v1.0.0
	github.com/stretchr/testify v1.3.0
	github.com/twmb/murmur3 v1.0.0
	go.uber.org/atomic v1.3.2 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/net v0.0.0-20190628185345-da137c7871d7 // indirect
	golang.org/x/sys v0.0.0-20190710143415-6ec70d6a5542 // indirect
	google.golang.org/api v0.13.0
	google.golang.org/genproto v0.0.0-20191028173616-919d9bdd9fe6
	google.golang.org/grpc v1.22.0
)

replace cloud.google.com/go => github.com/uschen/google-cloud-go v0.47.1-0.20191112205320-1228d6beb191

replace cloud.google.com/go/bigtable => github.com/uschen/google-cloud-go/bigtable v1.0.1-0.20191112205320-1228d6beb191
