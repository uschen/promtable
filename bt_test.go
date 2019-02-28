package promtable_test

import (
	"context"
	"testing"

	"github.com/uschen/promtable"
	"github.com/uschen/promtable/prompb"
	"github.com/uschen/promtable/util/testutil"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"

	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func TestStore_WriteRows(t *testing.T) {
	s := newBTTestingServer(t)
	defer s.Close()

	ts := newTimeseries(0)
	rs, err := promtable.RowsFromTimeseries(s.ns, ts)
	if err != nil {
		t.Error(err)
		t.FailNow()
		return
	}

	if err := s.store.WriteRows(s.ctx, rs); err != nil {
		t.Error(err)
		t.FailNow()
		return
	}

	// read the row.
	var rows []bigtable.Row

	tbl := s.c.Open(promtable.DefaultTableName)

	err = tbl.ReadRows(s.ctx, bigtable.InfiniteRange(""), func(r bigtable.Row) bool {
		rows = append(rows, r)
		return true
	})
	if err != nil {
		t.Error(err)
		t.FailNow()
		return
	}

	testutil.Equals(t, 3, len(rows))
	var actualSamples []prompb.Sample
	for ri := range rows {
		name, _, labels, ok := promtable.ParseRowKey(rows[ri].Key())
		testutil.Equals(t, len(ts.Labels)-1, len(labels))
		testutil.Equals(t, true, ok)
		ts1, err := promtable.BtRowToPromTimeSeries(rows[ri], name, labels)
		if err != nil {
			t.Error(err)
		}
		testutil.Equals(t, len(ts.Labels), len(ts1.Labels))
		for i := 0; i < len(ts.Labels); i++ {
			testutil.Equals(t, ts.Labels[i], ts1.Labels[i])
		}
		// each should have two samples, except the last one, due to the same timestamp
		if ri == len(rows)-1 {
			testutil.Equals(t, 1, len(ts1.Samples))
		} else {
			testutil.Equals(t, 2, len(ts1.Samples))
		}
		actualSamples = append(actualSamples, ts1.Samples...)
	}
	testutil.Equals(t, ts.Samples[0:3], actualSamples[0:3])
	testutil.Equals(t, ts.Samples[5], actualSamples[4])
}

func TestStore_Query(t *testing.T) {
	s := newBTTestingServer(t)
	defer s.Close()

	ts := newTimeseries(0)
	rs, err := promtable.RowsFromTimeseries(s.ns, ts)
	if err != nil {
		t.Error(err)
		t.FailNow()
		return
	}

	if err := s.store.WriteRows(s.ctx, rs); err != nil {
		t.Error(err)
		t.FailNow()
		return
	}

	q := &prompb.Query{
		StartTimestampMs: 1*60*60*1000 + 2,
		EndTimestampMs:   3*60*60*1000 + 5,
		Matchers: []*prompb.LabelMatcher{
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  promtable.MetricNameLabel,
				Value: "ma",
			},
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "l4",
				Value: "v4,#4",
			},
		},
	}

	res, err := s.store.Query(s.ctx, s.ns, q)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	// fmt.Printf("\nres:\n%v\n", res)
	testutil.Equals(t, 1, len(res))
	testutil.Equals(t, 4, len(res[0].Samples))
	testutil.Equals(t, ts.Samples[1:4], res[0].Samples[0:3])
	testutil.Equals(t, ts.Samples[5], res[0].Samples[3])
}

func TestStore_Query2(t *testing.T) {
	const baseHour = 430922
	s := newBTTestingServer(t)
	defer s.Close()

	tss := complexSerices(baseHour)
	var rrs []*promtable.TimeseriesRow
	for i := range tss {
		rs, err := promtable.RowsFromTimeseries(s.ns, tss[i])
		if err != nil {
			t.Error(err)
			t.FailNow()
			return
		}

		rrs = append(rrs, rs...)
	}

	if err := s.store.WriteRows(s.ctx, rrs); err != nil {
		t.Error(err)
		t.FailNow()
		return
	}

	q := &prompb.Query{
		StartTimestampMs: (baseHour+1)*60*60*1000 + 2,
		EndTimestampMs:   (baseHour+3)*60*60*1000 + 5,
		Matchers: []*prompb.LabelMatcher{
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  promtable.MetricNameLabel,
				Value: "ma",
			},
			{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "l4",
				Value: "v4,#4",
			},
		},
	}

	res, err := s.store.Query(s.ctx, s.ns, q)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	testutil.Equals(t, 2, len(res))
	testutil.Equals(t, 4, len(res[0].Samples))
}

func TestStore_Read(t *testing.T) {
	s := newBTTestingServer(t)
	defer s.Close()

	ts := newTimeseries(0)
	rs, err := promtable.RowsFromTimeseries(s.ns, ts)
	if err != nil {
		t.Error(err)
		t.FailNow()
		return
	}

	if err := s.store.WriteRows(s.ctx, rs); err != nil {
		t.Error(err)
		t.FailNow()
		return
	}

	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				StartTimestampMs: 1*60*60*1000 + 2,
				EndTimestampMs:   3*60*60*1000 + 5,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  promtable.MetricNameLabel,
						Value: "ma",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "l4",
						Value: "v4,#4",
					},
				},
			},
		},
	}

	res, err := s.store.Read(s.ctx, s.ns, req)
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	testutil.Equals(t, 1, len(res.Results))
	testutil.Equals(t, 1, len(res.Results[0].Timeseries))
	resTs := res.Results[0].Timeseries[0]
	testutil.Equals(t, 4, len(resTs.Samples))
	testutil.Equals(t, ts.Samples[1:4], resTs.Samples[0:3])
	testutil.Equals(t, ts.Samples[5], resTs.Samples[3])
}

func TestStore_Put(t *testing.T) {}

type btTestingServer struct {
	ac    *bigtable.AdminClient
	c     *bigtable.Client
	store *promtable.Store

	ctx    context.Context
	cancel context.CancelFunc

	ns string
}

func (bts *btTestingServer) Close() {
	bts.cancel()
	if err := bts.ac.Close(); err != nil {
	}
	if err := bts.c.Close(); err != nil {
	}
}

func newBTTestingServer(t *testing.T) *btTestingServer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	srv, err := bttest.NewServer("127.0.0.1:0")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	ac, c, err := emulatorClient(ctx, srv.Addr)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	bts := &btTestingServer{
		ac:     ac,
		c:      c,
		ctx:    ctx,
		cancel: cancel,

		ns: "ns",
	}

	store, err := promtable.NewStore(
		promtable.StoreWithBigtableAdminClient(ac),
		promtable.StoreWithBigtableClient(c),
	)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	bts.store = store

	if err := store.EnsureTables(ctx); err != nil {
		t.Error(err)
		t.FailNow()
		return nil
	}

	return bts
}

func emulatorClient(ctx context.Context, addr string) (*bigtable.AdminClient, *bigtable.Client, error) {
	conn1, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	conn2, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	proj, instance := "proj", "instance"
	adminClient, err := bigtable.NewAdminClient(ctx, proj, instance, option.WithGRPCConn(conn1))
	if err != nil {
		return nil, nil, err
	}
	client, err := bigtable.NewClient(ctx, proj, instance, option.WithGRPCConn(conn2))
	if err != nil {
		return nil, nil, err
	}
	return adminClient, client, nil
}

// newTimeseries return a series that will span across three
// base values (rows)
func newTimeseries(baseHour int64) prompb.TimeSeries {
	ts := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: promtable.MetricNameLabel, Value: "ma"},
			{Name: "l1", Value: "v1"},
			{Name: "l2", Value: "v2"},
			{Name: "l3", Value: "v3"},
			{Name: "l4", Value: "v4,#4"},
		},
		Samples: []prompb.Sample{
			{Value: 0.1, Timestamp: (baseHour+1)*60*60*1000 + 1},
			{Value: 0.2, Timestamp: (baseHour+1)*60*60*1000 + 2},
			{Value: 0.3, Timestamp: (baseHour+2)*60*60*1000 + 3},
			{Value: 0.4, Timestamp: (baseHour+2)*60*60*1000 + 4},
			{Value: 0.5, Timestamp: (baseHour+3)*60*60*1000 + 5},
			{Value: 0.6, Timestamp: (baseHour+3)*60*60*1000 + 5},
		},
	}
	return ts
}

func complexSerices(baseHour int64) []prompb.TimeSeries {
	tss := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: promtable.MetricNameLabel, Value: "ma"},
				{Name: "l1", Value: "v1"},
				{Name: "l2", Value: "v2"},
				{Name: "kubernetes_pod_name", Value: "nu-fe-grpc-gateway-deployment-757d8c8464-5bbzk"},
				{Name: "l3", Value: "v3"},
				{Name: "l4", Value: "v4,#4"},
			},
			Samples: []prompb.Sample{
				{Value: 0.1, Timestamp: (baseHour+1)*60*60*1000 + 1},
				{Value: 0.2, Timestamp: (baseHour+1)*60*60*1000 + 2},
				{Value: 0.3, Timestamp: (baseHour+2)*60*60*1000 + 3},
				{Value: 0.4, Timestamp: (baseHour+2)*60*60*1000 + 4},
				{Value: 0.5, Timestamp: (baseHour+3)*60*60*1000 + 5},
				{Value: 0.6, Timestamp: (baseHour+3)*60*60*1000 + 5},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: promtable.MetricNameLabel, Value: "ma"},
				{Name: "l1", Value: "v1"},
				{Name: "l2", Value: "v2"},
				{Name: "kubernetes_pod_name", Value: "nu-fe-grpc-gateway-deployment-757d8c8464-nc69q"},
				{Name: "l3", Value: "v3"},
				{Name: "l4", Value: "v4,#4"},
			},
			Samples: []prompb.Sample{
				{Value: 1.1, Timestamp: (baseHour+1)*60*60*1000 + 1},
				{Value: 1.2, Timestamp: (baseHour+1)*60*60*1000 + 2},
				{Value: 1.3, Timestamp: (baseHour+2)*60*60*1000 + 3},
				{Value: 1.4, Timestamp: (baseHour+2)*60*60*1000 + 4},
				{Value: 1.5, Timestamp: (baseHour+3)*60*60*1000 + 5},
				{Value: 1.6, Timestamp: (baseHour+3)*60*60*1000 + 5},
			},
		},
	}
	return tss
}
