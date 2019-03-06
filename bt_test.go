package promtable_test

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"

	"github.com/twmb/murmur3"
	"github.com/uschen/promtable"
	"github.com/uschen/promtable/prompb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	"github.com/stretchr/testify/assert"
)

func TestBigtable_Put(t *testing.T) {
	tests := []struct {
		hash bool
	}{
		{hash: true},
		{hash: false},
	}

	testFunc := func(test struct {
		hash bool
	}) {
		s := newBTTestingServer(t, test.hash)
		defer s.Close()

		baseDay := int64(5)
		ts, _ := complexSerices(baseDay, test.hash)
		var exp = make([]*prompb.TimeSeries, len(ts))
		for i := range ts {
			exp[i] = &ts[i]
		}

		err := s.store.Put(context.Background(), &prompb.WriteRequest{
			Timeseries: ts,
		})
		assert.Nil(t, err)

		// query it back
		assert.Equal(t, "ma", ts[0].Labels[0].Value)
		q := &prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: promtable.MetricNameLabel, Value: ts[0].Labels[0].Value},
			},
		}

		// meta row
		srs, err := s.store.QueryMetaRows(context.Background(), q)
		assert.Nil(t, err)
		assert.Len(t, srs, 2, "length should be 2")

		res, err := s.store.Query(context.Background(), q)
		assert.Nil(t, err)
		assert.NotNil(t, res)

		assert.EqualValues(t, exp, res)
	}
	for _, test := range tests {
		testFunc(test)
	}
}

// TestQuery mainly tests the timestamp ranges and errors.
func TestQuery(t *testing.T) {
	for _, hash := range []bool{true, false} {
		s := newBTTestingServer(t, hash)

		baseDay := int64(17961)
		ts, tsm := complexSerices(baseDay, hash)
		var exp = make([]*prompb.TimeSeries, len(ts))
		for i := range ts {
			exp[i] = &ts[i]
		}

		err := s.store.Put(context.Background(), &prompb.WriteRequest{
			Timeseries: ts,
		})
		assert.Nil(t, err)

		type expectedFunc func() []*prompb.TimeSeries

		tests := []struct {
			note         string
			query        *prompb.Query
			expected     []*prompb.TimeSeries
			expectedFunc expectedFunc // expectedFunc takes priority
			expErr       error
		}{
			{
				note: "all",
				query: &prompb.Query{
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: promtable.MetricNameLabel, Value: ts[0].Labels[0].Value},
					},
				},
				expected: exp,
			},
			{
				note: "none",
				query: &prompb.Query{
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: promtable.MetricNameLabel, Value: ts[0].Labels[0].Value},
						{Type: prompb.LabelMatcher_EQ, Name: "kubernetes_pod_name", Value: "5"},
					},
				},
				expected: []*prompb.TimeSeries{},
			},
			{
				note: "query selected labels",
				query: &prompb.Query{
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: promtable.MetricNameLabel, Value: ts[0].Labels[0].Value},
						{Type: prompb.LabelMatcher_EQ, Name: "kubernetes_pod_name", Value: "5bbzk"},
					},
				},
				expected: []*prompb.TimeSeries{tsm["5"]},
			},
			{
				note: "query with start",
				query: &prompb.Query{
					StartTimestampMs: (baseDay + 2) * 24 * 60 * 60 * 1000,
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: promtable.MetricNameLabel, Value: ts[0].Labels[0].Value},
					},
				},
				expectedFunc: func() []*prompb.TimeSeries {
					return []*prompb.TimeSeries{
						{
							Labels:  exp[0].Labels,
							Samples: exp[0].Samples[2:],
						},
						{
							Labels:  exp[1].Labels,
							Samples: exp[1].Samples[2:],
						},
					}
				},
			},
			{
				note: "query with start end",
				query: &prompb.Query{
					StartTimestampMs: (baseDay + 2) * 24 * 60 * 60 * 1000,
					EndTimestampMs:   (baseDay+2)*24*60*60*1000 + 3,
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: promtable.MetricNameLabel, Value: ts[0].Labels[0].Value},
					},
				},
				expectedFunc: func() []*prompb.TimeSeries {
					return []*prompb.TimeSeries{
						{
							Labels:  exp[0].Labels,
							Samples: exp[0].Samples[2:3],
						},
						{
							Labels:  exp[1].Labels,
							Samples: exp[1].Samples[2:3],
						},
					}
				},
			},
			{
				note: "err when no __name__",
				query: &prompb.Query{
					Matchers: []*prompb.LabelMatcher{
						{Type: prompb.LabelMatcher_EQ, Name: "kubernetes_pod_name", Value: "5"},
					},
				},
				expErr:   errors.New(""),
				expected: nil,
			},
		}

		for _, test := range tests {
			res, err := s.store.Query(context.Background(), test.query)
			if test.expErr != nil {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
			if test.expectedFunc != nil {
				assert.EqualValues(t, test.expectedFunc(), res)
			} else {
				assert.EqualValues(t, test.expected, res)
			}
		}
		s.Close()
	}

}

func TestBigtable_Read(t *testing.T) {
	for _, hash := range []bool{true, false} {
		s := newBTTestingServer(t, hash)

		baseDay := int64(5)
		ts, tsm := complexSerices(baseDay, hash)
		var exp = make([]*prompb.TimeSeries, len(ts))
		for i := range ts {
			exp[i] = &ts[i]
		}

		err := s.store.Put(context.Background(), &prompb.WriteRequest{
			Timeseries: ts,
		})
		assert.Nil(t, err)

		type expectedFunc func() []*prompb.TimeSeries

		req := &prompb.ReadRequest{
			Queries: []*prompb.Query{
				{
					StartTimestampMs: (baseDay+1)*24*60*60*1000 + 2,
					EndTimestampMs:   (baseDay+2)*24*60*60*1000 + 5,
					Matchers: []*prompb.LabelMatcher{
						{
							Type:  prompb.LabelMatcher_EQ,
							Name:  promtable.MetricNameLabel,
							Value: "ma",
						},
						{
							Type:  prompb.LabelMatcher_EQ,
							Name:  "kubernetes_pod_name",
							Value: "nc69q",
						},
					},
				},
				{
					StartTimestampMs: (baseDay+2)*24*60*60*1000 + 2,
					EndTimestampMs:   (baseDay+3)*24*60*60*1000 + 5,
					Matchers: []*prompb.LabelMatcher{
						{
							Type:  prompb.LabelMatcher_EQ,
							Name:  promtable.MetricNameLabel,
							Value: "ma",
						},
						{
							Type:  prompb.LabelMatcher_EQ,
							Name:  "kubernetes_pod_name",
							Value: "5bbzk",
						},
					},
				},
			},
		}

		res, err := s.store.Read(s.ctx, req)
		assert.Nil(t, err)
		assert.Len(t, res.Results, 2)

		expected := []*prompb.QueryResult{
			{
				Timeseries: []*prompb.TimeSeries{
					{
						Labels:  tsm["6"].Labels,
						Samples: tsm["6"].Samples[1:4],
					},
				},
			},
			{
				Timeseries: []*prompb.TimeSeries{
					{
						Labels:  tsm["5"].Labels,
						Samples: tsm["5"].Samples[2:],
					},
				},
			},
		}
		assert.EqualValues(t, expected, res.Results)
		s.Close()
	}
}

// func TestTimestampToColumn(t *testing.T) {
// 	var (
// 		baseTs      = int64(1551841950018)
// 		pre         = promtable.TimestampToColumn(baseTs)
// 		ok     bool = true
// 	)

// 	for i := int64(1); i < 1000; i++ {
// 		cur := promtable.TimestampToColumn(baseTs + i*100)
// 		if pre >= cur {
// 			ok = false
// 			break
// 		}
// 		pre = cur
// 	}
// 	assert.True(t, ok)
// }

func TestHash(t *testing.T) {
	var h128_byte murmur3.Hash128 = murmur3.New128()
	h128_byte.Write([]byte("testing"))
	res := h128_byte.Sum(nil)
	assert.Len(t, res, 128/8)
}

type btTestingServer struct {
	ac    *bigtable.AdminClient
	c     *bigtable.Client
	store *promtable.Store

	ctx    context.Context
	cancel context.CancelFunc
}

func (bts *btTestingServer) Close() {
	bts.cancel()
	if err := bts.ac.Close(); err != nil {
	}
	if err := bts.c.Close(); err != nil {
	}
	if err := bts.store.Close(); err != nil {
	}
}

func newBTTestingServer(t *testing.T, hash bool) *btTestingServer {
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
	}

	store, err := promtable.NewStore(
		promtable.StoreWithBigtableAdminClient(ac),
		promtable.StoreWithBigtableClient(c),
		promtable.StoreWithTableNamePrefix("testing"),
		promtable.StoreWithHashLabels(hash),
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

// complexSerices -
// hash will affect the sort order
func complexSerices(baseDay int64, hash bool) ([]prompb.TimeSeries, map[string]*prompb.TimeSeries) {
	ts5 := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: promtable.MetricNameLabel, Value: "ma"},
			{Name: "l1", Value: "v1"},
			{Name: "l2", Value: "v2"},
			{Name: "kubernetes_pod_name", Value: "5bbzk"},
			{Name: "l3", Value: "v3"},
			{Name: "l4", Value: "v4,#4"},
		},
		Samples: []prompb.Sample{
			{Value: 0.1, Timestamp: (baseDay+1)*24*60*60*1000 + 1},
			{Value: 0.2, Timestamp: (baseDay+1)*24*60*60*1000 + 2},
			{Value: 0.3, Timestamp: (baseDay+2)*24*60*60*1000 + 3},
			{Value: 0.4, Timestamp: (baseDay+2)*24*60*60*1000 + 4},
			{Value: 0.5, Timestamp: (baseDay+3)*24*60*60*1000 + 5},
			// {Value: 0.6, Timestamp: (baseDay+3)*24*60*60*1000 + 5},
		},
	}
	ts6 := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: promtable.MetricNameLabel, Value: "ma"},
			{Name: "l1", Value: "v1"},
			{Name: "l2", Value: "v2"},
			{Name: "kubernetes_pod_name", Value: "nc69q"},
			{Name: "l3", Value: "v3"},
			{Name: "l4", Value: "v4,#4"},
		},
		Samples: []prompb.Sample{
			{Value: 1.1, Timestamp: (baseDay+1)*24*60*60*1000 + 1},
			{Value: 1.2, Timestamp: (baseDay+1)*24*60*60*1000 + 2},
			{Value: 1.3, Timestamp: (baseDay+2)*24*60*60*1000 + 3},
			{Value: 1.4, Timestamp: (baseDay+2)*24*60*60*1000 + 4},
			{Value: 1.5, Timestamp: (baseDay+3)*24*60*60*1000 + 5},
			// {Value: 1.6, Timestamp: (baseDay+3)*24*60*60*1000 + 5},
		},
	}
	tss := []prompb.TimeSeries{
		ts5,
		ts6,
	}

	tsm := map[string]*prompb.TimeSeries{
		"5": &ts5,
		"6": &ts6,
	}

	if hash {
		var hashFunc = func(ts prompb.TimeSeries) string {
			var (
				buf  bytes.Buffer
				h128 = murmur3.New128()
			)
			name, labelsString, err := promtable.LabelsToRowKeyComponents(ts.Labels)
			if err != nil {
				panic(err)
			}
			buf.WriteString(name)
			buf.WriteRune('#')
			buf.WriteString(labelsString)
			buf.WriteTo(h128)
			return string(h128.Sum(nil))
		}

		sort.Slice(tss, func(i, j int) bool {
			return hashFunc(tss[i]) < hashFunc(tss[j])
		})
	}

	return tss, tsm
}
