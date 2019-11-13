package btfilter

import (
	"time"

	btpb "google.golang.org/genproto/googleapis/bigtable/v2"
)

// BTFiltersToRowFilters -
func BTFiltersToRowFilters(filters ...*btpb.RowFilter) []*RowFilter {
	if filters == nil {
		return nil
	}
	var res = make([]*RowFilter, len(filters))
	for i := range filters {
		res[i] = (*RowFilter)(filters[i])
	}
	return res
}

// RowFiltersToBTFilters -
func RowFiltersToBTFilters(filters ...*RowFilter) []*btpb.RowFilter {
	if filters == nil {
		return nil
	}
	var res = make([]*btpb.RowFilter, len(filters))
	for i := range filters {
		res[i] = (*btpb.RowFilter)(filters[i])
	}
	return res
}

// ChainFilter -
func ChainFilter(sub ...*RowFilter) *RowFilter {
	var res = RowFilter(btpb.RowFilter{
		Filter: &btpb.RowFilter_Chain_{
			Chain: &btpb.RowFilter_Chain{
				Filters: RowFiltersToBTFilters(sub...),
			},
		},
	})
	return &res
}

// InterleaveFilter -
func InterleaveFilter(sub ...*RowFilter) *RowFilter {
	var res = RowFilter(btpb.RowFilter{
		Filter: &btpb.RowFilter_Interleave_{
			Interleave: &btpb.RowFilter_Interleave{
				Filters: RowFiltersToBTFilters(sub...),
			},
		},
	})
	return &res
}

// ConditionFilter -
func ConditionFilter(predicate, trueFilter, falseFilter *RowFilter) *RowFilter {
	var res = RowFilter(btpb.RowFilter{
		Filter: &btpb.RowFilter_Condition_{
			Condition: &btpb.RowFilter_Condition{
				PredicateFilter: predicate.Proto(),
				TrueFilter:      trueFilter.Proto(),
				FalseFilter:     falseFilter.Proto(),
			},
		},
	})
	return &res
}

// SinkFilter -
func SinkFilter(sink bool) *RowFilter {
	var res = RowFilter(btpb.RowFilter{
		Filter: &btpb.RowFilter_Sink{
			Sink: sink,
		},
	})
	return &res
}

// PassAllFilter -
func PassAllFilter(passAll bool) *RowFilter {
	var res = RowFilter(btpb.RowFilter{
		Filter: &btpb.RowFilter_PassAllFilter{
			PassAllFilter: passAll,
		},
	})
	return &res
}

// BlockAllFilter -
func BlockAllFilter(blockAll bool) *RowFilter {
	var res = RowFilter(btpb.RowFilter{
		Filter: &btpb.RowFilter_BlockAllFilter{
			BlockAllFilter: blockAll,
		},
	})
	return &res
}

// ColumnRangeFilter -
func ColumnRangeFilter(family string, start, end isColumnRangeType) *RowFilter {
	var res = new(btpb.RowFilter)

	cr := &btpb.ColumnRange{
		FamilyName: family,
	}
	if start != nil {
		switch sq := start.(type) {
		case ColumnRangeClosed:
			cr.StartQualifier = &btpb.ColumnRange_StartQualifierClosed{
				StartQualifierClosed: sq,
			}
		case ColumnRangeOpen:
			cr.StartQualifier = &btpb.ColumnRange_StartQualifierOpen{
				StartQualifierOpen: sq,
			}
		}
	}
	if end != nil {
		switch sq := end.(type) {
		case ColumnRangeClosed:
			cr.EndQualifier = &btpb.ColumnRange_EndQualifierClosed{
				EndQualifierClosed: sq,
			}
		case ColumnRangeOpen:
			cr.EndQualifier = &btpb.ColumnRange_EndQualifierOpen{
				EndQualifierOpen: sq,
			}
		}
	}
	res.Filter = &btpb.RowFilter_ColumnRangeFilter{
		ColumnRangeFilter: cr,
	}
	return (*RowFilter)(res)
}

// TimestampRangeFilter -
func TimestampRangeFilter(start, end time.Time) *RowFilter {
	var res = new(btpb.RowFilter)
	tr := &btpb.TimestampRange{
		StartTimestampMicros: timeMicro(start),
		EndTimestampMicros:   timeMicro(end),
	}

	res.Filter = &btpb.RowFilter_TimestampRangeFilter{
		TimestampRangeFilter: tr,
	}
	return (*RowFilter)(res)
}

// CellsPerColumnLimitFilter -
func CellsPerColumnLimitFilter(limit int32) *RowFilter {
	var res = new(btpb.RowFilter)

	res.Filter = &btpb.RowFilter_CellsPerColumnLimitFilter{
		CellsPerColumnLimitFilter: limit,
	}
	return (*RowFilter)(res)
}

// ValueRangeFilter -
func ValueRangeFilter(start, end IsValueRangeType) *RowFilter {
	var res = new(btpb.RowFilter)

	cr := &btpb.ValueRange{}
	if start != nil {
		switch sq := start.(type) {
		case ValueRangeClosed:
			cr.StartValue = &btpb.ValueRange_StartValueClosed{
				StartValueClosed: sq,
			}
		case ValueRangeOpen:
			cr.StartValue = &btpb.ValueRange_StartValueOpen{
				StartValueOpen: sq,
			}
		}
	}
	if end != nil {
		switch sq := end.(type) {
		case ValueRangeClosed:
			cr.EndValue = &btpb.ValueRange_EndValueClosed{
				EndValueClosed: sq,
			}
		case ValueRangeOpen:
			cr.EndValue = &btpb.ValueRange_EndValueOpen{
				EndValueOpen: sq,
			}
		}
	}
	res.Filter = &btpb.RowFilter_ValueRangeFilter{
		ValueRangeFilter: cr,
	}
	return (*RowFilter)(res)
}

// ColumnFilter -
func ColumnFilter(family string, col []byte) *RowFilter {
	return ColumnRangeFilter(family, Column(col).RangeClosed(), Column(col).RangeClosed())
}

// ValueFilter -
func ValueFilter(val []byte) *RowFilter {
	return ValueRangeFilter(Value(val).RangeClosed(), Value(val).RangeClosed())
}

// Column -
type Column []byte

// RangeOpen -
func (c Column) RangeOpen() ColumnRangeOpen {
	return ColumnRangeOpen(c)
}

// RangeClosed -
func (c Column) RangeClosed() ColumnRangeClosed {
	return ColumnRangeClosed(c)
}

type isColumnRangeType interface {
	isColumnRangeType()
}

// ColumnRangeOpen -
type ColumnRangeOpen Column

func (ColumnRangeOpen) isColumnRangeType() {}

// ColumnRangeClosed -
type ColumnRangeClosed Column

func (ColumnRangeClosed) isColumnRangeType() {}

// Value -
type Value []byte

// RangeOpen -
func (c Value) RangeOpen() ValueRangeOpen {
	return ValueRangeOpen(c)
}

// RangeClosed -
func (c Value) RangeClosed() ValueRangeClosed {
	return ValueRangeClosed(c)
}

// IsValueRangeType -
type IsValueRangeType interface {
	IsValueRangeType()
}

// ValueRangeOpen -
type ValueRangeOpen Value

// IsValueRangeType -
func (ValueRangeOpen) IsValueRangeType() {}

// ValueRangeClosed -
type ValueRangeClosed Value

// IsValueRangeType -
func (ValueRangeClosed) IsValueRangeType() {}
