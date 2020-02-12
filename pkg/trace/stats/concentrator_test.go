// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.

package stats

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/datadog-agent/pkg/trace/pb"
	"github.com/DataDog/datadog-agent/pkg/trace/traceutil"

	"github.com/stretchr/testify/assert"
)

var (
	testBucketInterval = time.Duration(2 * time.Second).Nanoseconds()
	measuredSpanMeta   = map[string]string{"_dd.measured": "1"}
)

func NewTestConcentrator() *Concentrator {
	statsChan := make(chan []Bucket)
	return NewConcentrator([]string{}, time.Second.Nanoseconds(), statsChan)
}

// getTsInBucket gives a timestamp in ns which is `offset` buckets late
func getTsInBucket(alignedNow int64, bsize int64, offset int64) int64 {
	return alignedNow - offset*bsize + rand.Int63n(bsize)
}

// testSpan avoids typo and inconsistency in test spans (typical pitfall: duration, start time,
// and end time are aligned, and end time is the one that needs to be aligned
func testSpan(spanID uint64, parentID uint64, duration, offset int64, name, service, resource string, err int32, meta map[string]string) *pb.Span {
	now := time.Now().UnixNano()
	alignedNow := now - now%testBucketInterval

	return &pb.Span{
		SpanID:   spanID,
		ParentID: parentID,
		Duration: duration,
		Start:    getTsInBucket(alignedNow, testBucketInterval, offset) - duration,
		Service:  service,
		Name:     name,
		Resource: resource,
		Error:    err,
		Type:     "db",
		Meta:     meta,
	}
}

// countValsEq is a test utility function to assert expected == actual for count aggregations.
func countValsEq(t *testing.T, expected map[string]float64, actual map[string]Count) {
	assert := assert.New(t)
	assert.Equal(len(expected), len(actual))
	for key, val := range expected {
		count, ok := actual[key]
		assert.True(ok, "Missing expected key from actual counts: %s", key)
		assert.Equal(val, count.Value)
	}
}

func TestCountValsEq(t *testing.T) {
	expected := map[string]float64{
		"query|duration|env:staging,service:myservice,resource:resource1": 450.0,
		"query|hits|env:staging,service:myservice,resource:resource1":     1.0,
		"query|errors|env:staging,service:myservice,resource:resource1":   0.0,
	}
	ts := TagSet{
		Tag{
			Name:  "env",
			Value: "staging",
		},
		Tag{
			Name:  "service",
			Value: "myservice",
		},
		Tag{
			Name:  "resource",
			Value: "resource1",
		},
	}
	actual := map[string]Count{
		"query|duration|env:staging,service:myservice,resource:myresource": Count{
			Key:      "query|hits|env:staging,service:myservice,resource:myresource",
			Name:     "query",
			Measure:  "hits",
			TagSet:   ts,
			TopLevel: 1.0,
			Value:    450.0,
		},
		"query|hits|env:staging,service:myservice,resource:myresource": Count{
			Key:      "query|hits|env:staging,service:myservice,resource:myresource",
			Name:     "query",
			Measure:  "hits",
			TagSet:   ts,
			TopLevel: 1.0,
			Value:    1.0,
		},
		"query|errors|env:staging,service:myservice,resource:myresource": Count{
			Key:      "query|hits|env:staging,service:myservice,resource:myresource",
			Name:     "query",
			Measure:  "hits",
			TagSet:   ts,
			TopLevel: 1.0,
			Value:    1.0,
		},
	}
	countValsEq(t, expected, actual)
}

// TestConcentratorOldestTs tests that the Agent doesn't report time buckets from a
// time before its start
func TestConcentratorOldestTs(t *testing.T) {
	assert := assert.New(t)
	statsChan := make(chan []Bucket)

	now := time.Now().UnixNano()

	// Build that simply have spans spread over time windows.
	trace := pb.Trace{
		testSpan(1, 0, 50, 5, "query", "A1", "resource1", 0, nil),
		testSpan(1, 0, 40, 4, "query", "A1", "resource1", 0, nil),
		testSpan(1, 0, 30, 3, "query", "A1", "resource1", 0, nil),
		testSpan(1, 0, 20, 2, "query", "A1", "resource1", 0, nil),
		testSpan(1, 0, 10, 1, "query", "A1", "resource1", 0, nil),
		testSpan(1, 0, 1, 0, "query", "A1", "resource1", 0, nil),
		testSpan(2, 0, 500, 0, "custom_query_op", "A1", "resource1", 0, measuredSpanMeta),
		// these two measured spans are not top-level, but they should still get counts
		testSpan(3, 2, 1000, 0, "nested_op", "A1", "resource1", 0, measuredSpanMeta),
		testSpan(4, 2, 1500, 0, "nested_op", "A1", "resource1", 1, measuredSpanMeta), // error in span
	}

	traceutil.ComputeTopLevel(trace)
	wt := NewWeightedTrace(trace, traceutil.GetRoot(trace))

	testTrace := &Input{
		Env:   "none",
		Trace: wt,
	}

	t.Run("cold", func(t *testing.T) {
		// Running cold, all spans in the past should end up in the current time bucket.
		flushTime := now
		c := NewConcentrator([]string{}, testBucketInterval, statsChan)
		c.addNow(testTrace, time.Now().UnixNano())

		for i := 0; i < c.bufferLen; i++ {
			stats := c.flushNow(flushTime)
			if !assert.Equal(0, len(stats), "We should get exactly 0 Bucket") {
				t.FailNow()
			}
			flushTime += testBucketInterval
		}

		stats := c.flushNow(flushTime)

		if !assert.Equal(1, len(stats), "We should get exactly 1 Bucket") {
			t.FailNow()
		}

		// First oldest bucket aggregates old past time buckets, so each count
		// should be an aggregated total across the spans.
		expected := map[string]float64{
			"query|duration|env:none,resource:resource1,service:A1":           151,
			"query|hits|env:none,resource:resource1,service:A1":               6,
			"query|errors|env:none,resource:resource1,service:A1":             0,
			"custom_query_op|duration|env:none,resource:resource1,service:A1": 500,
			"custom_query_op|hits|env:none,resource:resource1,service:A1":     1,
			"custom_query_op|errors|env:none,resource:resource1,service:A1":   0,
			"nested_op|duration|env:none,resource:resource1,service:A1":       2500,
			"nested_op|hits|env:none,resource:resource1,service:A1":           2,
			"nested_op|errors|env:none,resource:resource1,service:A1":         1,
		}
		countValsEq(t, expected, stats[0].Counts)
	})

	t.Run("hot", func(t *testing.T) {
		flushTime := now
		c := NewConcentrator([]string{}, testBucketInterval, statsChan)
		c.oldestTs = alignTs(now, c.bsize) - int64(c.bufferLen-1)*c.bsize
		c.addNow(testTrace, time.Now().UnixNano())

		for i := 0; i < c.bufferLen-1; i++ {
			stats := c.flushNow(flushTime)
			if !assert.Equal(0, len(stats), "We should get exactly 0 Bucket") {
				t.FailNow()
			}
			flushTime += testBucketInterval
		}

		stats := c.flushNow(flushTime)
		if !assert.Equal(1, len(stats), "We should get exactly 1 Bucket") {
			t.FailNow()
		}
		flushTime += testBucketInterval

		// First oldest bucket aggregates, it should have it all except the
		// last four spans that have offset of 0.
		expected := map[string]float64{
			"query|duration|env:none,resource:resource1,service:A1": 150,
			"query|hits|env:none,resource:resource1,service:A1":     5,
			"query|errors|env:none,resource:resource1,service:A1":   0,
		}
		countValsEq(t, expected, stats[0].Counts)

		stats = c.flushNow(flushTime)
		if !assert.Equal(1, len(stats), "We should get exactly 1 Bucket") {
			t.FailNow()
		}

		// Stats of the last four spans.
		expected = map[string]float64{
			"query|duration|env:none,resource:resource1,service:A1":           1,
			"query|hits|env:none,resource:resource1,service:A1":               1,
			"query|errors|env:none,resource:resource1,service:A1":             0,
			"custom_query_op|duration|env:none,resource:resource1,service:A1": 500,
			"custom_query_op|hits|env:none,resource:resource1,service:A1":     1,
			"custom_query_op|errors|env:none,resource:resource1,service:A1":   0,
			"nested_op|duration|env:none,resource:resource1,service:A1":       2500,
			"nested_op|hits|env:none,resource:resource1,service:A1":           2,
			"nested_op|errors|env:none,resource:resource1,service:A1":         1,
		}
		countValsEq(t, expected, stats[0].Counts)
	})
}

//TestConcentratorStatsTotals tests that the total stats are correct, independently of the
// time bucket they end up.
func TestConcentratorStatsTotals(t *testing.T) {
	assert := assert.New(t)
	statsChan := make(chan []Bucket)
	c := NewConcentrator([]string{}, testBucketInterval, statsChan)

	now := time.Now().UnixNano()
	alignedNow := alignTs(now, c.bsize)

	// update oldestTs as it running for quite some time, to avoid the fact that at startup
	// it only allows recent stats.
	c.oldestTs = alignedNow - int64(c.bufferLen)*c.bsize

	// Build that simply have spans spread over time windows.
	trace := pb.Trace{
		testSpan(1, 0, 50, 5, "query", "A1", "resource1", 0, nil),
		testSpan(1, 0, 40, 4, "query", "A1", "resource1", 0, nil),
		testSpan(1, 0, 30, 3, "query", "A1", "resource1", 0, nil),
		testSpan(1, 0, 20, 2, "query", "A1", "resource1", 0, nil),
		testSpan(1, 0, 10, 1, "query", "A1", "resource1", 0, nil),
		testSpan(1, 0, 1, 0, "query", "A1", "resource1", 0, nil),
		testSpan(2, 0, 10, 0, "custom_query_op", "A1", "resource1", 0, measuredSpanMeta),
		// these two measured spans are not top-level, but they should still get counts
		testSpan(3, 2, 10, 0, "nested_op", "A1", "resource1", 0, measuredSpanMeta),
		testSpan(4, 2, 100, 0, "nested_op", "A1", "resource1", 1, measuredSpanMeta), // error in span
	}

	traceutil.ComputeTopLevel(trace)
	wt := NewWeightedTrace(trace, traceutil.GetRoot(trace))

	testTrace := &Input{
		Env:   "none",
		Trace: wt,
	}
	c.addNow(testTrace, time.Now().UnixNano())

	var duration float64
	var hits float64
	var errors float64

	flushTime := now
	for i := 0; i <= c.bufferLen; i++ {
		stats := c.flushNow(flushTime)

		if len(stats) == 0 {
			continue
		}

		for key, count := range stats[0].Counts {
			if strings.Contains(key, "|duration|") {
				duration += count.Value
			}
			if strings.Contains(key, "|hits|") {
				hits += count.Value
			}
			if strings.Contains(key, "|errors|") {
				errors += count.Value
			}
		}
		flushTime += c.bsize
	}

	assert.Equal(duration, float64(50+40+30+20+10+1+10+10+100), "Wrong value for total duration %d", duration)
	assert.Equal(hits, float64(len(trace)), "Wrong value for total hits %d", hits)
	assert.Equal(errors, float64(1), "Wrong value for total errors %d", errors)
}

// TestConcentratorStatsCounts tests exhaustively each stats bucket, over multiple time buckets.
func TestConcentratorStatsCounts(t *testing.T) {
	assert := assert.New(t)
	statsChan := make(chan []Bucket)
	c := NewConcentrator([]string{}, testBucketInterval, statsChan)

	now := time.Now().UnixNano()
	alignedNow := alignTs(now, c.bsize)

	// update oldestTs as it running for quite some time, to avoid the fact that at startup
	// it only allows recent stats.
	c.oldestTs = alignedNow - int64(c.bufferLen)*c.bsize

	// Build a trace with stats which should cover 3 time buckets.
	trace := pb.Trace{
		// more than 2 buckets old, should be added to the 2 bucket-old, first flush.
		testSpan(1, 0, 111, 10, "query", "A1", "resource1", 0, nil),
		testSpan(1, 0, 222, 3, "query", "A1", "resource1", 0, nil),
		testSpan(30, 0, 150, 12, "custom_query_op", "A1", "resource1", 0, measuredSpanMeta),
		// 2 buckets old, part of the first flush
		testSpan(1, 0, 24, 2, "query", "A1", "resource1", 0, nil),
		testSpan(2, 0, 12, 2, "query", "A1", "resource1", 2, nil),
		testSpan(3, 0, 40, 2, "query", "A2", "resource2", 2, nil),
		testSpan(4, 0, 300000000000, 2, "query", "A2", "resource2", 2, nil), // 5 minutes trace
		testSpan(5, 0, 30, 2, "query", "A2", "resourcefoo", 0, nil),
		// 1 bucket old, part of the second flush
		testSpan(6, 0, 24, 1, "query", "A1", "resource2", 0, nil),
		testSpan(7, 0, 12, 1, "query", "A1", "resource1", 2, nil),
		testSpan(8, 0, 40, 1, "query", "A2", "resource1", 2, nil),
		testSpan(9, 0, 30, 1, "query", "A2", "resource2", 2, nil),
		testSpan(10, 0, 3600000000000, 1, "query", "A2", "resourcefoo", 0, nil), // 1 hour trace
		// present data, part of the third flush
		testSpan(6, 0, 24, 0, "query", "A1", "resource2", 0, nil),
		testSpan(20, 0, 10, 0, "custom_query_op", "A1", "resource2", 0, measuredSpanMeta),
		testSpan(21, 20, 500, 0, "nested_op", "A1", "resource2", 1, measuredSpanMeta), // error in span
	}

	expectedCountValByKeyByTime := make(map[int64]map[string]float64)
	// 2-bucket old flush
	expectedCountValByKeyByTime[alignedNow-2*testBucketInterval] = map[string]float64{
		"query|duration|env:none,resource:resource1,service:A1":           369,
		"query|duration|env:none,resource:resource2,service:A2":           300000000040,
		"query|duration|env:none,resource:resourcefoo,service:A2":         30,
		"custom_query_op|duration|env:none,resource:resource1,service:A1": 150,
		"query|hits|env:none,resource:resource1,service:A1":               4,
		"query|hits|env:none,resource:resource2,service:A2":               2,
		"query|hits|env:none,resource:resourcefoo,service:A2":             1,
		"custom_query_op|hits|env:none,resource:resource1,service:A1":     1,
		"query|errors|env:none,resource:resource1,service:A1":             1,
		"query|errors|env:none,resource:resource2,service:A2":             2,
		"query|errors|env:none,resource:resourcefoo,service:A2":           0,
		"custom_query_op|errors|env:none,resource:resource1,service:A1":   0,
	}
	// 1-bucket old flush
	expectedCountValByKeyByTime[alignedNow-1*testBucketInterval] = map[string]float64{
		"query|duration|env:none,resource:resource1,service:A1":   12,
		"query|duration|env:none,resource:resource2,service:A1":   24,
		"query|duration|env:none,resource:resource1,service:A2":   40,
		"query|duration|env:none,resource:resource2,service:A2":   30,
		"query|duration|env:none,resource:resourcefoo,service:A2": 3600000000000,
		"query|hits|env:none,resource:resource1,service:A1":       1,
		"query|hits|env:none,resource:resource2,service:A1":       1,
		"query|hits|env:none,resource:resource1,service:A2":       1,
		"query|hits|env:none,resource:resource2,service:A2":       1,
		"query|hits|env:none,resource:resourcefoo,service:A2":     1,
		"query|errors|env:none,resource:resource1,service:A1":     1,
		"query|errors|env:none,resource:resource2,service:A1":     0,
		"query|errors|env:none,resource:resource1,service:A2":     1,
		"query|errors|env:none,resource:resource2,service:A2":     1,
		"query|errors|env:none,resource:resourcefoo,service:A2":   0,
	}
	// last bucket to be flushed
	expectedCountValByKeyByTime[alignedNow] = map[string]float64{
		"query|duration|env:none,resource:resource2,service:A1":           24,
		"query|hits|env:none,resource:resource2,service:A1":               1,
		"query|errors|env:none,resource:resource2,service:A1":             0,
		"custom_query_op|duration|env:none,resource:resource2,service:A1": 10,
		"custom_query_op|hits|env:none,resource:resource2,service:A1":     1,
		"custom_query_op|errors|env:none,resource:resource2,service:A1":   0,
		"nested_op|duration|env:none,resource:resource2,service:A1":       500,
		"nested_op|hits|env:none,resource:resource2,service:A1":           1,
		"nested_op|errors|env:none,resource:resource2,service:A1":         1,
	}
	expectedCountValByKeyByTime[alignedNow+testBucketInterval] = map[string]float64{}

	traceutil.ComputeTopLevel(trace)
	wt := NewWeightedTrace(trace, traceutil.GetRoot(trace))

	testTrace := &Input{
		Env:   "none",
		Trace: wt,
	}
	c.addNow(testTrace, time.Now().UnixNano())

	// flush every testBucketInterval
	flushTime := now
	for i := 0; i <= c.bufferLen+2; i++ {
		t.Run(fmt.Sprintf("flush-%d", i), func(t *testing.T) {
			stats := c.flushNow(flushTime)

			expectedFlushedTs := alignTs(flushTime, c.bsize) - int64(c.bufferLen)*testBucketInterval
			if len(expectedCountValByKeyByTime[expectedFlushedTs]) == 0 {
				// That's a flush for which we expect no data
				return
			}

			if !assert.Equal(1, len(stats), "We should get exactly 1 Bucket") {
				t.FailNow()
			}

			receivedBuckets := []Bucket{stats[0]}

			assert.Equal(expectedFlushedTs, receivedBuckets[0].Start)

			expectedCountValByKey := expectedCountValByKeyByTime[expectedFlushedTs]
			receivedCounts := receivedBuckets[0].Counts

			countValsEq(t, expectedCountValByKey, receivedCounts)

			// Flushing again at the same time should return nothing
			stats = c.flushNow(flushTime)

			if !assert.Equal(0, len(stats), "Second flush of the same time should be empty") {
				t.FailNow()
			}

		})
		flushTime += c.bsize
	}
}

// TestConcentratorSublayersStatsCounts tests exhaustively the sublayer stats of a single time window.
func TestConcentratorSublayersStatsCounts(t *testing.T) {
	assert := assert.New(t)
	statsChan := make(chan []Bucket)
	c := NewConcentrator([]string{}, testBucketInterval, statsChan)

	now := time.Now().UnixNano()
	alignedNow := now - now%c.bsize

	trace := pb.Trace{
		// first bucket
		// most of these are top-level spans, except otherwise noted
		testSpan(1, 0, 2000, 0, "query", "A1", "resource1", 0, nil),
		testSpan(2, 1, 1000, 0, "query", "A2", "resource2", 0, nil),
		// measured and top-level, but count and sublayer calculation should stay the same
		testSpan(3, 1, 1000, 0, "query", "A2", "resource3", 0, measuredSpanMeta),
		testSpan(4, 2, 40, 0, "query", "A3", "resource4", 0, nil),
		// measured, not top-level - should get counts and will add to the fourth span's sublayer metrics
		testSpan(5, 4, 300, 0, "query", "A3", "resource5", 0, measuredSpanMeta),
		testSpan(7, 5, 150, 0, "query", "A3", "resource7", 0, nil),
		testSpan(6, 2, 30, 0, "query", "A3", "resource6", 0, nil),
		// measured, not top-level - should get counts and will add to the first span's sublayer metrics
		testSpan(10, 1, 200, 2020, "nested_op", "A1", "resource1", 0, measuredSpanMeta),
	}
	traceutil.ComputeTopLevel(trace)
	wt := NewWeightedTrace(trace, traceutil.GetRoot(trace))

	subtraces := ExtractTopLevelSubtraces(trace, traceutil.GetRoot(trace))
	sublayers := make(map[*pb.Span][]SublayerValue)
	for _, subtrace := range subtraces {
		subtraceSublayers := ComputeSublayers(subtrace.Trace)
		sublayers[subtrace.Root] = subtraceSublayers
	}

	testTrace := &Input{
		Env:       "none",
		Trace:     wt,
		Sublayers: sublayers,
	}

	c.addNow(testTrace, time.Now().UnixNano())
	stats := c.flushNow(alignedNow + int64(c.bufferLen)*c.bsize)

	if !assert.Equal(1, len(stats), "We should get exactly 1 Bucket") {
		t.FailNow()
	}

	assert.Equal(alignedNow, stats[0].Start)

	var receivedCounts map[string]Count

	// Start with the first/older bucket
	receivedCounts = stats[0].Counts
	expectedCountValByKey := map[string]float64{
		"query|_sublayers.duration.by_service|env:none,resource:resource1,service:A1,sublayer_service:A1": 2200,
		"query|_sublayers.duration.by_service|env:none,resource:resource1,service:A1,sublayer_service:A2": 2000,
		"query|_sublayers.duration.by_service|env:none,resource:resource1,service:A1,sublayer_service:A3": 520,
		"query|_sublayers.duration.by_service|env:none,resource:resource4,service:A3,sublayer_service:A3": 490,
		"query|_sublayers.duration.by_service|env:none,resource:resource2,service:A2,sublayer_service:A2": 1000,
		"query|_sublayers.duration.by_service|env:none,resource:resource2,service:A2,sublayer_service:A3": 520,
		"query|_sublayers.duration.by_type|env:none,resource:resource1,service:A1,sublayer_type:db":       4720,
		"query|_sublayers.duration.by_type|env:none,resource:resource2,service:A2,sublayer_type:db":       1520,
		"query|_sublayers.duration.by_type|env:none,resource:resource4,service:A3,sublayer_type:db":       490,
		"query|_sublayers.span_count|env:none,resource:resource1,service:A1,:":                            8,
		"query|_sublayers.span_count|env:none,resource:resource2,service:A2,:":                            5,
		"query|_sublayers.span_count|env:none,resource:resource4,service:A3,:":                            3,
		"query|duration|env:none,resource:resource1,service:A1":                                           2000,
		"query|duration|env:none,resource:resource2,service:A2":                                           1000,
		"query|duration|env:none,resource:resource3,service:A2":                                           1000,
		"query|duration|env:none,resource:resource4,service:A3":                                           40,
		"query|duration|env:none,resource:resource5,service:A3":                                           300,
		"query|duration|env:none,resource:resource6,service:A3":                                           30,
		"nested_op|duration|env:none,resource:resource1,service:A1":                                       200,
		"query|hits|env:none,resource:resource1,service:A1":                                               1,
		"query|hits|env:none,resource:resource2,service:A2":                                               1,
		"query|hits|env:none,resource:resource3,service:A2":                                               1,
		"query|hits|env:none,resource:resource4,service:A3":                                               1,
		"query|hits|env:none,resource:resource5,service:A3":                                               1,
		"query|hits|env:none,resource:resource6,service:A3":                                               1,
		"nested_op|hits|env:none,resource:resource1,service:A1":                                           1,
		"query|errors|env:none,resource:resource1,service:A1":                                             0,
		"query|errors|env:none,resource:resource2,service:A2":                                             0,
		"query|errors|env:none,resource:resource3,service:A2":                                             0,
		"query|errors|env:none,resource:resource4,service:A3":                                             0,
		"query|errors|env:none,resource:resource5,service:A3":                                             0,
		"query|errors|env:none,resource:resource6,service:A3":                                             0,
		"nested_op|errors|env:none,resource:resource1,service:A1":                                         0,
	}
	countValsEq(t, expectedCountValByKey, receivedCounts)
}

func TestConcentratorAddNow(t *testing.T) {
	now := time.Now().UnixNano()

	t.Run("only top-level root span", func(t *testing.T) {
		statsChan := make(chan []Bucket)
		trace := pb.Trace{
			testSpan(1, 0, 50, 5, "query", "A1", "resource1", 0, nil),
			testSpan(2, 1, 40, 4, "query", "A1", "resource1", 1, nil),
		}
		traceutil.ComputeTopLevel(trace)
		wt := NewWeightedTrace(trace, traceutil.GetRoot(trace))
		testTrace := &Input{
			Env:   "none",
			Trace: wt,
		}
		c := NewConcentrator([]string{}, testBucketInterval, statsChan)
		c.addNow(testTrace, time.Now().UnixNano())
		expected := map[string]float64{
			"query|duration|env:none,resource:resource1,service:A1": 50,
			"query|hits|env:none,resource:resource1,service:A1":     1,
			"query|errors|env:none,resource:resource1,service:A1":   0,
		}
		// skip ahead to first possible flush
		stats := c.flushNow(now + (int64(c.bufferLen) * testBucketInterval))
		countValsEq(t, expected, stats[0].Counts)
	})

	t.Run("top-level root span also marked as measured", func(t *testing.T) {
		statsChan := make(chan []Bucket)
		trace := pb.Trace{
			testSpan(1, 0, 50, 5, "query", "A1", "resource1", 0, measuredSpanMeta),
			testSpan(2, 1, 40, 4, "query", "A1", "resource1", 1, nil),
		}
		traceutil.ComputeTopLevel(trace)
		wt := NewWeightedTrace(trace, traceutil.GetRoot(trace))
		testTrace := &Input{
			Env:   "none",
			Trace: wt,
		}
		c := NewConcentrator([]string{}, testBucketInterval, statsChan)
		c.addNow(testTrace, time.Now().UnixNano())
		expected := map[string]float64{
			"query|duration|env:none,resource:resource1,service:A1": 50,
			"query|hits|env:none,resource:resource1,service:A1":     1,
			"query|errors|env:none,resource:resource1,service:A1":   0,
		}
		// skip ahead to first possible flush
		stats := c.flushNow(now + (int64(c.bufferLen) * testBucketInterval))
		countValsEq(t, expected, stats[0].Counts)
	})

	t.Run("top-level span, measured span, unmarked span", func(t *testing.T) {
		statsChan := make(chan []Bucket)
		trace := pb.Trace{
			testSpan(1, 0, 50, 5, "query", "A1", "resource1", 0, nil),
			testSpan(2, 1, 40, 4, "custom_query_op", "A1", "resource1", 1, measuredSpanMeta),
			testSpan(3, 2, 40, 4, "nested_op", "A1", "resource1", 1, nil),
		}
		traceutil.ComputeTopLevel(trace)
		wt := NewWeightedTrace(trace, traceutil.GetRoot(trace))
		testTrace := &Input{
			Env:   "none",
			Trace: wt,
		}
		c := NewConcentrator([]string{}, testBucketInterval, statsChan)
		c.addNow(testTrace, time.Now().UnixNano())
		expected := map[string]float64{
			"query|duration|env:none,resource:resource1,service:A1":           50,
			"query|hits|env:none,resource:resource1,service:A1":               1,
			"query|errors|env:none,resource:resource1,service:A1":             0,
			"custom_query_op|duration|env:none,resource:resource1,service:A1": 40,
			"custom_query_op|hits|env:none,resource:resource1,service:A1":     1,
			"custom_query_op|errors|env:none,resource:resource1,service:A1":   1,
		}
		// skip ahead to first possible flush
		stats := c.flushNow(now + (int64(c.bufferLen) * testBucketInterval))
		countValsEq(t, expected, stats[0].Counts)
	})
}
