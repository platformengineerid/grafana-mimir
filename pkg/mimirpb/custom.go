// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"bytes"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
)

// MinTimestamp returns the minimum timestamp (milliseconds) among all series
// in the WriteRequest. Returns math.MaxInt64 if the request is empty.
func (m *WriteRequest) MinTimestamp() int64 {
	min := int64(math.MaxInt64)

	for _, series := range m.Timeseries {
		for _, entry := range series.Samples {
			if entry.TimestampMs < min {
				min = entry.TimestampMs
			}
		}

		for _, entry := range series.Histograms {
			if entry.Timestamp < min {
				min = entry.Timestamp
			}
		}

		for _, entry := range series.Exemplars {
			if entry.TimestampMs < min {
				min = entry.TimestampMs
			}
		}
	}

	return min
}

func (h Histogram) IsFloatHistogram() bool {
	_, ok := h.GetCount().(*Histogram_CountFloat)
	return ok
}

func (h Histogram) IsGauge() bool {
	return h.ResetHint == Histogram_GAUGE
}

// ReduceResolution will reduce the resolution of the histogram by one level.
// Returns the resulting bucket count and an error if the histogram is not
// possible to reduce further.
func (h *Histogram) ReduceResolution() (int, error) {
	if h.IsFloatHistogram() {
		return h.reduceFloatResolution()
	}

	return h.reduceIntResolution()
}

func (h *Histogram) reduceFloatResolution() (int, error) {
	if h.Schema == -4 {
		return 0, fmt.Errorf("cannot reduce resolution of histogram with schema %d", h.Schema)
	}
	h.PositiveSpans, h.PositiveCounts = reduceBucketResolution(h.PositiveSpans, h.PositiveCounts, h.Schema, h.Schema-1, false)
	h.NegativeSpans, h.NegativeCounts = reduceBucketResolution(h.NegativeSpans, h.NegativeCounts, h.Schema, h.Schema-1, false)
	h.Schema--
	return len(h.PositiveDeltas) + len(h.NegativeDeltas), nil
}

func (h *Histogram) reduceIntResolution() (int, error) {
	if h.Schema == -4 {
		return 0, fmt.Errorf("cannot reduce resolution of histogram with schema %d", h.Schema)
	}
	h.PositiveSpans, h.PositiveDeltas = reduceBucketResolution(h.PositiveSpans, h.PositiveDeltas, h.Schema, h.Schema-1, true)
	h.NegativeSpans, h.NegativeDeltas = reduceBucketResolution(h.NegativeSpans, h.NegativeDeltas, h.Schema, h.Schema-1, true)
	h.Schema--
	return len(h.PositiveDeltas) + len(h.NegativeDeltas), nil
}

func reduceBucketResolution[IBC histogram.InternalBucketCount](originSpans []BucketSpan, originBuckets []IBC, originSchema, targetSchema int32, deltaBuckets bool) ([]BucketSpan, []IBC) {
	var (
		targetSpans           []BucketSpan // The spans in the target schema.
		targetBuckets         []IBC        // The buckets in the target schema.
		bucketIdx             int32        // The index of bucket in the origin schema.
		lastBucketCount       IBC          // The last visited bucket's count in the origin schema.
		lastTargetBucketIdx   int32        // The index of the last added target bucket.
		lastTargetBucketCount IBC
		origBucketIdx         int // The position of a bucket in originBuckets slice.
	)

	for _, span := range originSpans {
		// Determine the index of the first bucket in this span.
		bucketIdx += span.Offset
		for j := 0; j < int(span.Length); j++ {
			// Determine the index of the bucket in the target schema from the index in the original schema.
			targetBucketIdx := targetIdx(bucketIdx, originSchema, targetSchema)

			switch {
			case len(targetSpans) == 0:
				// This is the first span in the targetSpans.
				span := BucketSpan{
					Offset: targetBucketIdx,
					Length: 1,
				}
				targetSpans = append(targetSpans, span)
				targetBuckets = append(targetBuckets, originBuckets[0])
				lastTargetBucketIdx = targetBucketIdx
				lastBucketCount = originBuckets[0]
				lastTargetBucketCount = originBuckets[0]

			case lastTargetBucketIdx == targetBucketIdx:
				// The current bucket has to be merged into the same target bucket as the previous bucket.
				if deltaBuckets {
					lastBucketCount += originBuckets[origBucketIdx]
					targetBuckets[len(targetBuckets)-1] += lastBucketCount
					lastTargetBucketCount += lastBucketCount
				} else {
					targetBuckets[len(targetBuckets)-1] += originBuckets[origBucketIdx]
				}

			case (lastTargetBucketIdx + 1) == targetBucketIdx:
				// The current bucket has to go into a new target bucket,
				// and that bucket is next to the previous target bucket,
				// so we add it to the current target span.
				targetSpans[len(targetSpans)-1].Length++
				lastTargetBucketIdx++
				if deltaBuckets {
					lastBucketCount += originBuckets[origBucketIdx]
					targetBuckets = append(targetBuckets, lastBucketCount-lastTargetBucketCount)
					lastTargetBucketCount = lastBucketCount
				} else {
					targetBuckets = append(targetBuckets, originBuckets[origBucketIdx])
				}

			case (lastTargetBucketIdx + 1) < targetBucketIdx:
				// The current bucket has to go into a new target bucket,
				// and that bucket is separated by a gap from the previous target bucket,
				// so we need to add a new target span.
				span := BucketSpan{
					Offset: targetBucketIdx - lastTargetBucketIdx - 1,
					Length: 1,
				}
				targetSpans = append(targetSpans, span)
				lastTargetBucketIdx = targetBucketIdx
				if deltaBuckets {
					lastBucketCount += originBuckets[origBucketIdx]
					targetBuckets = append(targetBuckets, lastBucketCount-lastTargetBucketCount)
					lastTargetBucketCount = lastBucketCount
				} else {
					targetBuckets = append(targetBuckets, originBuckets[origBucketIdx])
				}
			}

			bucketIdx++
			origBucketIdx++
		}
	}

	return targetSpans, targetBuckets
}

// targetIdx returns the bucket index in the target schema for the given bucket
// index idx in the original schema.
func targetIdx(idx, originSchema, targetSchema int32) int32 {
	return ((idx - 1) >> (originSchema - targetSchema)) + 1
}

// UnsafeByteSlice is an alternative to the default handling of []byte values in protobuf messages.
// Unlike the default protobuf implementation, when unmarshalling, UnsafeByteSlice holds a reference to the
// subslice of the original protobuf-encoded bytes, rather than copying them from the encoded buffer to a second slice.
// This reduces memory pressure when unmarshalling byte slices, at the cost of retaining the full buffer in memory. This
// tradeoff is usually only worthwhile when the protobuf message is dominated by []byte values (eg. when sending chunks
// to queriers).
//
// The implementations of all other methods are identical to those generated by the protobuf compiler.
//
// Note that UnsafeByteSlice will not behave correctly if the protobuf-encoded bytes are reused (eg. if the
// buffer is used for one request and then pooled and reused for a later request). At the time of writing, the gRPC
// client does not do this, but there is a TODO to investigate it (see
// https://github.com/grafana/mimir/blob/117f0d68785b8a3ca07a8b1dda5a350b17cdc09b/vendor/google.golang.org/grpc/rpc_util.go#L575-L576).
type UnsafeByteSlice []byte

func (t *UnsafeByteSlice) MarshalTo(data []byte) (n int, err error) {
	copy(data, *t)

	return t.Size(), nil
}

func (t *UnsafeByteSlice) Unmarshal(data []byte) error {
	*t = data

	return nil
}

func (t *UnsafeByteSlice) Size() int {
	return len(*t)
}

func (t UnsafeByteSlice) Equal(other UnsafeByteSlice) bool {
	return bytes.Equal(t, other)
}
