// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"fmt"
	"io"
	"sync"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var (
	chunkSlicePool = sync.Pool{
		New: func() interface{} { return &[]Chunk{} },
	}
	labelSlicePool = sync.Pool{
		New: func() interface{} {
			return &[]mimirpb.LabelAdapter{}
		},
	}
)

// IngesterQueryStreamClientWrappedReceiver extends the Ingester_QueryStreamClient interface
// adding a wrapped response receiver method.
// The purpose of using this method instead of the default Ingester_QueryStreamClient.Recv is to allow us to unmarshal
// stream responses using our custom wrapped type, so that we can reuse chunk and label slices.
type IngesterQueryStreamClientWrappedReceiver interface {
	Ingester_QueryStreamClient
	RecvWrapped(*WrappedQueryStreamResponse) error
}

func (x *ingesterQueryStreamClient) RecvWrapped(m *WrappedQueryStreamResponse) error {
	return x.ClientStream.RecvMsg(m)
}

// WrappedQueryStreamResponse is a wrapper around QueryStreamResponse that allows us to
// customize Unmarshal method.
type WrappedQueryStreamResponse struct {
	*QueryStreamResponse
}

func (m *WrappedQueryStreamResponse) Reset() {
	*m = WrappedQueryStreamResponse{&QueryStreamResponse{}}
}
func (m *WrappedQueryStreamResponse) XXX_Unmarshal(b []byte) error { return m.Unmarshal(b) } //nolint

// Unmarshal a WrappedQueryStreamResponse, implements proto.Unmarshaller.
// This is a copy of the autogenerated code to unmarshal a QueryStreamResponse
// using wrapped timeseries chunks.
func (m *WrappedQueryStreamResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowIngester
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: QueryStreamResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: QueryStreamResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Chunkseries", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			// << NON AUTO-GENERATED CODE >>
			//
			// m.Chunkseries = append(m.Chunkseries, TimeSeriesChunk{})
			// if err := m.Chunkseries[len(m.Chunkseries)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
			//	return err
			// }
			ts := WrappedTimeSeriesChunk{TimeSeriesChunk: &TimeSeriesChunk{}}
			if err := ts.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Chunkseries = append(m.Chunkseries, *ts.TimeSeriesChunk)
			//<< NON AUTO-GENERATED CODE >>
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Timeseries", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Timeseries = append(m.Timeseries, mimirpb.TimeSeries{})
			if err := m.Timeseries[len(m.Timeseries)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StreamingSeries", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.StreamingSeries = append(m.StreamingSeries, QueryStreamSeries{})
			if err := m.StreamingSeries[len(m.StreamingSeries)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field IsEndOfSeriesStream", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.IsEndOfSeriesStream = bool(v != 0)
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StreamingSeriesChunks", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.StreamingSeriesChunks = append(m.StreamingSeriesChunks, QueryStreamSeriesChunks{})
			if err := m.StreamingSeriesChunks[len(m.StreamingSeriesChunks)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipIngester(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthIngester
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthIngester
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// WrappedTimeSeriesChunk is a wrapper around TimeSeriesChunk that allows us to
// customize Unmarshal method.
type WrappedTimeSeriesChunk struct {
	*TimeSeriesChunk
}

func (m *WrappedTimeSeriesChunk) Reset() {
	*m = WrappedTimeSeriesChunk{&TimeSeriesChunk{}}
}
func (m *WrappedTimeSeriesChunk) XXX_Unmarshal(b []byte) error { return m.Unmarshal(b) } //nolint

// Unmarshal a WrappedTimeSeriesChunk, implements proto.Unmarshaller.
// This is a copy of the autogenerated code to unmarshal a TimeSeriesChunk,
// fetching chunk and label slices from a sync.Pool.
func (m *WrappedTimeSeriesChunk) Unmarshal(dAtA []byte) error {
	// << NON AUTO-GENERATED CODE >>
	m.Chunks = ChunkSliceFromPool()
	m.Labels = LabelSliceFromPool()
	m.Labels = m.Labels[:0]

	reusedChunks := 0
	poolChunksLength := len(m.Chunks)

	defer func() {
		// Readjust chunks slice length in case not all available slots have been reused.
		if reusedChunks < poolChunksLength {
			m.Chunks = m.Chunks[:reusedChunks]
		}
	}()
	// << NON AUTO-GENERATED CODE >>

	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowIngester
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TimeSeriesChunk: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TimeSeriesChunk: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field FromIngesterId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.FromIngesterId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field UserId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.UserId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Labels = append(m.Labels, mimirpb.LabelAdapter{})
			if err := m.Labels[len(m.Labels)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Chunks", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowIngester
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthIngester
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthIngester
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			// << NON AUTO-GENERATED CODE >>
			//
			//  m.Chunks = append(m.Chunks, Chunk{})
			//  if err := m.Chunks[len(m.Chunks)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
			//      return err
			//  }
			//
			if reusedChunks < poolChunksLength {
				if err := m.Chunks[reusedChunks].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
				reusedChunks++
			} else {
				m.Chunks = append(m.Chunks, Chunk{})
				if err := m.Chunks[len(m.Chunks)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
					return err
				}
			}
			// << NON AUTO-GENERATED CODE >>
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipIngester(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthIngester
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthIngester
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// ReuseQueryStreamResponse puts chunk and label slices contained in qr back into a sync.Pool for reuse.
func ReuseQueryStreamResponse(qr *QueryStreamResponse) {
	for i := 0; i < len(qr.Chunkseries); i++ {
		for j := 0; j < len(qr.Chunkseries[i].Labels); j++ {
			// Clear all label values to avoid retaining yolo string backed buffers.
			qr.Chunkseries[i].Labels[j] = mimirpb.LabelAdapter{}
		}
		for j := 0; j < len(qr.Chunkseries[i].Chunks); j++ {
			qr.Chunkseries[i].Chunks[j].Data = qr.Chunkseries[i].Chunks[j].Data[:0]
		}
		chunkSlicePool.Put(&qr.Chunkseries[i].Chunks)
		labelSlicePool.Put(&qr.Chunkseries[i].Labels)
	}
	qr.Chunkseries = nil
}

// LabelSliceFromPool returns a slice of LabelAdapter from a sync.Pool.
func LabelSliceFromPool() []mimirpb.LabelAdapter {
	return *(labelSlicePool.Get().(*[]mimirpb.LabelAdapter))
}

// ChunkSliceFromPool returns a slice of chunks from a sync.Pool.
func ChunkSliceFromPool() []Chunk {
	return *(chunkSlicePool.Get().(*[]Chunk))
}
