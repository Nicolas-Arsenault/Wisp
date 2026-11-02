package shards

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/klauspost/compress/s2"
)

// A frame is one record in the .log (big-endian):
//
//	[4 B body length][8 B event id][8 B timestamp ms][8 B producer id]
//	[8 B sequence][4 B CRC-32C][body]
//
// body is the S2-compressed payload. The CRC covers everything but itself.
// A producer id of 0 means the record has no sequence.
const (
	frameHeaderSize = 40
	frameCRCOffset  = 36
	maxFrameBody    = 64 << 20 // guards against a corrupt length
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record is one stored event.
type Record struct {
	ID        uint64
	Timestamp int64 // Unix ms, set when stored
	Payload   []byte
}

// frame is one record as stored in the .log.
type frame struct {
	id        uint64
	timestamp int64
	producer  uint64
	sequence  uint64
	body      []byte
}

// ValidatePayload checks that payload is a JSON object, without decoding it.
func ValidatePayload(payload []byte) error {
	trimmed := bytes.TrimLeft(payload, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(payload) {
		return ErrInvalidPayload
	}
	return nil
}

func compress(payload []byte) []byte {
	return s2.Encode(nil, payload)
}

func decompress(body []byte) ([]byte, error) {
	payload, err := s2.Decode(nil, body)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorruptRecord, err)
	}
	return payload, nil
}

// appendFrame appends f to dst.
func appendFrame(dst []byte, f frame) []byte {
	start := len(dst)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(f.body)))
	dst = binary.BigEndian.AppendUint64(dst, f.id)
	dst = binary.BigEndian.AppendUint64(dst, uint64(f.timestamp))
	dst = binary.BigEndian.AppendUint64(dst, f.producer)
	dst = binary.BigEndian.AppendUint64(dst, f.sequence)
	dst = append(dst, 0, 0, 0, 0) // CRC, set below
	dst = append(dst, f.body...)

	stored := dst[start:]
	binary.BigEndian.PutUint32(stored[frameCRCOffset:], frameCRC(stored[:frameCRCOffset], f.body))
	return dst
}

func frameCRC(header, body []byte) uint32 {
	return crc32.Update(crc32.Checksum(header, crcTable), crcTable, body)
}

func frameSize(bodyLen int) int64 {
	return frameHeaderSize + int64(bodyLen)
}

// readFrame reads the next frame from r. It returns io.EOF at the end, and
// ErrCorruptRecord for an incomplete or damaged frame. Other errors are I/O
// errors, not bad data.
func readFrame(r *bufio.Reader) (frame, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			err = fmt.Errorf("%w: incomplete header", ErrCorruptRecord)
		}
		return frame{}, err
	}
	bodyLen := binary.BigEndian.Uint32(header[0:4])
	if bodyLen > maxFrameBody {
		return frame{}, fmt.Errorf("%w: body too large", ErrCorruptRecord)
	}
	f := frame{
		id:        binary.BigEndian.Uint64(header[4:12]),
		timestamp: int64(binary.BigEndian.Uint64(header[12:20])),
		producer:  binary.BigEndian.Uint64(header[20:28]),
		sequence:  binary.BigEndian.Uint64(header[28:36]),
		body:      make([]byte, bodyLen),
	}
	if _, err := io.ReadFull(r, f.body); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			err = fmt.Errorf("%w: incomplete body", ErrCorruptRecord)
		}
		return frame{}, err
	}
	if frameCRC(header[:frameCRCOffset], f.body) != binary.BigEndian.Uint32(header[frameCRCOffset:]) {
		return frame{}, fmt.Errorf("%w: bad CRC for event %d", ErrCorruptRecord, f.id)
	}
	return f, nil
}
