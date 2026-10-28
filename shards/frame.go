package shards

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
)

// A frame is one record in the .log (big-endian):
//
//	[4 B body length][8 B event id][8 B timestamp ms][4 B CRC-32C][body]
//
// body is the zlib-compressed payload. The CRC covers everything but itself.
const (
	frameHeaderSize = 24
	frameCRCOffset  = 20
	maxFrameBody    = 64 << 20 // guards against a corrupt length
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record is one stored event.
type Record struct {
	ID        uint64
	Timestamp int64 // Unix ms, set when stored
	Payload   []byte
}

// ValidatePayload checks that payload is a JSON object, without decoding it.
func ValidatePayload(payload []byte) error {
	trimmed := bytes.TrimLeft(payload, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(payload) {
		return ErrInvalidPayload
	}
	return nil
}

func compress(payload []byte) ([]byte, error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	if _, err := w.Write(payload); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func decompress(body []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorruptRecord, err)
	}
	defer r.Close()
	payload, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorruptRecord, err)
	}
	return payload, nil
}

// appendFrame appends a frame to dst.
func appendFrame(dst []byte, id uint64, timestamp int64, body []byte) []byte {
	start := len(dst)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(body)))
	dst = binary.BigEndian.AppendUint64(dst, id)
	dst = binary.BigEndian.AppendUint64(dst, uint64(timestamp))
	dst = append(dst, 0, 0, 0, 0) // CRC, set below
	dst = append(dst, body...)

	frame := dst[start:]
	binary.BigEndian.PutUint32(frame[frameCRCOffset:], frameCRC(frame[:frameCRCOffset], body))
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
func readFrame(r *bufio.Reader) (id uint64, timestamp int64, body []byte, err error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			err = fmt.Errorf("%w: incomplete header", ErrCorruptRecord)
		}
		return 0, 0, nil, err
	}
	bodyLen := binary.BigEndian.Uint32(header[0:4])
	if bodyLen > maxFrameBody {
		return 0, 0, nil, fmt.Errorf("%w: body too large", ErrCorruptRecord)
	}
	id = binary.BigEndian.Uint64(header[4:12])
	timestamp = int64(binary.BigEndian.Uint64(header[12:20]))

	body = make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			err = fmt.Errorf("%w: incomplete body", ErrCorruptRecord)
		}
		return 0, 0, nil, err
	}
	if frameCRC(header[:frameCRCOffset], body) != binary.BigEndian.Uint32(header[frameCRCOffset:]) {
		return 0, 0, nil, fmt.Errorf("%w: bad CRC for event %d", ErrCorruptRecord, id)
	}
	return id, timestamp, body, nil
}
