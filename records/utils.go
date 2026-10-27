package records

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"io"
	"os"
)

func CompressRawJSON(jsonBytes []byte) (string, error) {
	var b bytes.Buffer

	// 1. Set up the Base64 encoder
	b64Encoder := base64.NewEncoder(base64.StdEncoding, &b)

	// 2. Set up the Zlib compressor
	zlibWriter := zlib.NewWriter(b64Encoder)

	// 3. Write the raw JSON bytes straight into the compression stream
	_, err := zlibWriter.Write(jsonBytes)
	if err != nil {
		return "", err
	}

	// 4. Flush and close streams in reverse order
	if err := zlibWriter.Close(); err != nil {
		return "", err
	}
	if err := b64Encoder.Close(); err != nil {
		return "", err
	}

	return b.String(), nil
}

func DecompressToRawJSON(compressedBytes []byte) ([]byte, error) {
	// 1. Decode from Base64
	b64Reader := base64.NewDecoder(base64.StdEncoding, bytes.NewBuffer(compressedBytes))

	// 2. Set up Zlib reader
	zlibReader, err := zlib.NewReader(b64Reader)
	if err != nil {
		return nil, err
	}
	defer zlibReader.Close()

	// 3. Read everything out into a byte slice
	return io.ReadAll(zlibReader)
}

func readFromFile(filename string) (error, []byte) {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return err, nil
	}
	data, err := DecompressToRawJSON(bytes)
	if err != nil {
		return err, nil
	}
	return nil, data
}

func appendToFile(filename string, content string) error {
	jsonBytes := []byte(content)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		return err
	}

	defer file.Close()
	compressed, err := CompressRawJSON(jsonBytes)
	if err != nil {
		return err
	}
	_, err = file.WriteString(compressed)
	if err != nil {
		return err
	}
	return nil
}
