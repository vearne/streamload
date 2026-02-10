package streamload

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/dsnet/compress/bzip2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// compressData compresses the data reader based on compression type
func (c *Client) compressData(data io.Reader, compression CompressionType) (io.Reader, error) {
	switch compression {
	case CompressionGZIP:
		return c.compressGZIP(data)
	case CompressionLZ4:
		return c.compressLZ4(data)
	case CompressionZSTD:
		return c.compressZSTD(data)
	case CompressionBZIP2:
		return c.compressBZIP2(data)
	default:
		return data, nil
	}
}

// compressGZIP compresses data using GZIP
func (c *Client) compressGZIP(data io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := io.Copy(writer, data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

// compressLZ4 compresses data using LZ4
func (c *Client) compressLZ4(data io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
	if _, err := io.Copy(writer, data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

// compressZSTD compresses data using ZSTD
func (c *Client) compressZSTD(data io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	encoder, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(encoder, data); err != nil {
		return nil, err
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

// compressBZIP2 compresses data using BZIP2
func (c *Client) compressBZIP2(data io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	writer, err := bzip2.NewWriter(&buf, &bzip2.WriterConfig{})
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(writer, data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}
