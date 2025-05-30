package main

import "os"

// pooledFileReader wraps *os.File and uses bufferPool for Read
type pooledFileReader struct {
	f *os.File
}

func (p *pooledFileReader) Read(dst []byte) (int, error) {
	return p.f.Read(dst)
}

func (p *pooledFileReader) Close() error {
	return p.f.Close()
}
