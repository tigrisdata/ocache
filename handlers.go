package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"

	zlog "github.com/rs/zerolog/log"
	stor "github.com/tigrisdata/cache_service/storage"
)

// handlePut streams the body into spillWriter and stores metadata in RocksDB
func handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	ttl := 0
	if t := r.URL.Query().Get("ttl"); t != "" {
		if v, _ := strconv.Atoi(t); v > 0 {
			ttl = v
		}
	}

	if err := stor.GetStorage().Put(key, r.Body, ttl); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte("OK"))
}

// handleGet retrieves and streams small or large object with byte-range support
func handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	start, end := int64(0), int64(-1)
	if s := r.URL.Query().Get("start"); s != "" {
		if v, _ := strconv.ParseInt(s, 10, 64); v >= 0 {
			start = v
		}
	}
	if e := r.URL.Query().Get("end"); e != "" {
		if v, _ := strconv.ParseInt(e, 10, 64); v >= 0 {
			end = v
		}
	}

	zlog.Debug().Str("key", key).Int64("start", start).Int64("end", end).Msg("handleGet: start")

	reader, exists, err := stor.GetStorage().Get(key)
	if err != nil {
		zlog.Error().Err(err).Str("key", key).Msg("handleGet: storage.Get error")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !exists {
		zlog.Debug().Str("key", key).Msg("handleGet: not found")
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	zlog.Debug().Str("key", key).Msg("handleGet: found, streaming response")

	w.Header().Set("Content-Type", "application/octet-stream")
	if start > 0 {
		if seeker, ok := reader.(io.Seeker); ok {
			_, err := seeker.Seek(start, io.SeekStart)
			if err != nil {
				zlog.Error().Err(err).Str("key", key).Msg("handleGet: invalid range on seek")
				http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
				return
			}
		}
	}

	buf := stor.GetBuffer()
	defer stor.PutBuffer(buf[:0])

	if end >= 0 && end > start {
		toRead := end - start
		for toRead > 0 {
			n := int64(len(buf))
			if n > toRead {
				n = toRead
			}
			readN, err := reader.Read(buf[:n])
			if readN > 0 {
				w.Write(buf[:readN])
				toRead -= int64(readN)
			}
			if err != nil {
				zlog.Debug().Err(err).Str("key", key).Msg("handleGet: read loop break")
				break
			}
		}
		return
	}

	// If no end specified, stream the whole object
	if _, err := io.CopyBuffer(w, reader, buf); err != nil {
		if err != io.EOF {
			zlog.Error().Err(err).Str("key", key).Msg("handleGet: io.Copy error")
		}
	}
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	stor.GetStorage().DeleteKey(key)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func handleList(w http.ResponseWriter, r *http.Request) {
	keys, err := stor.GetStorage().ListKeys()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(keys); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func readSmallObject(w http.ResponseWriter, data []byte, start, end int64) {
	total := int64(len(data))
	if end < 0 || end > total {
		end = total
	}
	if start < 0 || start >= end {
		http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	const chunkSize = 64 * 1024
	for offset := start; offset < end; offset += chunkSize {
		n := int(chunkSize)
		if offset+int64(n) > end {
			n = int(end - offset)
		}
		w.Write(data[offset : offset+int64(n)])
	}
}

func readLargeObject(w http.ResponseWriter, path string, start, end int64) {
	f, err := os.Open(path)
	if err != nil {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	defer f.Close()
	fi, _ := f.Stat()
	total := fi.Size()
	if end < 0 || end > total {
		end = total
	}
	if start < 0 || start >= end {
		http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	const chunkSize = 64 * 1024
	f.Seek(start, io.SeekStart)
	toRead := end - start
	buf := make([]byte, chunkSize)
	for toRead > 0 {
		n, err := f.Read(buf)
		if n > 0 {
			if int64(n) > toRead {
				n = int(toRead)
			}
			w.Write(buf[:n])
			toRead -= int64(n)
		}
		if err != nil {
			break
		}
	}
}
