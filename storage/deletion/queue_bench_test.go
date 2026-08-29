// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package deletion

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	grocksdb "github.com/linxGnu/grocksdb"
	"github.com/rs/zerolog"
	"github.com/tigrisdata/ocache/storage/keys"
	"github.com/tigrisdata/ocache/storage/metadata"
)

type pruneBenchmarkCase struct {
	name  string
	total int
	aged  int
}

var pruneBenchmarkCases = []pruneBenchmarkCase{
	{name: "depth=100/aged=1", total: 100, aged: 1},
	{name: "depth=100/aged=50", total: 100, aged: 50},
	{name: "depth=1000/aged=10", total: 1000, aged: 10},
	{name: "depth=1000/aged=500", total: 1000, aged: 500},
	{name: "depth=10000/aged=100", total: 10000, aged: 100},
	{name: "depth=10000/aged=9000", total: 10000, aged: 9000},
}

// BenchmarkQueuePruneOldEntries measures one scheduled pruning pass over a
// stable queue. Aged entries point at an existing file so the benchmark keeps
// the older prefix in place and measures the iterator and Stat work on every
// pass. The newer tail is never touched by the fixture.
func BenchmarkQueuePruneOldEntries(b *testing.B) {
	originalLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(originalLevel) })

	for _, tc := range pruneBenchmarkCases {
		b.Run(tc.name, func(b *testing.B) {
			queue, _ := newPruneBenchmarkQueue(b, tc)
			queue.pruneTrigger = make(chan struct{})
			queue.pruneComplete = make(chan struct{}, 1)
			queue.Start()
			runPrune := func() {
				queue.pruneTrigger <- struct{}{}
				<-queue.pruneComplete
			}
			runPrune() // Migrate any legacy rows before timing steady-state pruning.
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				runPrune()
			}
		})
	}
}

// BenchmarkQueuePruneOldEntriesIteratorRows records the number of RocksDB rows
// returned by the same prefix scan with and without the exclusive cutoff bound.
// The paired prune benchmark above measures the full parser, Stat, and batch
// path; this focused benchmark makes the iterator work reduction visible.
func BenchmarkQueuePruneOldEntriesIteratorRows(b *testing.B) {
	originalLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(originalLevel) })

	for _, tc := range pruneBenchmarkCases {
		for _, bounded := range []struct {
			name string
			set  bool
		}{
			{name: "unbounded", set: false},
			{name: "cutoff-bound", set: true},
		} {
			b.Run(tc.name+"/"+bounded.name, func(b *testing.B) {
				queue, cutoff := newPruneBenchmarkQueue(b, tc)
				iterations := 0
				rows := 0
				b.ResetTimer()
				for b.Loop() {
					rows += countDeletionQueueRows(queue.meta, cutoff, bounded.set)
					iterations++
				}
				b.StopTimer()
				b.ReportMetric(float64(rows)/float64(iterations), "iterator-rows/op")
			})
		}
	}
}

func newPruneBenchmarkQueue(b *testing.B, tc pruneBenchmarkCase) (*Queue, int64) {
	b.Helper()

	meta, err := metadata.NewMetaDB(b.TempDir(), 0, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	queue := NewQueue(meta, Config{
		BatchSize:       tc.total,
		ProcessInterval: time.Hour,
		PruneAge:        time.Hour,
	})
	b.Cleanup(func() {
		queue.Stop()
		meta.Close()
	})

	retainedPath := filepath.Join(b.TempDir(), "retained")
	if err := os.WriteFile(retainedPath, []byte("retained"), 0o644); err != nil {
		b.Fatal(err)
	}

	cutoff := time.Now().Add(-time.Hour).UnixNano()
	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	for i := 0; i < tc.total; i++ {
		var timestamp int64
		var filepath string
		if i < tc.aged {
			timestamp = cutoff - int64(tc.aged-i)
			filepath = retainedPath
		} else {
			timestamp = cutoff + int64(time.Minute) + int64(i-tc.aged)
			filepath = fmt.Sprintf("/future/%06d", i)
		}
		key := keys.MakeDeletionQueueKey(timestamp, filepath)
		if err := meta.Handle().Put(wo, key, []byte{0x01}); err != nil {
			b.Fatal(err)
		}
	}
	return queue, cutoff
}

func countDeletionQueueRows(meta *metadata.MetaDB, cutoff int64, bounded bool) int {
	ro := metadata.CreateReadOptions(true, false)
	defer ro.Destroy()
	if bounded {
		ro.SetIterateUpperBound(keys.MakeDeletionQueueKey(cutoff, ""))
	}

	it := meta.Handle().NewIterator(ro)
	defer it.Close()
	prefix := []byte(keys.DeletionQueuePrefix)
	rows := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Key()
		value := it.Value()
		key.Free()
		value.Free()
		rows++
	}
	if err := it.Err(); err != nil {
		panic(err)
	}
	return rows
}
