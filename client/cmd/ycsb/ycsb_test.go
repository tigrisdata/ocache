// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package ycsb

import (
	"bytes"
	"context"
	"net"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pterm/pterm"
	cacheclient "github.com/tigrisdata/ocache/client"
	pb "github.com/tigrisdata/ocache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ycsbReadWorkers    = 4
	ycsbReadKeys       = 1
	ycsbReadOperations = 64

	// Observe several 500 ms reporter intervals after RunYCSBWithContext returns.
	reporterShutdownObservationWindow = 3 * 500 * time.Millisecond
)

type ycsbReadServer struct {
	pb.UnimplementedCacheServiceServer

	mu            sync.RWMutex
	values        map[string][]byte
	getCalls      atomic.Int64
	responseBytes atomic.Int64

	getErr     error
	blockReads bool
	getStarted chan struct{}
}

func newYCSBReadServer() *ycsbReadServer {
	return &ycsbReadServer{values: make(map[string][]byte)}
}

func (s *ycsbReadServer) PutObject(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	data := append([]byte(nil), req.Data...)

	s.mu.Lock()
	s.values[req.Key] = data
	s.mu.Unlock()

	return &pb.PutResponse{Success: true}, nil
}

func (s *ycsbReadServer) Get(req *pb.GetRequest, stream pb.CacheService_GetServer) error {
	if s.getStarted != nil {
		select {
		case s.getStarted <- struct{}{}:
		default:
		}
	}
	if s.getErr != nil {
		return s.getErr
	}
	if s.blockReads {
		<-stream.Context().Done()
		return stream.Context().Err()
	}

	s.mu.RLock()
	data, ok := s.values[req.Key]
	s.mu.RUnlock()
	if !ok {
		return status.Error(codes.NotFound, "key not found")
	}

	for len(data) > 0 {
		chunkSize := min(len(data), cacheclient.DefaultBufferSize)
		chunk := data[:chunkSize]
		if err := stream.Send(&pb.GetResponse{Data: chunk}); err != nil {
			return err
		}
		s.responseBytes.Add(int64(len(chunk)))
		data = data[chunkSize:]
	}
	s.getCalls.Add(1)

	return nil
}

func startYCSBReadServer(tb testing.TB) (*ycsbReadServer, string) {
	tb.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	cacheServer := newYCSBReadServer()
	pb.RegisterCacheServiceServer(grpcServer, cacheServer)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	tb.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	return cacheServer, listener.Addr().String()
}

func readOnlyYCSBConfig(addr string, valueSize int) YCSBConfig {
	return YCSBConfig{
		Addr:               addr,
		ConnMode:           string(cacheclient.ModeSimple),
		ConnectionPoolSize: ycsbReadWorkers,
		NumKeys:            ycsbReadKeys,
		ValueSize:          valueSize,
		NumOps:             ycsbReadOperations,
		Concurrency:        ycsbReadWorkers,
		Workload:           "C",
		Seed:               1,
		NoProgress:         true,
		ForceStreaming:     false,
	}
}

func reporterShutdownYCSBConfig(addr string) YCSBConfig {
	return YCSBConfig{
		Addr:               addr,
		ConnMode:           string(cacheclient.ModeSimple),
		ConnectionPoolSize: 2,
		NumKeys:            1,
		ValueSize:          1,
		NumOps:             5,
		Concurrency:        2,
		Workload:           "C",
		Seed:               1,
	}
}

type reporterShutdownScenario int

const (
	reporterShutdownIncompleteCompletion reporterShutdownScenario = iota
	reporterShutdownCancellation
	reporterShutdownConnectionError
)

func startReporterShutdownServer(tb testing.TB, scenario reporterShutdownScenario) (*ycsbReadServer, YCSBConfig) {
	tb.Helper()

	cacheServer, addr := startYCSBReadServer(tb)
	cfg := reporterShutdownYCSBConfig(addr)
	switch scenario {
	case reporterShutdownCancellation:
		cacheServer.blockReads = true
		cacheServer.getStarted = make(chan struct{}, cfg.Concurrency)
	case reporterShutdownConnectionError:
		cacheServer.getErr = status.Error(codes.Unavailable, "connection unavailable")
	}
	return cacheServer, cfg
}

func drainGetStarted(getStarted <-chan struct{}) {
	for {
		select {
		case <-getStarted:
		default:
			return
		}
	}
}

func disablePtermOutput(tb testing.TB) {
	tb.Helper()

	output := pterm.Output
	pterm.Output = false
	tb.Cleanup(func() {
		pterm.Output = output
	})
}

var reporterTerminalMarker = []byte("\x00ocache-ycsb-reporter-terminal-marker\x00")

// terminalOutputTrace captures the pterm area printer, which writes directly to os.Stdout.
type terminalOutputTrace struct {
	originalStdout *os.File
	originalOutput bool
	writer         *os.File
	written        atomic.Int64
	drainDone      chan struct{}
	markerSeen     chan struct{}
	markerOnce     sync.Once
}

func startTerminalOutputTrace(tb testing.TB) *terminalOutputTrace {
	tb.Helper()

	reader, writer, err := os.Pipe()
	if err != nil {
		tb.Fatal(err)
	}

	trace := &terminalOutputTrace{
		originalStdout: os.Stdout,
		originalOutput: pterm.Output,
		writer:         writer,
		drainDone:      make(chan struct{}),
		markerSeen:     make(chan struct{}),
	}
	os.Stdout = writer
	// AreaPrinter writes to os.Stdout even when regular pterm output is disabled.
	// Keep preload setup identical in both benchmark revisions: preloadKeys skips
	// its spinner while this output is disabled.
	pterm.Output = false

	go trace.drain(reader)
	return trace
}

func (trace *terminalOutputTrace) drain(reader *os.File) {
	defer close(trace.drainDone)
	defer reader.Close()

	buf := make([]byte, 32*1024)
	tail := make([]byte, 0, len(reporterTerminalMarker)-1)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			trace.written.Add(int64(n))
			data := append(tail, buf[:n]...)
			if bytes.Contains(data, reporterTerminalMarker) {
				trace.markerOnce.Do(func() {
					close(trace.markerSeen)
				})
			}
			if len(data) >= len(reporterTerminalMarker)-1 {
				tail = append(tail[:0], data[len(data)-len(reporterTerminalMarker)+1:]...)
			} else {
				tail = append(tail[:0], data...)
			}
		}
		if err != nil {
			return
		}
	}
}

func (trace *terminalOutputTrace) snapshot(tb testing.TB) int64 {
	tb.Helper()

	if _, err := trace.writer.Write(reporterTerminalMarker); err != nil {
		tb.Fatal(err)
	}
	select {
	case <-trace.markerSeen:
	case <-time.After(time.Second):
		tb.Fatal("timed out draining terminal output")
	}
	return trace.written.Load()
}

func (trace *terminalOutputTrace) close() {
	os.Stdout = trace.originalStdout
	pterm.Output = trace.originalOutput
	_ = trace.writer.Close()
	<-trace.drainDone
}

type reporterShutdownObservation struct {
	reporterGoroutines int
	terminalBytes      int64
}

func reporterMetricsGoroutines(tb testing.TB) int {
	tb.Helper()

	var profile bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&profile, 2); err != nil {
		tb.Fatal(err)
	}
	return strings.Count(profile.String(), "PtermProgressReporter).updateMetrics")
}

func observeReporterShutdown(tb testing.TB, cacheServer *ycsbReadServer, cfg YCSBConfig, scenario reporterShutdownScenario) reporterShutdownObservation {
	tb.Helper()

	trace := startTerminalOutputTrace(tb)
	defer trace.close()

	beforeGoroutines := reporterMetricsGoroutines(tb)
	var result Result
	var err error
	switch scenario {
	case reporterShutdownIncompleteCompletion:
		result, err = RunYCSBWithContext(context.Background(), cfg)
	case reporterShutdownCancellation:
		drainGetStarted(cacheServer.getStarted)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		resultCh := make(chan struct {
			result Result
			err    error
		}, 1)
		go func() {
			result, err := RunYCSBWithContext(ctx, cfg)
			resultCh <- struct {
				result Result
				err    error
			}{result, err}
		}()

		select {
		case <-cacheServer.getStarted:
		case <-time.After(time.Second):
			tb.Fatal("YCSB worker did not start a read before cancellation")
		}
		cancel()

		select {
		case run := <-resultCh:
			result, err = run.result, run.err
		case <-time.After(5 * time.Second):
			tb.Fatal("cancelled YCSB run did not return")
		}
	case reporterShutdownConnectionError:
		cacheServer.getErr = status.Error(codes.Unavailable, "connection unavailable")
		result, err = RunYCSBWithContext(context.Background(), cfg)
	default:
		tb.Fatalf("unknown reporter shutdown scenario: %d", scenario)
	}
	if err != nil {
		tb.Fatal(err)
	}
	if result.Ops != cfg.NumOps {
		tb.Fatalf("RunYCSBWithContext reported %d operations, want %d", result.Ops, cfg.NumOps)
	}

	bytesAtReturn := trace.snapshot(tb)
	time.Sleep(reporterShutdownObservationWindow)

	return reporterShutdownObservation{
		reporterGoroutines: reporterMetricsGoroutines(tb) - beforeGoroutines,
		terminalBytes:      trace.written.Load() - bytesAtReturn,
	}
}

func TestRunYCSBWithContextStopsReporter(t *testing.T) {
	for _, tc := range []struct {
		name     string
		scenario reporterShutdownScenario
	}{
		{name: "incomplete_completion", scenario: reporterShutdownIncompleteCompletion},
		{name: "cancellation", scenario: reporterShutdownCancellation},
		{name: "connection_error", scenario: reporterShutdownConnectionError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cacheServer, cfg := startReporterShutdownServer(t, tc.scenario)
			observation := observeReporterShutdown(t, cacheServer, cfg, tc.scenario)
			if observation.reporterGoroutines != 0 {
				t.Errorf("retained reporter goroutines = %d, want 0", observation.reporterGoroutines)
			}
			if observation.terminalBytes != 0 {
				t.Errorf("terminal bytes written after return = %d, want 0", observation.terminalBytes)
			}
		})
	}
}

func TestRunYCSBReadOnlyDrainsResponses(t *testing.T) {
	disablePtermOutput(t)
	cacheServer, addr := startYCSBReadServer(t)
	cfg := readOnlyYCSBConfig(addr, 2*cacheclient.DefaultBufferSize+1)

	result, err := RunYCSBWithContext(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Errors != 0 {
		t.Fatalf("RunYCSBWithContext reported %d errors", result.Errors)
	}

	if got, want := cacheServer.getCalls.Load(), int64(cfg.NumOps); got != want {
		t.Errorf("Get calls = %d, want %d", got, want)
	}
	if got, want := cacheServer.responseBytes.Load(), int64(cfg.NumOps*cfg.ValueSize); got != want {
		t.Errorf("response bytes = %d, want %d", got, want)
	}
}

func benchmarkReporterShutdown(b *testing.B, scenario reporterShutdownScenario) {
	cacheServer, cfg := startReporterShutdownServer(b, scenario)

	var reporterGoroutines int64
	var terminalBytes int64
	for b.Loop() {
		observation := observeReporterShutdown(b, cacheServer, cfg, scenario)
		reporterGoroutines += int64(observation.reporterGoroutines)
		terminalBytes += observation.terminalBytes
	}
	b.ReportMetric(float64(reporterGoroutines)/float64(b.N), "post-return-reporter-goroutines/op")
	b.ReportMetric(float64(terminalBytes)/float64(b.N), "post-return-terminal-bytes/op")
}

func BenchmarkRunYCSBWithContextIncompleteCompletionReporterShutdown(b *testing.B) {
	benchmarkReporterShutdown(b, reporterShutdownIncompleteCompletion)
}

func BenchmarkRunYCSBWithContextCancellationReporterShutdown(b *testing.B) {
	benchmarkReporterShutdown(b, reporterShutdownCancellation)
}

func BenchmarkRunYCSBWithContextConnectionErrorReporterShutdown(b *testing.B) {
	benchmarkReporterShutdown(b, reporterShutdownConnectionError)
}

func BenchmarkRunYCSBReadOnly(b *testing.B) {
	disablePtermOutput(b)

	for _, tc := range []struct {
		name      string
		valueSize int
	}{
		{name: "64KiB", valueSize: 64 * 1024},
		{name: "256KiB", valueSize: 256 * 1024},
		{name: "1MiB", valueSize: 1024 * 1024},
	} {
		b.Run(tc.name, func(b *testing.B) {
			_, addr := startYCSBReadServer(b)
			cfg := readOnlyYCSBConfig(addr, tc.valueSize)

			b.ReportAllocs()
			b.SetBytes(int64(cfg.NumOps * cfg.ValueSize))
			for b.Loop() {
				result, err := RunYCSBWithContext(context.Background(), cfg)
				if err != nil {
					b.Fatal(err)
				}
				if result.Errors != 0 {
					b.Fatalf("RunYCSBWithContext reported %d errors", result.Errors)
				}
			}
		})
	}
}
