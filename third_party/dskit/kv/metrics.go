package kv

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/instrument"
	"github.com/grafana/dskit/kv/memberlist"
)

// RegistererWithKVName wraps the provided Registerer with the KV name label. If a nil reg
// is provided, a nil registry is returned
func RegistererWithKVName(reg prometheus.Registerer, name string) prometheus.Registerer {
	if reg == nil {
		return nil
	}

	return prometheus.WrapRegistererWith(prometheus.Labels{"kv_name": name}, reg)
}

// getCasErrorCode converts the provided CAS error into the code that should be used to track the operation
// in metrics.
func getCasErrorCode(err error) string {
	if err == nil {
		return "200"
	}
	if resp, ok := httpgrpc.HTTPResponseFromError(err); ok {
		return strconv.Itoa(int(resp.GetCode()))
	}

	// If the error has been returned to abort the CAS operation, then we shouldn't
	// consider it an error when tracking metrics.
	if casErr, ok := err.(interface{ IsOperationAborted() bool }); ok && casErr.IsOperationAborted() {
		return "200"
	}

	return "500"
}

type metrics struct {
	c               Client
	requestDuration *instrument.HistogramCollector
}

func newMetricsClient(backend string, c Client, reg prometheus.Registerer) Client {
	return &metrics{
		c: c,
		requestDuration: instrument.NewHistogramCollector(
			promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
				Name:    "kv_request_duration_seconds",
				Help:    "Time spent on kv store requests.",
				Buckets: prometheus.DefBuckets,
				ConstLabels: prometheus.Labels{
					"type": backend,
				},
			}, []string{"operation", "status_code"}),
		),
	}
}

func (m metrics) List(ctx context.Context, prefix string) ([]string, error) {
	var result []string
	err := instrument.CollectedRequest(ctx, "List", m.requestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		result, err = m.c.List(ctx, prefix)
		return err
	})
	return result, err
}

func (m metrics) Get(ctx context.Context, key string) (interface{}, error) {
	var result interface{}
	err := instrument.CollectedRequest(ctx, "GET", m.requestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		result, err = m.c.Get(ctx, key)
		return err
	})
	return result, err
}

// GetWithVersion forwards the optional memberlist sequence used for delta
// recovery. Other backends return their ordinary snapshot with no sequence.
func (m metrics) GetWithVersion(ctx context.Context, key string) (interface{}, uint64, error) {
	if versioned, ok := m.c.(interface {
		GetWithVersion(context.Context, string) (interface{}, uint64, error)
	}); ok {
		return versioned.GetWithVersion(ctx, key)
	}
	value, err := m.Get(ctx, key)
	return value, 0, err
}

func (m metrics) Delete(ctx context.Context, key string) error {
	err := instrument.CollectedRequest(ctx, "Delete", m.requestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		return m.c.Delete(ctx, key)
	})
	return err
}

func (m metrics) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	return instrument.CollectedRequest(ctx, "CAS", m.requestDuration, getCasErrorCode, func(ctx context.Context) error {
		return m.c.CAS(ctx, key, f)
	})
}

func (m metrics) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	_ = instrument.CollectedRequest(ctx, "WatchKey", m.requestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		m.c.WatchKey(ctx, key, f)
		return nil
	})
}

// WatchKeyWithChanges forwards the optional memberlist delta stream through
// the metrics wrapper. Backends without that extension retain full-snapshot
// WatchKey semantics.
func (m metrics) WatchKeyWithChanges(ctx context.Context, key string, f func(memberlist.WatchKeyChange) bool) {
	if watcher, ok := m.c.(interface {
		WatchKeyWithChanges(context.Context, string, func(memberlist.WatchKeyChange) bool)
	}); ok {
		watcher.WatchKeyWithChanges(ctx, key, f)
		return
	}

	m.WatchKey(ctx, key, func(value interface{}) bool {
		var mergeable memberlist.Mergeable
		if value != nil {
			var ok bool
			mergeable, ok = value.(memberlist.Mergeable)
			if !ok {
				return true
			}
		}
		return f(memberlist.WatchKeyChange{Value: mergeable, FullSnapshot: true})
	})
}

func (m metrics) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	_ = instrument.CollectedRequest(ctx, "WatchPrefix", m.requestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		m.c.WatchPrefix(ctx, prefix, f)
		return nil
	})
}
