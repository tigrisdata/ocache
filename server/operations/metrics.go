// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package operations

import (
	"time"

	"github.com/tigrisdata/ocache/common/metrics"
)

// recordOperationStart records the start of an operation and returns a function
// to record the completion. This pattern ensures consistent metrics recording.
func recordOperationStart(operation string) func(err error, statusOverride ...string) {
	start := time.Now()
	return func(err error, statusOverride ...string) {
		duration := time.Since(start)
		metrics.RPCDuration.WithLabelValues(operation).Observe(float64(duration.Milliseconds()))

		status := "success"
		if err != nil {
			status = "error"
			metrics.Errors.WithLabelValues("operations", operation).Inc()
		}
		if len(statusOverride) > 0 {
			status = statusOverride[0]
		}
		metrics.RPCRequests.WithLabelValues(operation, status).Inc()
	}
}

// recordBytesTransferred records bytes transferred for streaming operations.
func recordBytesTransferred(direction string, bytes int64) {
	metrics.StreamBytesTransferred.WithLabelValues(direction).Add(float64(bytes))
}

// recordStreamError counts a cross-node Get that failed after streaming began,
// i.e. after Operations.Get already returned (found resolved) so the failure
// can't flow through its done(err). Keeps these failures visible in error
// metrics (issue #162).
func recordStreamError() {
	metrics.Errors.WithLabelValues("operations", "getRemote").Inc()
}

// recordStreamActive increments the active streams counter and returns a function
// to decrement it when the stream is done.
func recordStreamActive() func() {
	metrics.StreamsActive.Inc()
	return func() {
		metrics.StreamsActive.Dec()
	}
}
