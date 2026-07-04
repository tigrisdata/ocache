// Package logsample provides a shared, rate-limited logger for high-frequency
// failure-path log lines.
//
// During a single-node loss the ring degrades and every request that maps to a
// down node hard-fails, so per-request failure logs ("Failed to route key",
// "Circuit breaker open for node", ...) can fan out to millions of identical
// WARN lines fleet-wide. Those lines are themselves a load amplifier: they burn
// CPU and disk on every node and worsen the storm they describe (issue #164).
//
// The exact failure counts are already tracked in Prometheus at each of these
// sites, so the log line is narration we can safely thin. DegradedRing() returns
// a sampled logger that lets a short burst through immediately (so an incident
// is still visible in logs the moment it starts) and then falls back to 1-in-N,
// bounding total volume without losing the metric-backed counts.
package logsample

import (
	"time"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

// degradedRingSampler bounds the volume of the per-request failure-path logs
// that fire during a ring outage. Burst lines per Period pass unconditionally;
// beyond that, one in N is emitted. The pointer is shared across all call sites
// so the bound is aggregate, not per-site.
var degradedRingSampler = &zerolog.BurstSampler{
	Burst:       5,
	Period:      10 * time.Second,
	NextSampler: &zerolog.BasicSampler{N: 1000},
}

// DegradedRing returns a Warn event on the shared rate-limited logger. Use it in
// place of zlog.Warn() for per-request failure-path logs that can fire millions
// of times during a single-node loss. The failure must also be counted by a
// metric at the call site, since sampling drops most of these lines.
//
// It reads the global zlog.Logger at call time so it picks up the process's
// configured output/level (set after package init in the server's logger setup).
func DegradedRing() *zerolog.Event {
	l := zlog.Logger.Sample(degradedRingSampler)
	return l.Warn()
}
