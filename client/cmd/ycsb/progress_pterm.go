package ycsb

import (
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/pterm/pterm"
)

// PtermProgressReporter handles real-time progress reporting using pterm
type PtermProgressReporter struct {
	totalOps     int64
	completedOps atomic.Int64
	errorCount   atomic.Int64
	startTime    time.Time

	// Per-operation type counters
	opCounts [OpNum]atomic.Int64

	// Pterm components
	progressBar *pterm.ProgressbarPrinter
	liveText    *pterm.AreaPrinter

	// Metrics for display
	currentOpsPerSec atomic.Value // stores float64
	overallOpsPerSec atomic.Value // stores float64
	lastLatencies    atomic.Value // stores LatencySnapshot
}

type LatencySnapshot struct {
	P50  float64
	P95  float64
	P99  float64
	P999 float64
}

// NewPtermProgressReporter creates a new pterm-based progress reporter
func NewPtermProgressReporter(totalOps int) *PtermProgressReporter {
	pr := &PtermProgressReporter{
		totalOps:  int64(totalOps),
		startTime: time.Now(),
	}
	pr.currentOpsPerSec.Store(float64(0))
	pr.overallOpsPerSec.Store(float64(0))
	pr.lastLatencies.Store(LatencySnapshot{})
	return pr
}

// Start begins the progress reporting
func (pr *PtermProgressReporter) Start() error {
	// Create progress bar
	progressBar, err := pterm.DefaultProgressbar.
		WithTotal(int(pr.totalOps)).
		WithTitle("Benchmark Progress").
		WithShowCount(true).
		WithShowPercentage(true).
		WithShowElapsedTime(true).
		WithRemoveWhenDone(true).
		Start()
	if err != nil {
		return err
	}
	pr.progressBar = progressBar

	// Create live text area for additional metrics
	pr.liveText, _ = pterm.DefaultArea.Start()

	// Start metrics update goroutine
	go pr.updateMetrics()

	return nil
}

// updateMetrics runs in background to calculate and display metrics
func (pr *PtermProgressReporter) updateMetrics() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	lastOps := int64(0)
	lastTime := pr.startTime

	for range ticker.C {
		completed := pr.completedOps.Load()
		if completed >= pr.totalOps {
			break
		}

		// Calculate metrics
		elapsed := time.Since(pr.startTime)
		errors := pr.errorCount.Load()

		// Calculate ops/sec
		intervalOps := completed - lastOps
		intervalDuration := time.Since(lastTime).Seconds()
		if intervalDuration > 0 {
			pr.currentOpsPerSec.Store(float64(intervalOps) / intervalDuration)
		}
		pr.overallOpsPerSec.Store(float64(completed) / elapsed.Seconds())

		// Build metrics display
		var metrics string

		// Operations breakdown
		metrics += pterm.LightCyan("Operations: ")
		for i := range OpNum {
			count := pr.opCounts[i].Load()
			if count > 0 {
				metrics += fmt.Sprintf("%s:%d ", opNames[i], count)
			}
		}
		metrics += "\n"

		// Throughput
		currentOps := pr.currentOpsPerSec.Load().(float64)
		overallOps := pr.overallOpsPerSec.Load().(float64)
		metrics += fmt.Sprintf("%s Current: %.0f ops/s | Overall: %.0f ops/s\n",
			pterm.LightCyan("Throughput:"), currentOps, overallOps)

		// Error rate
		errorRate := float64(errors) / float64(max(completed, 1)) * 100
		metrics += fmt.Sprintf("%s %d (%.2f%%)\n",
			pterm.LightCyan("Errors:"), errors, errorRate)

		// Latencies if available
		if latSnapshot, ok := pr.lastLatencies.Load().(LatencySnapshot); ok && latSnapshot.P50 > 0 {
			metrics += fmt.Sprintf("%s P50=%.1fms P95=%.1fms P99=%.1fms\n",
				pterm.LightCyan("Latency:"), latSnapshot.P50, latSnapshot.P95, latSnapshot.P99)
		}

		// Update live display
		pr.liveText.Update(metrics)

		lastOps = completed
		lastTime = time.Now()
	}
}

// RecordOp records a completed operation
func (pr *PtermProgressReporter) RecordOp(opType OpType, latency time.Duration, err error) {
	pr.completedOps.Add(1)
	pr.opCounts[opType].Add(1)

	if err != nil {
		pr.errorCount.Add(1)
	}

	// Update progress bar
	if pr.progressBar != nil {
		pr.progressBar.Increment()
	}
}

// UpdateLatencies updates the latency snapshot
func (pr *PtermProgressReporter) UpdateLatencies(p50, p95, p99, p999 float64) {
	pr.lastLatencies.Store(LatencySnapshot{
		P50:  p50,
		P95:  p95,
		P99:  p99,
		P999: p999,
	})
}

// Stop stops the progress reporting
func (pr *PtermProgressReporter) Stop() {
	if pr.progressBar != nil {
		// This will remove the progress bar when done
		pr.progressBar.Stop()
	}
	if pr.liveText != nil {
		pr.liveText.Stop()
	}
}

// GetStats returns final statistics
func (pr *PtermProgressReporter) GetStats() (completed int64, errors int64, opCounts []int64) {
	completed = pr.completedOps.Load()
	errors = pr.errorCount.Load()
	opCounts = make([]int64, OpNum)
	for i := range OpNum {
		opCounts[i] = pr.opCounts[i].Load()
	}
	return
}

// DisplayFinalResults displays the final benchmark results using pterm tables
func DisplayFinalResults(cfg YCSBConfig, result Result, totalOps []int) {
	pterm.Println() // Add spacing

	// Title with style
	title := pterm.DefaultHeader.
		WithFullWidth().
		WithBackgroundStyle(pterm.NewStyle(pterm.BgGray)).
		WithMargin(1)
	title.Println("BENCHMARK RESULTS")

	// Create summary table
	summaryTable := pterm.TableData{
		{"Metric", "Value"},
		{"Total Operations", fmt.Sprintf("%d", cfg.NumOps)},
		{"Total Duration", result.Duration.Round(time.Millisecond).String()},
		{"Total Errors", fmt.Sprintf("%d", result.Errors)},
		{"Error Rate", fmt.Sprintf("%.2f%%", float64(result.Errors)/float64(cfg.NumOps)*100)},
		{"Throughput", fmt.Sprintf("%.2f ops/s", float64(cfg.NumOps)/result.Duration.Seconds())},
	}

	pterm.DefaultTable.
		WithHasHeader(true).
		WithHeaderStyle(pterm.NewStyle(pterm.FgLightCyan)).
		WithBoxed(true).
		WithData(summaryTable).
		Render()

	// Latency percentiles
	if len(result.Latencies) > 0 {
		pterm.DefaultSection.Println("Latency Percentiles (ms)")

		latencyTable := pterm.TableData{
			{"P50", "P95", "P99", "P99.9", "Max"},
			{
				fmt.Sprintf("%.2f", float64(percentile(result.Latencies, 0.50))/float64(time.Millisecond)),
				fmt.Sprintf("%.2f", float64(percentile(result.Latencies, 0.95))/float64(time.Millisecond)),
				fmt.Sprintf("%.2f", float64(percentile(result.Latencies, 0.99))/float64(time.Millisecond)),
				fmt.Sprintf("%.2f", float64(percentile(result.Latencies, 0.999))/float64(time.Millisecond)),
				fmt.Sprintf("%.2f", float64(percentile(result.Latencies, 1.00))/float64(time.Millisecond)),
			},
		}

		pterm.DefaultTable.
			WithHasHeader(true).
			WithHeaderStyle(pterm.NewStyle(pterm.FgLightCyan)).
			WithBoxed(true).
			WithData(latencyTable).
			Render()
	}

	// Operations breakdown
	if len(totalOps) > 0 {
		pterm.DefaultSection.Println("Operations Breakdown")

		opsTable := pterm.TableData{
			{"Operation", "Count", "Percentage", "Throughput"},
		}

		for i := range int(OpNum) {
			if i < len(totalOps) && totalOps[i] > 0 {
				count := totalOps[i]
				percent := float64(count) / float64(cfg.NumOps) * 100
				opsPerSec := float64(count) / result.Duration.Seconds()

				opsTable = append(opsTable, []string{
					opNames[i],
					fmt.Sprintf("%d", count),
					fmt.Sprintf("%.1f%%", percent),
					fmt.Sprintf("%.2f ops/s", opsPerSec),
				})
			}
		}

		pterm.DefaultTable.
			WithHasHeader(true).
			WithHeaderStyle(pterm.NewStyle(pterm.FgLightCyan)).
			WithBoxed(true).
			WithData(opsTable).
			Render()
	}

	pterm.Println() // Final spacing
}

// DisplayFinalResultsWithMetrics displays the final benchmark results with enhanced metrics
func DisplayFinalResultsWithMetrics(cfg YCSBConfig, result Result, totalOps []int, metrics *MetricsCollector) {
	pterm.Println() // Add spacing

	// Title with style
	title := pterm.DefaultHeader.
		WithFullWidth().
		WithBackgroundStyle(pterm.NewStyle(pterm.BgGray)).
		WithMargin(1)
	title.Println("BENCHMARK RESULTS")

	// Create summary table
	summaryTable := pterm.TableData{
		{"Metric", "Value"},
		{"Total Operations", fmt.Sprintf("%d", cfg.NumOps)},
		{"Total Duration", result.Duration.Round(time.Millisecond).String()},
		{"Total Errors", fmt.Sprintf("%d", result.Errors)},
		{"Error Rate", fmt.Sprintf("%.2f%%", float64(result.Errors)/float64(cfg.NumOps)*100)},
		{"Throughput", fmt.Sprintf("%.2f ops/s", float64(cfg.NumOps)/result.Duration.Seconds())},
	}

	pterm.DefaultTable.
		WithHasHeader(true).
		WithHeaderStyle(pterm.NewStyle(pterm.FgLightCyan)).
		WithBoxed(true).
		WithData(summaryTable).
		Render()

	// Per-Operation Statistics
	opStats := metrics.GetPerOperationStats()
	if len(opStats) > 0 {
		pterm.DefaultSection.Println("Per-Operation Latency Statistics")

		opsLatencyTable := pterm.TableData{
			{"Operation", "Count", "Errors", "Min", "P50", "P95", "P99", "P99.9", "Max", "Avg"},
		}

		for opType, stats := range opStats {
			if opType < OpNum {
				errorRate := "0.0%"
				if stats.Count > 0 {
					errorRate = fmt.Sprintf("%.1f%%", float64(stats.ErrorCount)/float64(stats.Count)*100)
				}
				opsLatencyTable = append(opsLatencyTable, []string{
					opNames[opType],
					fmt.Sprintf("%d", stats.Count),
					fmt.Sprintf("%d (%s)", stats.ErrorCount, errorRate),
					fmt.Sprintf("%.2fms", float64(stats.MinLatency)/float64(time.Millisecond)),
					fmt.Sprintf("%.2fms", float64(stats.P50Latency)/float64(time.Millisecond)),
					fmt.Sprintf("%.2fms", float64(stats.P95Latency)/float64(time.Millisecond)),
					fmt.Sprintf("%.2fms", float64(stats.P99Latency)/float64(time.Millisecond)),
					fmt.Sprintf("%.2fms", float64(stats.P999Latency)/float64(time.Millisecond)),
					fmt.Sprintf("%.2fms", float64(stats.MaxLatency)/float64(time.Millisecond)),
					fmt.Sprintf("%.2fms", float64(stats.AvgLatency)/float64(time.Millisecond)),
				})
			}
		}

		pterm.DefaultTable.
			WithHasHeader(true).
			WithHeaderStyle(pterm.NewStyle(pterm.FgLightCyan)).
			WithBoxed(true).
			WithData(opsLatencyTable).
			Render()
	}

	// Latency Histogram
	histogram := metrics.GetHistogram()
	if len(histogram) > 0 {
		pterm.DefaultSection.Println("Latency Distribution Histogram")

		// Find max count for scaling
		maxCount := 0
		totalCount := 0
		for _, b := range histogram {
			if b.Count > maxCount {
				maxCount = b.Count
			}
			totalCount += b.Count
		}

		if maxCount > 0 {
			histogramData := []pterm.Bar{}

			for _, bucket := range histogram {
				if bucket.Count == 0 {
					continue
				}

				var label string
				if bucket.Max == time.Duration(math.MaxInt64) {
					label = fmt.Sprintf("> %s", formatDuration(bucket.Min))
				} else {
					label = fmt.Sprintf("%s-%s", formatDuration(bucket.Min), formatDuration(bucket.Max))
				}

				percentage := float64(bucket.Count) / float64(totalCount) * 100
				histogramData = append(histogramData, pterm.Bar{
					Label: fmt.Sprintf("%-15s (%d, %.1f%%)", label, bucket.Count, percentage),
					Value: bucket.Count,
				})
			}

			if len(histogramData) > 0 {
				pterm.DefaultBarChart.
					WithBars(histogramData).
					WithShowValue().
					WithHorizontal().
					WithWidth(60).
					Render()
			}
		}
	}

	// Throughput Analysis
	throughputSeries := metrics.GetThroughputSeries()
	// Filter for aggregate throughput only (OpType == OpNum)
	aggregateSeries := []ThroughputPoint{}
	for _, point := range throughputSeries {
		if point.OpType == OpNum {
			aggregateSeries = append(aggregateSeries, point)
		}
	}

	if len(aggregateSeries) > 0 {
		summary := AnalyzeThroughput(aggregateSeries)

		pterm.DefaultSection.Println("Throughput Analysis")

		throughputTable := pterm.TableData{
			{"Metric", "Value"},
			{"Minimum", fmt.Sprintf("%.2f ops/s", summary.MinThroughput)},
			{"Maximum", fmt.Sprintf("%.2f ops/s", summary.MaxThroughput)},
			{"Average", fmt.Sprintf("%.2f ops/s", summary.AvgThroughput)},
			{"Std Deviation", fmt.Sprintf("%.2f ops/s", summary.StdDevThroughput)},
			{"Trend", summary.ThroughputTrend},
		}

		pterm.DefaultTable.
			WithHasHeader(true).
			WithHeaderStyle(pterm.NewStyle(pterm.FgLightCyan)).
			WithBoxed(true).
			WithData(throughputTable).
			Render()

		// Show a simple throughput trend chart
		if len(aggregateSeries) > 20 {
			// Sample points for visualization
			step := len(aggregateSeries) / 20
			chartData := []pterm.Bar{}

			for i := 0; i < len(aggregateSeries); i += step {
				point := aggregateSeries[i]
				timeLabel := fmt.Sprintf("%.1fs", point.Time.Seconds())
				chartData = append(chartData, pterm.Bar{
					Label: timeLabel,
					Value: int(point.OpsPerSec),
				})
			}

			pterm.Println("Throughput Over Time:")
			pterm.DefaultBarChart.
				WithBars(chartData).
				WithShowValue().
				WithHeight(10).
				Render()
		}
	}

	// Error Breakdown
	errorsByType, errorsByOp := metrics.GetErrorBreakdown()
	if len(errorsByType) > 0 || len(errorsByOp) > 0 {
		pterm.DefaultSection.Println("Error Analysis")

		if len(errorsByOp) > 0 {
			errorOpTable := pterm.TableData{
				{"Operation", "Error Count", "Percentage"},
			}

			totalErrors := 0
			for _, count := range errorsByOp {
				totalErrors += count
			}

			for opType, count := range errorsByOp {
				if opType < OpNum && count > 0 {
					percentage := float64(count) / float64(totalErrors) * 100
					errorOpTable = append(errorOpTable, []string{
						opNames[opType],
						fmt.Sprintf("%d", count),
						fmt.Sprintf("%.1f%%", percentage),
					})
				}
			}

			if len(errorOpTable) > 1 {
				pterm.Println("Errors by Operation:")
				pterm.DefaultTable.
					WithHasHeader(true).
					WithHeaderStyle(pterm.NewStyle(pterm.FgLightRed)).
					WithBoxed(true).
					WithData(errorOpTable).
					Render()
			}
		}

		if len(errorsByType) > 0 {
			errorTypeTable := pterm.TableData{
				{"Error Type", "Count", "Percentage"},
			}

			totalErrors := 0
			for _, count := range errorsByType {
				totalErrors += count
			}

			// Sort errors by count for better display
			type errorEntry struct {
				errType string
				count   int
			}
			errorList := []errorEntry{}
			for errType, count := range errorsByType {
				errorList = append(errorList, errorEntry{errType, count})
			}
			sort.Slice(errorList, func(i, j int) bool {
				return errorList[i].count > errorList[j].count
			})

			// Show top 10 error types
			maxErrors := 10
			if len(errorList) < maxErrors {
				maxErrors = len(errorList)
			}

			for i := 0; i < maxErrors; i++ {
				entry := errorList[i]
				percentage := float64(entry.count) / float64(totalErrors) * 100

				// Truncate long error messages
				errMsg := entry.errType
				if len(errMsg) > 50 {
					errMsg = errMsg[:47] + "..."
				}

				errorTypeTable = append(errorTypeTable, []string{
					errMsg,
					fmt.Sprintf("%d", entry.count),
					fmt.Sprintf("%.1f%%", percentage),
				})
			}

			if len(errorList) > maxErrors {
				otherCount := 0
				for i := maxErrors; i < len(errorList); i++ {
					otherCount += errorList[i].count
				}
				percentage := float64(otherCount) / float64(totalErrors) * 100
				errorTypeTable = append(errorTypeTable, []string{
					"Other errors",
					fmt.Sprintf("%d", otherCount),
					fmt.Sprintf("%.1f%%", percentage),
				})
			}

			pterm.Println("\nErrors by Type (Top 10):")
			pterm.DefaultTable.
				WithHasHeader(true).
				WithHeaderStyle(pterm.NewStyle(pterm.FgLightRed)).
				WithBoxed(true).
				WithData(errorTypeTable).
				Render()
		}
	}

	pterm.Println() // Final spacing
}
