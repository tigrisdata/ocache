package integration

import (
	"flag"
	"os"
	"testing"
)

var (
	runAll    = flag.Bool("all", false, "Run all Integration tests including stress tests")
	runStress = flag.Bool("stress", false, "Run stress tests")
)

func TestMain(m *testing.M) {
	flag.Parse()

	// Set up any global test configuration here
	code := m.Run()

	// Clean up any global resources
	os.Exit(code)
}

// TestIntegration_SmallObjects runs the small object test suite
func TestIntegration_SmallObjects(t *testing.T) {
	t.Run("SmallObjectSuite", func(t *testing.T) {
		TestIntegrationSmallObjects(t)
	})
}

// TestIntegration_MediumObjects runs the medium object test suite
func TestIntegration_MediumObjects(t *testing.T) {
	t.Run("MediumObjectSuite", func(t *testing.T) {
		TestIntegrationMediumObjects(t)
	})
}

// TestIntegration_LargeObjects runs the large object test suite
func TestIntegration_LargeObjects(t *testing.T) {
	t.Run("LargeObjectSuite", func(t *testing.T) {
		TestIntegrationLargeObjects(t)
	})
}

// TestIntegration_Cleaner runs the cleaner test suite
func TestIntegration_Cleaner(t *testing.T) {
	t.Run("CleanerSuite", func(t *testing.T) {
		TestIntegrationCleaner(t)
	})
}

// TestIntegration_Compaction runs the compaction test suite
func TestIntegration_Compaction(t *testing.T) {
	t.Run("CompactionSuite", func(t *testing.T) {
		TestIntegrationCompaction(t)
	})
}

// TestIntegration_Workflow runs the workflow test suite
func TestIntegration_Workflow(t *testing.T) {
	t.Run("WorkflowSuite", func(t *testing.T) {
		TestIntegrationWorkflow(t)
	})
}

// TestIntegration_All runs all Integration test suites
func TestIntegration_All(t *testing.T) {
	if !*runAll {
		t.Skip("Skipping full Integration suite. Use -all flag to run all tests")
	}

	RunAllIntegrationTests(t)
}
