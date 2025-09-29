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

// TestIntegration_Objects runs the consolidated objects test suite
func TestIntegration_Objects(t *testing.T) {
	t.Run("ObjectsSuite", func(t *testing.T) {
		TestIntegrationObjects(t)
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

// TestIntegration_Coordinator runs the coordinator test suite
func TestIntegration_Coordinator(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping coordinator tests in short mode")
	}
	t.Run("CoordinatorSuite", func(t *testing.T) {
		TestIntegrationCoordinator(t)
	})
}

// TestIntegration_All runs all Integration test suites
func TestIntegration_All(t *testing.T) {
	if !*runAll {
		t.Skip("Skipping full Integration suite. Use -all flag to run all tests")
	}

	RunAllIntegrationTests(t)
}
