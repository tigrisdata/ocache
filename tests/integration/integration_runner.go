package integration

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
)

var logInitOnce sync.Once

// InitTestLogging initializes logging for coordinator tests
func InitTestLogging() {
	logInitOnce.Do(func() {
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		zlog.Logger = zlog.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	})
}

// IntegrationTestSuite is the base test suite for Integration tests
type IntegrationTestSuite struct {
	suite.Suite
	Harness TestHarnessInterface
	Config  IntegrationTestConfig
}

// SetupSuite runs once before all tests in the suite
func (s *IntegrationTestSuite) SetupSuite() {
	// Suite-level setup if needed
}

// TearDownSuite runs once after all tests in the suite
func (s *IntegrationTestSuite) TearDownSuite() {
	// Suite-level teardown if needed
}

// SetupTest runs before each test
func (s *IntegrationTestSuite) SetupTest() {
	// Per-test setup is handled by each specific suite
}

// TearDownTest runs after each test
func (s *IntegrationTestSuite) TearDownTest() {
	if s.Harness != nil {
		s.Harness.Cleanup()
		s.Harness.PrintMetrics()
	}
}

// ObjectsSuite tests all object sizes (small, medium, large)
type ObjectsSuite struct {
	IntegrationTestSuite
}

// SetupTest sets up for object tests
func (s *ObjectsSuite) SetupTest() {
	// Initialize logging for tests
	InitTestLogging()

	config := DefaultIntegrationTestConfig()
	config.InlineThreshold = 64 * 1024         // 64KB
	config.CompactThreshold = 16 * 1024 * 1024 // 16MB
	config.SegmentSize = 256 * 1024 * 1024     // 256MB
	config.CompactionInterval = 1 * time.Second
	s.Config = config
	s.Harness = NewIntegrationTestHarness(s.T(), config)
}

// CleanerSuite tests cleaner functionality (TTL and LRU)
type CleanerSuite struct {
	IntegrationTestSuite
}

// SetupTest sets up for cleaner tests
func (s *CleanerSuite) SetupTest() {
	// Initialize logging for tests
	InitTestLogging()

	config := DefaultIntegrationTestConfig()
	config.CleanupInterval = 200 * time.Millisecond // Fast cleanup for testing
	config.MaxDiskUsage = 50 * 1024                 // 50KB limit for LRU testing
	s.Config = config
	s.Harness = NewIntegrationTestHarness(s.T(), config)
}

// CompactionSuite tests compaction functionality
type CompactionSuite struct {
	IntegrationTestSuite
}

// SetupTest sets up for compaction tests
func (s *CompactionSuite) SetupTest() {
	// Initialize logging for tests
	InitTestLogging()

	config := DefaultIntegrationTestConfig()
	config.CompactionInterval = 500 * time.Millisecond // Fast compaction for testing
	config.SegmentSize = 2 * 1024 * 1024               // 2MB segments to create multiple segments during tests
	s.Config = config
	s.Harness = NewIntegrationTestHarness(s.T(), config)
}

// WorkflowSuite tests cross-component workflows
type WorkflowSuite struct {
	IntegrationTestSuite
}

// SetupTest sets up for workflow tests
func (s *WorkflowSuite) SetupTest() {
	// Initialize logging for tests
	InitTestLogging()

	// Each workflow test creates its own harness with custom configuration
	// so we don't create a harness here to avoid conflicts
	s.Config = DefaultIntegrationTestConfig()
	s.Harness = nil
}

// TearDownTest cleans up after each workflow test
func (s *WorkflowSuite) TearDownTest() {
	// Cleanup is handled by each test individually
}

// CoordinatorSuite tests cluster coordinator functionality
type CoordinatorSuite struct {
	IntegrationTestSuite
	harness *CoordinatorTestHarness
}

// SetupTest sets up for coordinator tests
func (s *CoordinatorSuite) SetupTest() {
	// Initialize logging for tests
	InitTestLogging()

	// Create a simple 3-node test harness
	s.harness = NewCoordinatorTestHarness(s.T(), 3)

	// Use default integration test config for base suite
	config := DefaultIntegrationTestConfig()
	s.Config = config
	// Don't set s.Harness since we're not using storage for coordinator tests
}

// TearDownTest cleans up after each coordinator test
func (s *CoordinatorSuite) TearDownTest() {
	if s.harness != nil {
		s.harness.Cleanup()
	}
}

// ClusterSuite tests object operations in cluster mode
type ClusterSuite struct {
	IntegrationTestSuite
}

// SetupTest sets up for cluster object tests
func (s *ClusterSuite) SetupTest() {
	// Initialize logging for tests
	InitTestLogging()

	config := DefaultIntegrationTestConfig()
	config.InlineThreshold = 64 * 1024         // 64KB
	config.CompactThreshold = 16 * 1024 * 1024 // 16MB
	config.SegmentSize = 256 * 1024 * 1024     // 256MB
	config.CompactionInterval = 1 * time.Second
	s.Config = config

	// Create cluster harness with 3 nodes
	clusterHarness := NewClusterTestHarness(s.T(), 3, config)
	err := clusterHarness.StartAllNodes()
	if err != nil {
		s.T().Fatalf("Failed to start cluster: %v", err)
	}
	s.Harness = clusterHarness
}

// TearDownTest cleans up after each cluster object test
func (s *ClusterSuite) TearDownTest() {
	if s.Harness != nil {
		s.Harness.PrintMetrics() // Print metrics while storage is still open
		s.Harness.Cleanup()      // Cleanup after metrics are collected
	}
}

// Test suite runners

// TestIntegrationObjects runs the consolidated objects test suite
func TestIntegrationObjects(t *testing.T) {
	suite.Run(t, new(ObjectsSuite))
}

// TestIntegrationCleaner runs the cleaner test suite
func TestIntegrationCleaner(t *testing.T) {
	suite.Run(t, new(CleanerSuite))
}

// TestIntegrationCompaction runs the compaction test suite
func TestIntegrationCompaction(t *testing.T) {
	suite.Run(t, new(CompactionSuite))
}

// TestIntegrationWorkflow runs the workflow test suite
func TestIntegrationWorkflow(t *testing.T) {
	suite.Run(t, new(WorkflowSuite))
}

// TestIntegrationCoordinator runs the coordinator test suite
func TestIntegrationCoordinator(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping coordinator tests in short mode")
	}
	suite.Run(t, new(CoordinatorSuite))
}

// TestIntegrationCluster runs the cluster object test suite
func TestIntegrationCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster tests in short mode")
	}
	suite.Run(t, new(ClusterSuite))
}

// Helper function to run all Integration tests
func RunAllIntegrationTests(t *testing.T) {
	t.Run("Objects", TestIntegrationObjects)
	t.Run("Cleaner", TestIntegrationCleaner)
	t.Run("Compaction", TestIntegrationCompaction)
	t.Run("Workflows", TestIntegrationWorkflow)
	if !testing.Short() {
		t.Run("Coordinator", TestIntegrationCoordinator)
		t.Run("Cluster", TestIntegrationCluster)
	}
}
