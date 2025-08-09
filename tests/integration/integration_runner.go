package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// IntegrationTestSuite is the base test suite for Integration tests
type IntegrationTestSuite struct {
	suite.Suite
	Harness *IntegrationTestHarness
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

// SmallObjectSuite tests small objects (< 64KB)
type SmallObjectSuite struct {
	IntegrationTestSuite
}

// SetupTest sets up for small object tests
func (s *SmallObjectSuite) SetupTest() {
	config := DefaultIntegrationTestConfig()
	config.InlineThreshold = 64 * 1024 // 64KB
	s.Config = config
	s.Harness = NewIntegrationTestHarness(s.T(), config)
}

// MediumObjectSuite tests medium objects (64KB - 16MB)
type MediumObjectSuite struct {
	IntegrationTestSuite
}

// SetupTest sets up for medium object tests
func (s *MediumObjectSuite) SetupTest() {
	config := DefaultIntegrationTestConfig()
	config.InlineThreshold = 64 * 1024         // 64KB
	config.CompactThreshold = 16 * 1024 * 1024 // 16MB
	config.CompactionInterval = 1 * time.Second
	s.Config = config
	s.Harness = NewIntegrationTestHarness(s.T(), config)
}

// LargeObjectSuite tests large objects (> 16MB)
type LargeObjectSuite struct {
	IntegrationTestSuite
}

// SetupTest sets up for large object tests
func (s *LargeObjectSuite) SetupTest() {
	config := DefaultIntegrationTestConfig()
	config.InlineThreshold = 64 * 1024         // 64KB
	config.CompactThreshold = 16 * 1024 * 1024 // 16MB
	config.SegmentSize = 256 * 1024 * 1024     // 256MB
	s.Config = config
	s.Harness = NewIntegrationTestHarness(s.T(), config)
}

// CleanerSuite tests cleaner functionality (TTL and LRU)
type CleanerSuite struct {
	IntegrationTestSuite
}

// SetupTest sets up for cleaner tests
func (s *CleanerSuite) SetupTest() {
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
	// Each workflow test creates its own harness with custom configuration
	// so we don't create a harness here to avoid conflicts
	s.Config = DefaultIntegrationTestConfig()
	s.Harness = nil
}

// TearDownTest cleans up after each workflow test
func (s *WorkflowSuite) TearDownTest() {
	// Cleanup is handled by each test individually
}

// StressSuite tests system under stress
type StressSuite struct {
	IntegrationTestSuite
}

// SetupTest sets up for stress tests
func (s *StressSuite) SetupTest() {
	config := DefaultIntegrationTestConfig()
	config.FDCacheSize = 10 // Low FD cache for stress testing
	s.Config = config
	s.Harness = NewIntegrationTestHarness(s.T(), config)
}

// Test suite runners
func TestIntegrationSmallObjects(t *testing.T) {
	suite.Run(t, new(SmallObjectSuite))
}

func TestIntegrationMediumObjects(t *testing.T) {
	suite.Run(t, new(MediumObjectSuite))
}

func TestIntegrationLargeObjects(t *testing.T) {
	suite.Run(t, new(LargeObjectSuite))
}

func TestIntegrationCleaner(t *testing.T) {
	suite.Run(t, new(CleanerSuite))
}

func TestIntegrationCompaction(t *testing.T) {
	suite.Run(t, new(CompactionSuite))
}

func TestIntegrationWorkflow(t *testing.T) {
	suite.Run(t, new(WorkflowSuite))
}

func TestIntegrationStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress tests in short mode")
	}
	suite.Run(t, new(StressSuite))
}

// Helper function to run all Integration tests
func RunAllIntegrationTests(t *testing.T) {
	t.Run("SmallObjects", TestIntegrationSmallObjects)
	t.Run("MediumObjects", TestIntegrationMediumObjects)
	t.Run("LargeObjects", TestIntegrationLargeObjects)
	t.Run("Cleaner", TestIntegrationCleaner)
	t.Run("Compaction", TestIntegrationCompaction)
	t.Run("Workflows", TestIntegrationWorkflow)
	if !testing.Short() {
		t.Run("Stress", TestIntegrationStress)
	}
}
