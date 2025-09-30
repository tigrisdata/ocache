package integration

// CoordinatorObjectsSuite uses the same tests as ObjectsSuite since they share
// the TestHarnessInterface. The tests are defined in objects_test.go.

// Test_Objects_BasicFlow - inherited from ObjectsSuite via embedding
func (s *ClusterSuite) Test_Objects_BasicFlow() {
	// Call the ObjectsSuite implementation
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_BasicFlow()
}

// Test_Objects_EdgeCases - inherited from ObjectsSuite
func (s *ClusterSuite) Test_Objects_EdgeCases() {
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_EdgeCases()
}

// Test_Objects_UpdateExisting - inherited from ObjectsSuite
func (s *ClusterSuite) Test_Objects_UpdateExisting() {
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_UpdateExisting()
}

// Test_Objects_MixedSizes - inherited from ObjectsSuite
func (s *ClusterSuite) Test_Objects_MixedSizes() {
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_MixedSizes()
}

// Test_Objects_StreamingWrite - inherited from ObjectsSuite
func (s *ClusterSuite) Test_Objects_StreamingWrite() {
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_StreamingWrite()
}

// Test_Objects_CompactionBehavior - inherited from ObjectsSuite
func (s *ClusterSuite) Test_Objects_CompactionBehavior() {
	objSuite := &ObjectsSuite{IntegrationTestSuite: s.IntegrationTestSuite}
	objSuite.Test_Objects_CompactionBehavior()
}

// Note: Test_Objects_LRUEviction and Test_Objects_MixedTTL are automatically
// skipped in cluster mode by the checks in objects_test.go
