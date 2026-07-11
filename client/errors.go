// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package cacheclient

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// isRoutingError checks if an error indicates we should refresh topology
func isRoutingError(err error) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	// These errors indicate the node doesn't own the key anymore
	return st.Code() == codes.FailedPrecondition ||
		st.Code() == codes.NotFound ||
		st.Code() == codes.Unavailable
}

// isConnectionError checks if an error indicates a connection problem
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		// Non-gRPC errors might be network errors
		return true
	}

	// These errors indicate connection issues
	return st.Code() == codes.Unavailable ||
		st.Code() == codes.Internal ||
		st.Code() == codes.Unknown ||
		st.Code() == codes.DeadlineExceeded ||
		st.Code() == codes.Canceled
}
