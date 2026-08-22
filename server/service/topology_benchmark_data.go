// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build ocache_topology_benchmark

package service

// cacheServiceData is empty because topology requests do not access the storage
// data path.
type cacheServiceData struct{}
