// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"context"

	"github.com/grafana/dskit/kv"
	dskitmemberlist "github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
)

// observedClient exposes the ring decoder's synchronous change hook while
// preserving the kv.Client API used by dskit ring and lifecycler clients.
type observedClient struct {
	delegate *dskitmemberlist.Client
	codec    *membershipCodec
}

func (c *observedClient) List(ctx context.Context, prefix string) ([]string, error) {
	return c.delegate.List(ctx, prefix)
}

func (c *observedClient) Get(ctx context.Context, key string) (interface{}, error) {
	return c.delegate.Get(ctx, key)
}

func (c *observedClient) Delete(ctx context.Context, key string) error {
	return c.delegate.Delete(ctx, key)
}

func (c *observedClient) CAS(ctx context.Context, key string, f func(interface{}) (interface{}, bool, error)) error {
	return c.delegate.CAS(ctx, key, f)
}

func (c *observedClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	c.delegate.WatchKey(ctx, key, f)
}

func (c *observedClient) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	c.delegate.WatchPrefix(ctx, prefix, f)
}

func (c *observedClient) RegisterRingChangeObserver(observer func(*ring.Desc)) {
	c.codec.RegisterRingChangeObserver(observer)
}

var _ kv.Client = (*observedClient)(nil)
