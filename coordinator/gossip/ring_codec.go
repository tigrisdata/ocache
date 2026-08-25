// Copyright 2026 Tigris Data, Inc.
// SPDX-License-Identifier: Apache-2.0

package gossip

import (
	"sync"

	"github.com/grafana/dskit/kv/codec"
	dskitring "github.com/grafana/dskit/ring"
)

// membershipCodec keeps the ring wire format unchanged while exposing decoded
// ring deltas to local consumers. The memberlist KV decodes an incoming change
// immediately before merging it into its store; observers can therefore update
// their process-local membership state before a concurrent local CAS reads that
// store.
type membershipCodec struct {
	delegate codec.Codec

	mu        sync.RWMutex
	observers []func(*dskitring.Desc)
}

func newMembershipCodec(delegate codec.Codec) *membershipCodec {
	return &membershipCodec{delegate: delegate}
}

func (c *membershipCodec) CodecID() string {
	return c.delegate.CodecID()
}

func (c *membershipCodec) Encode(value interface{}) ([]byte, error) {
	return c.delegate.Encode(value)
}

func (c *membershipCodec) Decode(data []byte) (interface{}, error) {
	value, err := c.delegate.Decode(data)
	if err != nil {
		return nil, err
	}

	desc, ok := value.(*dskitring.Desc)
	if !ok || desc == nil {
		return value, nil
	}

	c.mu.RLock()
	observers := append([]func(*dskitring.Desc){}, c.observers...)
	c.mu.RUnlock()
	for _, observer := range observers {
		observer(desc)
	}
	return value, nil
}

// RegisterRingChangeObserver registers a callback for decoded ring changes.
// Callbacks run synchronously in the decoder's goroutine and must not retain or
// mutate desc. A ring manager registers before its lifecycler starts.
func (c *membershipCodec) RegisterRingChangeObserver(observer func(*dskitring.Desc)) {
	if observer == nil {
		return
	}
	c.mu.Lock()
	c.observers = append(c.observers, observer)
	c.mu.Unlock()
}

var _ codec.Codec = (*membershipCodec)(nil)
