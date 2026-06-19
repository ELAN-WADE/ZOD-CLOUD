package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ncloud/platform/internal/domain"
	"github.com/ncloud/platform/internal/ports"
)

// ChannelEventBus is a Phase 1 implementation of EventBus using Go Channels.
// Pros: Zero dependencies, fast.
// Cons: Single process, loses events on crash.
type ChannelEventBus struct {
	mu          sync.RWMutex
	subscribers map[string]map[int]ports.EventHandler
	nextID      int
}

func NewChannelEventBus() *ChannelEventBus {
	return &ChannelEventBus{
		subscribers: make(map[string]map[int]ports.EventHandler),
	}
}

// Publish serializes the event and sends it to all handlers registered for the topic.
func (b *ChannelEventBus) Publish(ctx context.Context, event domain.Event) error {
	topic := event.EventName()
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	b.mu.RLock()
	handlers := make([]ports.EventHandler, 0, len(b.subscribers[topic]))
	for _, h := range b.subscribers[topic] {
		handlers = append(handlers, h)
	}
	b.mu.RUnlock()

	// In a real system, we'd dispatch to a Go channel for async processing.
	// For this prototype, we'll spawn a goroutine to simulate async workers.
	for _, handler := range handlers {
		go func(h ports.EventHandler) {
			// Simulate network latency/worker pick-up time
			_ = h(context.Background(), payload)
		}(handler)
	}

	return nil
}

// Subscribe registers a handler for a topic.
func (b *ChannelEventBus) Subscribe(ctx context.Context, topic string, handler ports.EventHandler) (func() error, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subscribers[topic] == nil {
		b.subscribers[topic] = make(map[int]ports.EventHandler)
	}
	id := b.nextID
	b.nextID++
	b.subscribers[topic][id] = handler

	unsubscribe := func() error {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.subscribers[topic], id)
		return nil
	}

	return unsubscribe, nil
}
