package websocket

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/ncloud/platform/internal/domain"
	"github.com/ncloud/platform/internal/ports"
)

// WebSocketEventBus implements EventBus for Phase 5.
// It simulates pushing events directly to Edge POPs or connected clients
// via WebSockets for real-time reactivity without polling.
type WebSocketEventBus struct {
	mu          sync.RWMutex
	subscribers map[string]map[int]ports.EventHandler
	nextID      int
}

func NewWebSocketEventBus() *WebSocketEventBus {
	return &WebSocketEventBus{
		subscribers: make(map[string]map[int]ports.EventHandler),
	}
}

func (b *WebSocketEventBus) Publish(ctx context.Context, event domain.Event) error {
	topic := event.EventName()
	
	// Simulate JSON serialization for WebSocket frame
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	b.mu.RLock()
	handlers := make([]ports.EventHandler, 0, len(b.subscribers[topic]))
	for _, h := range b.subscribers[topic] {
		handlers = append(handlers, h)
	}
	b.mu.RUnlock()

	log.Printf("[WebSocket Router] Pushing %s event to %d Edge nodes", topic, len(handlers))

	// Simulate async network push
	for _, handler := range handlers {
		go func(h ports.EventHandler) {
			_ = h(context.Background(), payload)
		}(handler)
	}

	return nil
}

func (b *WebSocketEventBus) Subscribe(ctx context.Context, topic string, handler ports.EventHandler) (func() error, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subscribers[topic] == nil {
		b.subscribers[topic] = make(map[int]ports.EventHandler)
	}
	
	id := b.nextID
	b.nextID++
	b.subscribers[topic][id] = handler

	log.Printf("[WebSocket Router] Client subscribed to stream: %s", topic)

	unsubscribe := func() error {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.subscribers[topic], id)
		return nil
	}

	return unsubscribe, nil
}
