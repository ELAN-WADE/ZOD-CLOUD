package mesh

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/ncloud/platform/internal/domain"
	"github.com/ncloud/platform/internal/ports"
)

// MeshEventBus implements the EventBus interface with a Global Mesh topology.
// In Phase 4, this simulates NATS JetStream with Cross-Region Mirrors
// and Vector Clocks for active-active conflict resolution.
type MeshEventBus struct {
	mu           sync.RWMutex
	subscribers  map[string]map[int]ports.EventHandler
	vectorClocks map[string]int64 // Simulated vector clocks
	region       string
	nextID       int
}

func NewMeshEventBus(region string) *MeshEventBus {
	return &MeshEventBus{
		subscribers:  make(map[string]map[int]ports.EventHandler),
		vectorClocks: make(map[string]int64),
		region:       region,
	}
}

// Publish publishes an event to the global mesh.
func (m *MeshEventBus) Publish(ctx context.Context, event domain.Event) error {
	topic := event.EventName()
	
	// Simulate adding vector clock metadata to event payload
	// Do expensive JSON marshal OUTSIDE the lock
	eventData, err := json.Marshal(event)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.vectorClocks[m.region]++
	clock := m.vectorClocks[m.region]
	
	handlers := make([]ports.EventHandler, 0, len(m.subscribers[topic]))
	for _, h := range m.subscribers[topic] {
		handlers = append(handlers, h)
	}
	m.mu.Unlock()

	log.Printf("[Mesh] [%s] Publishing %s (Clock: %d): %s", m.region, topic, clock, string(eventData))

	// 2. Publish to local subscribers (simulating local mirror read/write)
	for _, handler := range handlers {
		go func(h ports.EventHandler) {
			_ = h(context.Background(), eventData)
		}(handler)
	}

	return nil
}

// Subscribe subscribes to a topic. In a mesh, this would attach to a local Mirror.
func (m *MeshEventBus) Subscribe(ctx context.Context, topic string, handler ports.EventHandler) (func() error, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.subscribers[topic] == nil {
		m.subscribers[topic] = make(map[int]ports.EventHandler)
	}
	id := m.nextID
	m.nextID++
	m.subscribers[topic][id] = handler

	log.Printf("[Mesh] [%s] Subscribed to %s via Local Mirror (sub-10ms latency)", m.region, topic)

	unsubscribe := func() error {
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.subscribers[topic], id)
		return nil
	}

	return unsubscribe, nil
}
