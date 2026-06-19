package ports

import (
	"context"
	"github.com/ncloud/platform/internal/domain"
)

// EventHandler is the callback signature for consumers.
type EventHandler func(ctx context.Context, eventBytes []byte) error

// EventBus defines the contract for pub/sub messaging.
// This allows us to evolve from Channels -> NATS Core -> NATS JetStream -> Global Event Mesh.
type EventBus interface {
	// Publish emits a domain event to the bus.
	Publish(ctx context.Context, event domain.Event) error
	
	// Subscribe registers a handler for a specific event topic.
	// For NATS, topic could be "deployments.*", for Channels it's just a string map key.
	// It returns a function that can be called to unsubscribe.
	Subscribe(ctx context.Context, topic string, handler EventHandler) (func() error, error)
}
