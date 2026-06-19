package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/ncloud/platform/internal/domain"
	"github.com/ncloud/platform/internal/ports"
)

// JetStreamEventBus implements the EventBus port using NATS JetStream.
// This is the Phase 2 upgrade from Go Channels, providing persistent, at-least-once delivery.
type JetStreamEventBus struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

func NewJetStreamEventBus(url string) (*JetStreamEventBus, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	// In a real setup, we would ensure the stream exists here:
	// js.AddStream(&nats.StreamConfig{Name: "deployments", Subjects: []string{"deployments.*"}})

	return &JetStreamEventBus{nc: nc, js: js}, nil
}

func (b *JetStreamEventBus) Publish(ctx context.Context, event domain.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// We map the EventName() directly to a NATS Subject
	// e.g., "deployments.created"
	_, err = b.js.Publish(event.EventName(), payload)
	if err != nil {
		return fmt.Errorf("failed to publish to NATS JetStream: %w", err)
	}

	log.Printf("Published %s to JetStream successfully", event.EventName())
	return nil
}

func (b *JetStreamEventBus) Subscribe(ctx context.Context, topic string, handler ports.EventHandler) (func() error, error) {
	// For workers, we use QueueSubscribe to load balance across multiple instances
	sub, err := b.js.QueueSubscribe(topic, "ncloud-workers", func(msg *nats.Msg) {
		// Call the application's handler
		err := handler(context.Background(), msg.Data)
		if err != nil {
			log.Printf("Worker failed to process message: %v", err)
			// Depending on policy, we might Nak() here
			// msg.Nak()
			return
		}
		// Acknowledge the message so it's not redelivered
		msg.Ack()
	})

	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to JetStream: %w", err)
	}

	unsubscribe := func() error {
		return sub.Unsubscribe()
	}

	return unsubscribe, nil
}
