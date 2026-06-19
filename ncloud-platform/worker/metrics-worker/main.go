package main

import (
	"context"
	"log"
	"time"

	"github.com/ncloud/platform/adapters/mesh"
)

// MetricsCollectedEvent is a new domain event for Phase 3
type MetricsCollectedEvent struct {
	DeploymentID string
	CPUUsage     float64
	Timestamp    time.Time
}

func (e MetricsCollectedEvent) EventName() string {
	return "metrics.collected"
}

func main() {
	log.Println("Starting NCloud Metrics Worker (Phase 3/4)...")

	// 1. Initialize Adapters
	eventBus := mesh.NewMeshEventBus("global-mesh")

	// 2. Setup ticker for 60 seconds
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	log.Println("Metrics collection loop started...")

	for {
		select {
		case <-ticker.C:
			// In reality, we'd query the Scheduler (e.g. s.GetMetrics("dep_xxx"))
			// For this mock, we generate a fake metric
			event := MetricsCollectedEvent{
				DeploymentID: "dep_abc123",
				CPUUsage:     85.5, // High CPU to trigger autoscaler
				Timestamp:    time.Now().UTC(),
			}

			// Publish to NATS JetStream
			err := eventBus.Publish(context.Background(), event)
			if err != nil {
				log.Printf("Failed to publish metrics: %v", err)
			} else {
				log.Printf("Published metrics for %s: CPU %.2f%%", event.DeploymentID, event.CPUUsage)
			}
		}
	}
}
