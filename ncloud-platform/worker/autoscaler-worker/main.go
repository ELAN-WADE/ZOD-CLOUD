package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/ncloud/platform/adapters/mesh"
)

// This struct mirrors the one in metrics-worker
type MetricsCollectedEvent struct {
	DeploymentID string  `json:"deployment_id"`
	CPUUsage     float64 `json:"cpu_usage"`
	Timestamp    string  `json:"timestamp"`
}

func main() {
	log.Println("Starting NCloud Auto-Scaler Worker (Phase 3/4)...")

	// Initialize Adapters
	eventBus := mesh.NewMeshEventBus("global-mesh")

	// Subscribe to metrics
	unsub, err := eventBus.Subscribe(context.Background(), "metrics.collected", handleMetrics)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	defer unsub()

	log.Println("Auto-Scaler listening for metrics.collected events...")
	select {}
}

func handleMetrics(ctx context.Context, payload []byte) error {
	var event MetricsCollectedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}

	// ML-Based / Threshold-Based Auto-Scaling Logic
	if event.CPUUsage > 80.0 {
		log.Printf("ALERT: High CPU detected (%.2f%%) for %s. Triggering scale up!", event.CPUUsage, event.DeploymentID)
		
		// Here we would call: scheduler.Scale(ctx, event.DeploymentID, currentReplicas + 1)
		// And maybe publish a "deployments.scaled" event for billing to charge more.
	}

	return nil
}
