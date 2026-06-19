package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	"github.com/ncloud/platform/adapters/docker"
	"github.com/ncloud/platform/adapters/mesh"
	"github.com/ncloud/platform/internal/domain"
	"github.com/ncloud/platform/internal/ports"
)



func main() {
	log.Println("Starting NCloud Deploy Worker (Phase 7)...")

	eventBus := mesh.NewMeshEventBus("global-mesh")
	scheduler := docker.NewLocalDockerScheduler()

	unsub, err := eventBus.Subscribe(context.Background(), domain.EventBuildCompleted, func(ctx context.Context, payload []byte) error {
		var event domain.BuildCompletedEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			log.Printf("Failed to unmarshal event: %v", err)
			return err
		}

		log.Printf("[DeployWorker] Received build.completed for %s. Triggering Docker Scheduler...", event.DeploymentID)

		spec := ports.DeploymentSpec{
			DeploymentID: event.DeploymentID,
			Image:        event.ImageName,
		}

		if err := scheduler.Deploy(ctx, spec); err != nil {
			log.Printf("[DeployWorker] Deployment FAILED: %v", err)
			return err
		}

		log.Printf("[DeployWorker] Successfully deployed %s via Docker Scheduler!", event.DeploymentID)
		return nil
	})

	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	defer unsub()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}
