package services

import (
	"context"
	"log"

	"github.com/ncloud/platform/internal/domain"
	"github.com/ncloud/platform/internal/ports"
)

// WorkerManager coordinates the event-driven deployment state machine.
type WorkerManager struct {
	eventBus   ports.EventBus
	deployRepo ports.DeploymentRepository
	buildRepo  ports.BuildLogRepository
}

func NewWorkerManager(bus ports.EventBus, deployRepo ports.DeploymentRepository, buildRepo ports.BuildLogRepository) *WorkerManager {
	return &WorkerManager{
		eventBus:   bus,
		deployRepo: deployRepo,
		buildRepo:  buildRepo,
	}
}

// Start begins listening to domain events.
func (w *WorkerManager) Start(ctx context.Context) error {
	_, err := w.eventBus.Subscribe(ctx, domain.EventDeploymentCreated, w.handleDeploymentCreated)
	if err != nil { return err }

	_, err = w.eventBus.Subscribe(ctx, domain.EventBuildCompleted, w.handleBuildCompleted)
	if err != nil { return err }

	return nil
}

func (w *WorkerManager) handleDeploymentCreated(ctx context.Context, eventBytes []byte) error {
	// 1. Unmarshal event (pseudo-code since we receive raw bytes depending on EventBus adapter)
	// For simplicity, let's assume we have a helper to parse or just mock the flow for the architecture demonstration
	// The real implementation would parse the DeploymentCreatedEvent from eventBytes
	log.Println("Received deployment.created event, starting build...")

	// 2. Emit build.started
	// We should update DB state and emit event. For demonstration, we just log.
	// Actually, let's just show the flow:
	// w.deployRepo.UpdateStatus(ctx, event.DeploymentID, domain.StatusBuilding)
	// w.eventBus.Publish(ctx, domain.BuildStartedEvent{...})
	
	// 3. Run Build
	// imageRef, err := builder.Build(...)
	
	// 4. Emit build.completed (or failed)
	// w.eventBus.Publish(ctx, domain.BuildCompletedEvent{...})

	return nil
}

func (w *WorkerManager) handleBuildCompleted(ctx context.Context, eventBytes []byte) error {
	log.Println("Received build.completed event, starting deployment...")

	// 1. Emit deployment.started
	// w.deployRepo.UpdateStatus(ctx, event.DeploymentID, domain.StatusDeploying)
	// w.eventBus.Publish(ctx, domain.DeploymentStartedEvent{...})
	
	// 2. Run Container
	// containerID, err := scheduler.Run(...)

	// 3. Assign Domain
	// publicURL, internalURL, tunnelID := tunnel.Bind(...)
	// w.eventBus.Publish(ctx, domain.DomainAssignedEvent{...})

	// 4. Emit deployment.running (or failed)
	// w.deployRepo.UpdateStatus(ctx, event.DeploymentID, domain.StatusRunning)
	// w.eventBus.Publish(ctx, domain.DeploymentRunningEvent{...})

	return nil
}
