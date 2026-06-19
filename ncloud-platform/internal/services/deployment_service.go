package services

import (
	"context"
	"fmt"
	"time"

	"github.com/ncloud/platform/internal/domain"
	"github.com/ncloud/platform/internal/ports"
)

// DeploymentService orchestrates the deployment lifecycle.
// It uses pure domain entities and interfaces, remaining oblivious to whether
// we are using SQLite or CockroachDB, Docker or K8s.
type DeploymentService struct {
	projectRepo ports.ProjectRepository
	deployRepo  ports.DeploymentRepository
	eventBus    ports.EventBus
}

func NewDeploymentService(projectRepo ports.ProjectRepository, deployRepo ports.DeploymentRepository, bus ports.EventBus) *DeploymentService {
	return &DeploymentService{
		projectRepo: projectRepo,
		deployRepo:  deployRepo,
		eventBus:    bus,
	}
}

// TriggerDeployment is the primary use case called by the API layer.
func (s *DeploymentService) TriggerDeployment(ctx context.Context, projectID string) (*domain.Deployment, error) {
	// 1. Verify project exists
	_, err := s.projectRepo.GetByID(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("project lookup failed: %w", err)
	}

	// 2. Create the deployment domain entity
	deploymentID := fmt.Sprintf("dep_%d", time.Now().UnixNano()) // Use ULID in real life
	deployment := domain.NewDeployment(deploymentID, projectID)

	// Persist the deployment
	err = s.deployRepo.Create(ctx, deployment)
	if err != nil {
		return nil, fmt.Errorf("failed to save deployment: %w", err)
	}

	// 3. Fire the Domain Event
	event := domain.DeploymentCreatedEvent{
		BaseEvent: domain.BaseEvent{
			DeploymentID: deployment.ID,
			ProjectID:    deployment.ProjectID,
			Timestamp:    time.Now().UTC(),
		},
	}

	err = s.eventBus.Publish(ctx, event)
	if err != nil {
		return nil, fmt.Errorf("failed to publish deployment event: %w", err)
	}

	// Return to API layer immediately (Choreography pattern)
	return deployment, nil
}
