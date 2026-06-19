package edgekv

import (
	"context"
	"log"

	"github.com/ncloud/platform/internal/domain"
)

// EdgeKVDeploymentRepository implements DeploymentRepository for Phase 5.
type EdgeKVDeploymentRepository struct {
	db *EdgeKVProjectRepository // In reality, we'd have an EdgeKVDatabase struct containing the maps
}

func NewEdgeKVDeploymentRepository(db *EdgeKVProjectRepository) *EdgeKVDeploymentRepository {
	return &EdgeKVDeploymentRepository{db: db}
}

func (r *EdgeKVDeploymentRepository) Create(ctx context.Context, deployment *domain.Deployment) error {
	// For this simulation, we'll just log it. A real KV store would have a deployments table/namespace.
	r.db.simulateEdgeReplication()
	log.Printf("[EdgeKV] Created deployment %s with eventual consistency", deployment.ID)
	return nil
}

func (r *EdgeKVDeploymentRepository) GetByID(ctx context.Context, id string) (*domain.Deployment, error) {
	// Simulated
	return nil, domain.ErrNotFound
}

func (r *EdgeKVDeploymentRepository) UpdateStatus(ctx context.Context, id string, status domain.DeploymentStatus) error {
	r.db.simulateEdgeReplication()
	return nil
}

func (r *EdgeKVDeploymentRepository) Update(ctx context.Context, deployment *domain.Deployment) error {
	r.db.simulateEdgeReplication()
	return nil
}
