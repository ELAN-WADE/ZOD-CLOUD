package spanner

import (
	"context"
	"log"
	"sync"

	"github.com/ncloud/platform/internal/domain"
)

// SpannerDatabase simulates a Spanner cluster
type SpannerDatabase struct {
	mu sync.RWMutex
	projects    map[string]*domain.Project
	deployments map[string]*domain.Deployment
}

func NewSpannerDatabase() *SpannerDatabase {
	return &SpannerDatabase{
		projects:    make(map[string]*domain.Project),
		deployments: make(map[string]*domain.Deployment),
	}
}

func (db *SpannerDatabase) simulateTrueTime() {
	log.Println("[Spanner] Waiting for TrueTime commit bound...")
}

type SpannerProjectRepository struct {
	db *SpannerDatabase
}

func NewSpannerProjectRepository(db *SpannerDatabase) *SpannerProjectRepository {
	return &SpannerProjectRepository{db: db}
}

func (r *SpannerProjectRepository) Create(ctx context.Context, project *domain.Project) error {
	r.db.simulateTrueTime()
	r.db.mu.Lock()
	defer r.db.mu.Unlock()
	r.db.projects[project.ID] = project
	log.Printf("[Spanner] Created project %s with TrueTime consistency", project.ID)
	return nil
}

func (r *SpannerProjectRepository) GetByID(ctx context.Context, id string) (*domain.Project, error) {
	r.db.mu.RLock()
	defer r.db.mu.RUnlock()
	if p, ok := r.db.projects[id]; ok {
		return p, nil
	}
	return nil, domain.ErrNotFound
}

func (r *SpannerProjectRepository) GetByOwner(ctx context.Context, ownerID string) ([]*domain.Project, error) {
	r.db.mu.RLock()
	defer r.db.mu.RUnlock()
	var result []*domain.Project
	for _, p := range r.db.projects {
		if p.OwnerID == ownerID {
			result = append(result, p)
		}
	}
	return result, nil
}

func (r *SpannerProjectRepository) Update(ctx context.Context, project *domain.Project) error {
	r.db.simulateTrueTime()
	r.db.mu.Lock()
	defer r.db.mu.Unlock()
	if _, ok := r.db.projects[project.ID]; !ok {
		return domain.ErrNotFound
	}
	r.db.projects[project.ID] = project
	return nil
}

func (r *SpannerProjectRepository) Delete(ctx context.Context, id string) error {
	r.db.simulateTrueTime()
	r.db.mu.Lock()
	defer r.db.mu.Unlock()
	delete(r.db.projects, id)
	return nil
}

type SpannerDeploymentRepository struct {
	db *SpannerDatabase
}

func NewSpannerDeploymentRepository(db *SpannerDatabase) *SpannerDeploymentRepository {
	return &SpannerDeploymentRepository{db: db}
}

func (r *SpannerDeploymentRepository) Create(ctx context.Context, deployment *domain.Deployment) error {
	r.db.simulateTrueTime()
	r.db.mu.Lock()
	defer r.db.mu.Unlock()
	r.db.deployments[deployment.ID] = deployment
	log.Printf("[Spanner] Created deployment %s with TrueTime consistency", deployment.ID)
	return nil
}

func (r *SpannerDeploymentRepository) GetByID(ctx context.Context, id string) (*domain.Deployment, error) {
	r.db.mu.RLock()
	defer r.db.mu.RUnlock()
	if d, ok := r.db.deployments[id]; ok {
		return d, nil
	}
	return nil, domain.ErrNotFound
}

func (r *SpannerDeploymentRepository) UpdateStatus(ctx context.Context, id string, status domain.DeploymentStatus) error {
	r.db.simulateTrueTime()
	r.db.mu.Lock()
	defer r.db.mu.Unlock()
	if d, ok := r.db.deployments[id]; ok {
		d.Status = status
		return nil
	}
	return domain.ErrNotFound
}

func (r *SpannerDeploymentRepository) Update(ctx context.Context, d *domain.Deployment) error {
	r.db.simulateTrueTime()
	r.db.mu.Lock()
	defer r.db.mu.Unlock()
	if _, ok := r.db.deployments[d.ID]; !ok {
		return domain.ErrNotFound
	}
	r.db.deployments[d.ID] = d
	return nil
}
