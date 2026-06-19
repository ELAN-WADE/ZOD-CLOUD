package edgekv

import (
	"context"
	"log"
	"sync"

	"github.com/ncloud/platform/internal/domain"
)

// EdgeKVProjectRepository implements ProjectRepository for Phase 5.
// It simulates a globally distributed, low-latency Key-Value store
// (like Cloudflare KV or DynamoDB Global Tables) optimized for Edge reads.
type EdgeKVProjectRepository struct {
	mu       sync.RWMutex
	projects map[string]*domain.Project
	region   string
}

func NewEdgeKVProjectRepository(region string) *EdgeKVProjectRepository {
	return &EdgeKVProjectRepository{
		projects: make(map[string]*domain.Project),
		region:   region,
	}
}

func (r *EdgeKVProjectRepository) simulateEdgeReplication() {
	// Simulate eventual consistency replication to other Edge POPs
	log.Printf("[EdgeKV] [%s] Replicating mutation globally (eventual consistency)", r.region)
}

func (r *EdgeKVProjectRepository) Create(ctx context.Context, project *domain.Project) error {
	r.mu.Lock()
	r.projects[project.ID] = project
	r.mu.Unlock()

	r.simulateEdgeReplication()
	return nil
}

func (r *EdgeKVProjectRepository) GetByID(ctx context.Context, id string) (*domain.Project, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	log.Printf("[EdgeKV] [%s] Fast read (< 5ms latency) for project %s", r.region, id)
	if p, ok := r.projects[id]; ok {
		return p, nil
	}
	return nil, domain.ErrNotFound
}

func (r *EdgeKVProjectRepository) GetByOwner(ctx context.Context, ownerID string) ([]*domain.Project, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// KV stores typically use secondary indexes for this
	var result []*domain.Project
	for _, p := range r.projects {
		if p.OwnerID == ownerID {
			result = append(result, p)
		}
	}
	return result, nil
}

func (r *EdgeKVProjectRepository) Update(ctx context.Context, project *domain.Project) error {
	r.mu.Lock()
	if _, ok := r.projects[project.ID]; !ok {
		r.mu.Unlock()
		return domain.ErrNotFound
	}
	r.projects[project.ID] = project
	r.mu.Unlock()

	r.simulateEdgeReplication()
	return nil
}

func (r *EdgeKVProjectRepository) Delete(ctx context.Context, id string) error {
	r.mu.Lock()
	delete(r.projects, id)
	r.mu.Unlock()

	r.simulateEdgeReplication()
	return nil
}
