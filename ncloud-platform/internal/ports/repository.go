package ports

import (
	"context"
	"github.com/ncloud/platform/internal/domain"
)

// UserRepository defines data access for Users.
type UserRepository interface {
	Create(ctx context.Context, user *domain.User) error
	GetByID(ctx context.Context, id string) (*domain.User, error)
	GetByEmail(ctx context.Context, email string) (*domain.User, error)
}

// ProjectRepository defines the data access contract for Projects.
type ProjectRepository interface {
	Create(ctx context.Context, project *domain.Project) error
	GetByID(ctx context.Context, id string) (*domain.Project, error)
	GetByOwner(ctx context.Context, ownerID string) ([]*domain.Project, error)
	Update(ctx context.Context, project *domain.Project) error
	Delete(ctx context.Context, id string) error
}

// DeploymentRepository defines the data access contract for Deployments.
type DeploymentRepository interface {
	Create(ctx context.Context, deployment *domain.Deployment) error
	GetByID(ctx context.Context, id string) (*domain.Deployment, error)
	UpdateStatus(ctx context.Context, id string, status domain.DeploymentStatus) error
	Update(ctx context.Context, deployment *domain.Deployment) error
}

// DomainRepository defines data access for Domains.
type DomainRepository interface {
	Create(ctx context.Context, domain *domain.Domain) error
	GetByProjectID(ctx context.Context, projectID string) ([]*domain.Domain, error)
	Delete(ctx context.Context, id string) error
}

// EnvironmentVariableRepository defines data access for Env Vars.
type EnvironmentVariableRepository interface {
	Create(ctx context.Context, envVar *domain.EnvironmentVariable) error
	GetByProjectID(ctx context.Context, projectID string) ([]*domain.EnvironmentVariable, error)
	Delete(ctx context.Context, id string) error
}

// BuildLogRepository defines data access for Build Logs.
type BuildLogRepository interface {
	Create(ctx context.Context, buildLog *domain.BuildLog) error
	GetByDeploymentID(ctx context.Context, deploymentID string) ([]*domain.BuildLog, error)
}
