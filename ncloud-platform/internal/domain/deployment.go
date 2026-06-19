package domain

import "time"

// DeploymentStatus represents the lifecycle phase of a deployment.
type DeploymentStatus string

const (
	StatusQueued    DeploymentStatus = "queued"
	StatusBuilding  DeploymentStatus = "building"
	StatusDeploying DeploymentStatus = "deploying"
	StatusRunning   DeploymentStatus = "running"
	StatusFailed    DeploymentStatus = "failed"
)

// Deployment represents a specific build and run instance of a Project.
type Deployment struct {
	ID          string           `json:"id"`
	ProjectID   string           `json:"project_id"`
	Status      DeploymentStatus `json:"status"`
	ImageName   string           `json:"image_name"`
	ContainerID string           `json:"container_id"`
	PublicURL   string           `json:"public_url"`
	InternalURL string           `json:"internal_url"`
	TunnelID    string           `json:"tunnel_id"`
	CreatedAt   time.Time        `json:"created_at"`
}

// NewDeployment creates a new deployment in the queued status.
func NewDeployment(id, projectID string) *Deployment {
	return &Deployment{
		ID:        id,
		ProjectID: projectID,
		Status:    StatusQueued,
		CreatedAt: time.Now().UTC(),
	}
}

// UpdateStatus transitions the deployment to a new status.
func (d *Deployment) UpdateStatus(newStatus DeploymentStatus) {
	d.Status = newStatus
}
