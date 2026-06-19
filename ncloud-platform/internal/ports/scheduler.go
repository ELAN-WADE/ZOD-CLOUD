package ports

import (
	"context"
)

// DeploymentSpec holds the instructions for the orchestrator.
// Note: There are ZERO K8s/Docker specific fields here.
type DeploymentSpec struct {
	DeploymentID  string
	ProjectID     string
	Image         string
	Region        string
	Replicas      int
	CPURequest    string
	MemoryRequest string
	EnvVars       map[string]string
}

// LogLine represents a single output line from a running container.
type LogLine struct {
	Timestamp string
	Message   string
}

// Metrics represents runtime stats for a deployment.
type Metrics struct {
	CPUUsagePercentage float64
	MemoryUsageBytes   int64
}

// Scheduler defines the contract for our container orchestrator.
// This allows us to swap Docker -> K3s -> K8s -> Custom Scheduler.
type Scheduler interface {
	Deploy(ctx context.Context, spec DeploymentSpec) error
	Scale(ctx context.Context, deploymentID string, replicas int) error
	Stop(ctx context.Context, deploymentID string) error
	GetLogs(ctx context.Context, deploymentID string, tail int) ([]LogLine, error)
	GetMetrics(ctx context.Context, deploymentID string) (Metrics, error)
}
