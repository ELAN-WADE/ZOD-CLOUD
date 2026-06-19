package domain

import "time"

// Event is the base interface for all domain events in the system.
type Event interface {
	EventName() string
}

// Event topic constants
const (
	EventDeploymentCreated = "deployment.created"
	EventBuildStarted      = "build.started"
	EventBuildCompleted    = "build.completed"
	EventBuildFailed       = "build.failed"
	EventDeploymentStarted = "deployment.started"
	EventDomainAssigned    = "domain.assigned"
	EventDeploymentRunning = "deployment.running"
	EventDeploymentFailed  = "deployment.failed"
)

// BaseEvent struct containing common fields
type BaseEvent struct {
	DeploymentID string    `json:"deployment_id"`
	ProjectID    string    `json:"project_id"`
	Timestamp    time.Time `json:"timestamp"`
}

// Specific Event Structs
type DeploymentCreatedEvent struct {
	BaseEvent
}
func (e DeploymentCreatedEvent) EventName() string { return EventDeploymentCreated }

type BuildStartedEvent struct{ BaseEvent }
func (e BuildStartedEvent) EventName() string { return EventBuildStarted }

type BuildCompletedEvent struct {
	BaseEvent
	ImageName string `json:"image_name"`
}
func (e BuildCompletedEvent) EventName() string { return EventBuildCompleted }

type BuildFailedEvent struct {
	BaseEvent
	Error string `json:"error"`
}
func (e BuildFailedEvent) EventName() string { return EventBuildFailed }

type DeploymentStartedEvent struct{ BaseEvent }
func (e DeploymentStartedEvent) EventName() string { return EventDeploymentStarted }

type DomainAssignedEvent struct {
	BaseEvent
	InternalURL string `json:"internal_url"`
	PublicURL   string `json:"public_url"`
	TunnelID    string `json:"tunnel_id"`
}
func (e DomainAssignedEvent) EventName() string { return EventDomainAssigned }

type DeploymentRunningEvent struct{ BaseEvent }
func (e DeploymentRunningEvent) EventName() string { return EventDeploymentRunning }

type DeploymentFailedEvent struct {
	BaseEvent
	Error string `json:"error"`
}
func (e DeploymentFailedEvent) EventName() string { return EventDeploymentFailed }
