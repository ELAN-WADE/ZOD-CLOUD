package raft_scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"github.com/ncloud/platform/internal/ports"
)

// RaftScheduler implements the ports.Scheduler using Raft consensus.
type RaftScheduler struct {
	raftNode *raft.Raft
	fsm      *SchedulerFSM
}

// NewRaftScheduler creates a new RaftScheduler
func NewRaftScheduler(raftNode *raft.Raft, fsm *SchedulerFSM) *RaftScheduler {
	return &RaftScheduler{
		raftNode: raftNode,
		fsm:      fsm,
	}
}

func (s *RaftScheduler) applyCommand(command string, spec *ports.DeploymentSpec, id string, replicas int) error {
	if s.raftNode.State() != raft.Leader {
		return fmt.Errorf("not the leader, cannot apply command (leader is %s)", s.raftNode.Leader())
	}

	payload := LogPayload{
		Command:  command,
		Spec:     spec,
		ID:       id,
		Replicas: replicas,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// Apply the log to the Raft cluster
	future := s.raftNode.Apply(b, 5*time.Second)
	if err := future.Error(); err != nil {
		return err
	}

	// Wait for the FSM to actually apply it and return any response
	response := future.Response()
	if err, ok := response.(error); ok && err != nil {
		return err
	}

	return nil
}

func (s *RaftScheduler) Deploy(ctx context.Context, spec ports.DeploymentSpec) error {
	return s.applyCommand("DEPLOY", &spec, spec.DeploymentID, spec.Replicas)
}

func (s *RaftScheduler) Scale(ctx context.Context, deploymentID string, replicas int) error {
	return s.applyCommand("SCALE", nil, deploymentID, replicas)
}

func (s *RaftScheduler) Stop(ctx context.Context, deploymentID string) error {
	return s.applyCommand("STOP", nil, deploymentID, 0)
}

func (s *RaftScheduler) GetLogs(ctx context.Context, deploymentID string, tail int) ([]ports.LogLine, error) {
	// For now, return dummy logs
	return []ports.LogLine{
		{Timestamp: time.Now().Format(time.RFC3339), Message: "Raft Control Plane: Logs not yet integrated."},
	}, nil
}

func (s *RaftScheduler) GetMetrics(ctx context.Context, deploymentID string) (ports.Metrics, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()

	_, exists := s.fsm.deployments[deploymentID]
	if !exists {
		return ports.Metrics{}, fmt.Errorf("deployment %s not found in FSM", deploymentID)
	}

	// Mock metrics based on FSM presence
	return ports.Metrics{
		CPUUsagePercentage: 15.0,
		MemoryUsageBytes:   1024 * 1024 * 50,
	}, nil
}
