package raft_scheduler

import (
	"encoding/json"
	"io"
	"log"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/ncloud/platform/internal/ports"
)

// SchedulerFSM implements the raft.FSM interface.
// It maintains the state of deployments in memory.
type SchedulerFSM struct {
	mu          sync.RWMutex
	deployments map[string]ports.DeploymentSpec
}

func NewSchedulerFSM() *SchedulerFSM {
	return &SchedulerFSM{
		deployments: make(map[string]ports.DeploymentSpec),
	}
}

// LogPayload is what we store in the Raft log
type LogPayload struct {
	Command string // "DEPLOY", "SCALE", "STOP"
	Spec    *ports.DeploymentSpec
	ID      string
	Replicas int
}

// Apply applies a Raft log entry to the state machine.
func (f *SchedulerFSM) Apply(l *raft.Log) interface{} {
	var payload LogPayload
	if err := json.Unmarshal(l.Data, &payload); err != nil {
		log.Printf("[FSM] Error unmarshaling log data: %v", err)
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch payload.Command {
	case "DEPLOY":
		if payload.Spec != nil {
			f.deployments[payload.Spec.DeploymentID] = *payload.Spec
			log.Printf("[FSM] Applied DEPLOY for deployment %s. Current state updated.", payload.Spec.DeploymentID)
		}
	case "SCALE":
		if spec, exists := f.deployments[payload.ID]; exists {
			spec.Replicas = payload.Replicas
			f.deployments[payload.ID] = spec
			log.Printf("[FSM] Applied SCALE for deployment %s to %d replicas.", payload.ID, payload.Replicas)
		} else {
			log.Printf("[FSM] SCALE failed, deployment %s not found.", payload.ID)
		}
	case "STOP":
		delete(f.deployments, payload.ID)
		log.Printf("[FSM] Applied STOP for deployment %s.", payload.ID)
	default:
		log.Printf("[FSM] Unknown command: %s", payload.Command)
	}

	return nil
}

// Snapshot returns an FSMSnapshot representing the current state.
func (f *SchedulerFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Clone the state for the snapshot
	stateCopy := make(map[string]ports.DeploymentSpec)
	for k, v := range f.deployments {
		stateCopy[k] = v
	}

	return &fsmSnapshot{state: stateCopy}, nil
}

// Restore restores the state machine from a snapshot.
func (f *SchedulerFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var stateCopy map[string]ports.DeploymentSpec
	if err := json.NewDecoder(rc).Decode(&stateCopy); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.deployments = stateCopy
	log.Printf("[FSM] State restored from snapshot. %d deployments loaded.", len(f.deployments))

	return nil
}

// fsmSnapshot implements raft.FSMSnapshot
type fsmSnapshot struct {
	state map[string]ports.DeploymentSpec
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(s.state)
		if err != nil {
			return err
		}

		if _, err := sink.Write(b); err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (s *fsmSnapshot) Release() {}
