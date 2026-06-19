package raft_scheduler

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// SetupRaftNode initializes a Raft node, returning the raft instance and the FSM.
// nodeID should be unique across the cluster.
// baseDir is where logs and snapshots will be stored.
// bindAddr is the address to bind the Raft transport to (e.g., "127.0.0.1:8080").
// bootstrap true will bootstrap the cluster if it's new.
func SetupRaftNode(nodeID string, baseDir string, bindAddr string, bootstrap bool) (*raft.Raft, *SchedulerFSM, error) {
	// 1. Setup the configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	// 2. Setup the FSM
	fsm := NewSchedulerFSM()

	// 3. Setup the Log Store (BoltDB)
	logStorePath := filepath.Join(baseDir, "raft-log.db")
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("could not create base dir: %v", err)
	}
	
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create bolt store: %v", err)
	}

	// Stable store can be the same boltdb store
	stableStore := logStore

	// 4. Setup the Snapshot Store
	snapshotStore, err := raft.NewFileSnapshotStore(baseDir, 1, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create snapshot store: %v", err)
	}

	// 5. Setup the Transport
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("could not resolve tcp address: %v", err)
	}

	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create tcp transport: %v", err)
	}

	// 6. Create the Raft Node
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create raft node: %v", err)
	}

	// 7. Bootstrap the cluster if requested
	if bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	return r, fsm, nil
}
