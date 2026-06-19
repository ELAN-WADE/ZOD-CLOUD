package wasm_edge

import (
	"context"
	"fmt"
	"log"

	"github.com/ncloud/platform/internal/ports"
)

// WasmEdgeScheduler implements the Scheduler port for Phase 5.
// It simulates deploying WebAssembly modules to Edge POPs (Points of Presence)
// with sub-millisecond cold starts, bypassing heavy K8s orchestrators.
type WasmEdgeScheduler struct {
	// In reality, this would manage a pool of wazero or WasmEdge runtimes
	activeFunctions map[string]ports.DeploymentSpec
}

func NewWasmEdgeScheduler() *WasmEdgeScheduler {
	return &WasmEdgeScheduler{
		activeFunctions: make(map[string]ports.DeploymentSpec),
	}
}

func (s *WasmEdgeScheduler) Deploy(ctx context.Context, spec ports.DeploymentSpec) error {
	// Simulate downloading the .wasm binary from a registry
	log.Printf("[WASM Edge] Deploying %s (WASM module: %s) to Edge POPs in Region %s", spec.DeploymentID, spec.Image, spec.Region)

	s.activeFunctions[spec.DeploymentID] = spec

	// Simulate < 1ms cold start
	log.Printf("[WASM Edge] Function %s active and ready in 0.8ms", spec.DeploymentID)
	return nil
}

func (s *WasmEdgeScheduler) Scale(ctx context.Context, deploymentID string, replicas int) error {
	// WASM at the edge scales implicitly based on request volume.
	// Explicit replica counts aren't as meaningful, but we can log it.
	log.Printf("[WASM Edge] Scaling %s concurrency to %d (Edge instances)", deploymentID, replicas)
	return nil
}

func (s *WasmEdgeScheduler) Stop(ctx context.Context, deploymentID string) error {
	if _, ok := s.activeFunctions[deploymentID]; ok {
		delete(s.activeFunctions, deploymentID)
		log.Printf("[WASM Edge] Evicted %s from Edge cache", deploymentID)
		return nil
	}
	return fmt.Errorf("wasm function not found")
}

func (s *WasmEdgeScheduler) GetLogs(ctx context.Context, deploymentID string, tail int) ([]ports.LogLine, error) {
	return []ports.LogLine{
		{Timestamp: "Now", Message: "[Edge V8 Isolate] Function executed successfully"},
	}, nil
}

func (s *WasmEdgeScheduler) GetMetrics(ctx context.Context, deploymentID string) (ports.Metrics, error) {
	return ports.Metrics{
		CPUUsagePercentage: 1.2, // WASM is very lightweight
		MemoryUsageBytes:   1024 * 512, // 512 KB
	}, nil
}
