package docker

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"

	"github.com/ncloud/platform/internal/ports"
)

// LocalDockerScheduler is a Phase 1 implementation of Scheduler.
// It executes local Docker CLI commands.
// It allows us to simulate Kubernetes behavior on a single dev machine.
type LocalDockerScheduler struct{}

func NewLocalDockerScheduler() *LocalDockerScheduler {
	return &LocalDockerScheduler{}
}

// Deploy spins up a Docker container based on the spec.
func (s *LocalDockerScheduler) Deploy(ctx context.Context, spec ports.DeploymentSpec) error {
	// Construct the docker run command
	// e.g., docker run -d --name dep_xxx -m 512m --cpus 0.25 image_name
	
	args := []string{
		"run", "-d", "-P",
		"--name", spec.DeploymentID,
	}

	// Add environment variables
	for k, v := range spec.EnvVars {
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
	}

	// Add the image
	args = append(args, spec.Image)

	cmd := exec.CommandContext(ctx, "docker", args...)
	
	// Execute the command
	// In reality we'd parse output to confirm it's running
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to deploy to docker: %w", err)
	}

	return nil
}

func (s *LocalDockerScheduler) Scale(ctx context.Context, deploymentID string, replicas int) error {
	// Local docker doesn't do "replicas" easily without compose/swarm.
	// For prototype, we might just ignore or start multiple containers.
	return fmt.Errorf("scale not implemented in local docker scheduler")
}

func (s *LocalDockerScheduler) Stop(ctx context.Context, deploymentID string) error {
	cmd := exec.CommandContext(ctx, "docker", "rm", "-f", deploymentID)
	return cmd.Run()
}

func (s *LocalDockerScheduler) GetLogs(ctx context.Context, deploymentID string, tail int) ([]ports.LogLine, error) {
	cmd := exec.CommandContext(ctx, "docker", "logs", "--tail", strconv.Itoa(tail), deploymentID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	
	// Simplistic parsing for prototype
	return []ports.LogLine{
		{Timestamp: "now", Message: string(out)},
	}, nil
}

func (s *LocalDockerScheduler) GetMetrics(ctx context.Context, deploymentID string) (ports.Metrics, error) {
	// docker stats --no-stream ...
	return ports.Metrics{CPUUsagePercentage: 10.5, MemoryUsageBytes: 1024 * 1024 * 50}, nil
}
