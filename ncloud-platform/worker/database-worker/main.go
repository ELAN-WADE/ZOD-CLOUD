package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"

	"github.com/ncloud/platform/adapters/mesh"
)

type DatabaseProvisionEvent struct {
	ProjectID string `json:"project_id"`
	DBType    string `json:"db_type"` // "postgres", "mysql", "redis"
}

func main() {
	log.Println("Starting NCloud Database Worker (Phase 8)...")

	eventBus := mesh.NewMeshEventBus("global-mesh")

	unsub, err := eventBus.Subscribe(context.Background(), "database.provision", func(ctx context.Context, payload []byte) error {
		var event DatabaseProvisionEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			log.Printf("Failed to unmarshal event: %v", err)
			return err
		}

		log.Printf("[DatabaseWorker] Provisioning %s database for project %s...", event.DBType, event.ProjectID)

		if event.DBType != "postgres" {
			log.Printf("[DatabaseWorker] Only postgres is supported right now")
			return nil
		}

		containerName := fmt.Sprintf("ncloud-db-%s", event.ProjectID)
		dbPassword := "ncloud_secure_pass" // In production, generate this dynamically
		
		// Run postgres container physically via Docker
		log.Printf("[DatabaseWorker] Running docker run -d --name %s postgres", containerName)
		cmd := exec.Command("docker", "run", "-d", "-P", 
			"--name", containerName, 
			"-e", fmt.Sprintf("POSTGRES_PASSWORD=%s", dbPassword), 
			"postgres:15-alpine")
		
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("[DatabaseWorker] Docker run failed: %v. Output: %s", err, string(out))
			return err
		}

		// Typically, we would publish a "database.ready" event with the connection string back to the dashboard/mesh
		log.Printf("[DatabaseWorker] Database successfully provisioned! Container ID: %s", string(out))
		
		return nil
	})

	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	defer unsub()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}
