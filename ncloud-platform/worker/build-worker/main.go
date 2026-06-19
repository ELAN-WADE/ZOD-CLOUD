package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"

	"github.com/ncloud/platform/adapters/localdisk"
	"github.com/ncloud/platform/adapters/mesh"
	"github.com/ncloud/platform/internal/domain"
	"github.com/ncloud/platform/internal/ports"
)

func main() {
	log.Println("Starting NCloud Real Build Worker (Phase 7)...")

	// 1. Initialize Adapters
	eventBus := mesh.NewMeshEventBus("global-mesh")
	objectStorage, err := localdisk.NewLocalDiskStorage("./ncloud-s3-bucket")
	if err != nil {
		log.Fatalf("Failed to initialize object storage: %v", err)
	}

	// 2. Subscribe to "deployment.created"
	unsub, err := eventBus.Subscribe(context.Background(), domain.EventDeploymentCreated, func(ctx context.Context, payload []byte) error {
		return handleDeploymentCreated(ctx, payload, objectStorage, eventBus)
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	defer unsub()

	log.Println("Worker listening for deployments.created events...")
	
	// Block until interrupted
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}

func handleDeploymentCreated(ctx context.Context, payload []byte, storage ports.ObjectStorage, bus ports.EventBus) error {
	var event domain.DeploymentCreatedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		log.Printf("Failed to unmarshal event: %v", err)
		return err
	}

	log.Printf("[BuildWorker] Starting REAL build for Deployment %s (Project: %s)", 
		event.DeploymentID, event.ProjectID)

	// Step 1: Download from Object Storage
	key := fmt.Sprintf("projects/%s/source.zip", event.ProjectID) // Assuming uploaded as source.zip
	readCloser, err := storage.Get(ctx, key)
	if err != nil {
		log.Printf("[BuildWorker] Failed to get source code: %v", err)
		bus.Publish(ctx, domain.BuildFailedEvent{
			BaseEvent: domain.BaseEvent{DeploymentID: event.DeploymentID, ProjectID: event.ProjectID},
			Error:     fmt.Sprintf("Failed to get source code: %v", err),
		})
		return err
	}
	defer readCloser.Close()

	// Step 2: Extract Zip to temp dir
	tmpDir, err := os.MkdirTemp("", "ncloud-build-*")
	if err != nil {
		bus.Publish(ctx, domain.BuildFailedEvent{
			BaseEvent: domain.BaseEvent{DeploymentID: event.DeploymentID, ProjectID: event.ProjectID},
			Error:     fmt.Sprintf("Failed to create temp dir: %v", err),
		})
		return err
	}
	defer os.RemoveAll(tmpDir)

	zipPath := filepath.Join(tmpDir, "source.zip")
	outFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	_, err = io.Copy(outFile, readCloser)
	outFile.Close()
	if err != nil {
		return err
	}

	err = extractZip(zipPath, tmpDir)
	if err != nil {
		log.Printf("[BuildWorker] Failed to extract zip: %v", err)
		bus.Publish(ctx, domain.BuildFailedEvent{
			BaseEvent: domain.BaseEvent{DeploymentID: event.DeploymentID, ProjectID: event.ProjectID},
			Error:     fmt.Sprintf("Failed to extract zip: %v", err),
		})
		return err
	}

	imageName := fmt.Sprintf("ncloud-project-%s:%s", event.ProjectID, event.DeploymentID)
	log.Printf("[BuildWorker] Running nixpacks (via Docker container) build /app --name %s", imageName)
	
	nixpacksPath := filepath.Join(".", "bin", "nixpacks.exe")
	if _, err := os.Stat(nixpacksPath); os.IsNotExist(err) {
		log.Println("[BuildWorker] nixpacks.exe not found. Downloading from GitHub...")
		os.MkdirAll(filepath.Dir(nixpacksPath), os.ModePerm)
		resp, err := http.Get("https://github.com/railwayapp/nixpacks/releases/download/v1.31.0/nixpacks-v1.31.0-x86_64-pc-windows-msvc.zip")
		if err != nil {
			return fmt.Errorf("failed to download nixpacks: %w", err)
		}
		defer resp.Body.Close()
		zipPath := filepath.Join(".", "bin", "nixpacks.zip")
		outFile, err := os.Create(zipPath)
		if err != nil {
			return err
		}
		_, err = io.Copy(outFile, resp.Body)
		outFile.Close()
		if err != nil {
			return err
		}
		err = extractZip(zipPath, filepath.Join(".", "bin"))
		if err != nil {
			return err
		}
		os.Remove(zipPath)
	}

	args := []string{"build", tmpDir, "--name", imageName}
	
	// Intelligent framework detection & patching (handling ZIP subdirectories recursively)
	_ = filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && info.Name() == "package.json" {
			// It's a Node.js project. We automatically inject a nixpacks.toml 
			// to magically fix the notorious Webpack/Next.js ERR_OSSL_EVP_UNSUPPORTED error on Node 17+
			tomlPath := filepath.Join(filepath.Dir(path), "nixpacks.toml")
			if _, err := os.Stat(tomlPath); os.IsNotExist(err) {
				_ = os.WriteFile(tomlPath, []byte("[variables]\nNODE_OPTIONS = \"--openssl-legacy-provider\"\n"), 0644)
			}
			return filepath.SkipAll
		}
		return nil
	})

	cmd := exec.Command(nixpacksPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		log.Printf("[BuildWorker] Docker build FAILED: %v", err)
		bus.Publish(ctx, domain.BuildFailedEvent{
			BaseEvent: domain.BaseEvent{DeploymentID: event.DeploymentID, ProjectID: event.ProjectID},
			Error:     fmt.Sprintf("Docker build FAILED: %v", err),
		})
		return err
	}

	log.Printf("[BuildWorker] Successfully built image %s", imageName)

	// Publish a "builds.completed" event so the DeployWorker can pick it up.
	completedEvent := domain.BuildCompletedEvent{
		BaseEvent: domain.BaseEvent{
			DeploymentID: event.DeploymentID,
			ProjectID:    event.ProjectID,
		},
		ImageName: imageName,
	}
	
	log.Printf("[BuildWorker] Triggering Deployment Worker via builds.completed event...")
	
	err = bus.Publish(ctx, completedEvent)
	if err != nil {
		log.Printf("[BuildWorker] Failed to publish builds.completed: %v", err)
	}

	return nil
}

func extractZip(zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		fpath := filepath.Join(destDir, f.Name)
		if f.FileInfo().IsDir() {
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}
		
		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return err
		}

		_, err = io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()

		if err != nil {
			return err
		}
	}
	return nil
}
