package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/ncloud/platform/adapters/edgekv"
	"github.com/ncloud/platform/adapters/localdisk"
	"github.com/ncloud/platform/adapters/mesh"
	"github.com/ncloud/platform/adapters/spanner"
	"github.com/ncloud/platform/adapters/websocket"
	"github.com/ncloud/platform/internal/domain"
	"github.com/ncloud/platform/internal/services"
)

func main() {
	log.Println("Starting NCloud Multi-Modal API Gateway (Phase 5)...")

	// --- Phase 4 (Core/Stateful) Setup ---
	coreEventBus := mesh.NewMeshEventBus("global-mesh")
	spannerDB := spanner.NewSpannerDatabase()
	coreRepo := spanner.NewSpannerProjectRepository(spannerDB)
	coreDeployRepo := spanner.NewSpannerDeploymentRepository(spannerDB)

	// Pre-seed core DB
	_ = coreRepo.Create(context.Background(), &domain.Project{
		ID:      "proj_core",
		Name:    "Core Project",
		OwnerID: "team_zzz",
	})
	coreDeploySvc := services.NewDeploymentService(coreRepo, coreDeployRepo, coreEventBus)

	// --- Phase 5 (Edge/Serverless) Setup ---
	edgeEventBus := websocket.NewWebSocketEventBus()
	edgeRepo := edgekv.NewEdgeKVProjectRepository("global-edge")
	edgeDeployRepo := edgekv.NewEdgeKVDeploymentRepository(edgeRepo)

	// Pre-seed edge DB
	_ = edgeRepo.Create(context.Background(), &domain.Project{
		ID:      "proj_edge",
		Name:    "Edge Project",
		OwnerID: "team_zzz",
	})
	edgeDeploySvc := services.NewDeploymentService(edgeRepo, edgeDeployRepo, edgeEventBus)

	// --- Object Storage Setup ---
	objectStorage, err := localdisk.NewLocalDiskStorage("./ncloud-s3-bucket")
	if err != nil {
		log.Fatalf("Failed to initialize object storage: %v", err)
	}

	// 3. Setup HTTP Routes
	
	// Upload Endpoint (Source Ingestion)
	http.HandleFunc("/api/v1/projects/upload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		projectID := r.URL.Query().Get("project_id")
		if projectID == "" {
			http.Error(w, "project_id query param is required", http.StatusBadRequest)
			return
		}

		// Parse the multipart form (max 50MB)
		err := r.ParseMultipartForm(50 << 20)
		if err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}

		file, _, err := r.FormFile("source")
		if err != nil {
			http.Error(w, "Missing 'source' file in form", http.StatusBadRequest)
			return
		}
		defer file.Close()

		// Key format: projects/{projectID}/source.zip
		key := fmt.Sprintf("projects/%s/source.zip", projectID)
		err = objectStorage.Put(r.Context(), key, file)
		if err != nil {
			http.Error(w, "Failed to store file", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status": "uploaded", "key": "%s"}`, key)
	})

	// Deployment Endpoint
	http.HandleFunc("/api/v1/deployments/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Read workload type from query param, e.g. ?type=edge
		workloadType := r.URL.Query().Get("type")
		projectID := "proj_core"
		
		svc := coreDeploySvc

		if workloadType == "edge" {
			svc = edgeDeploySvc
			projectID = "proj_edge"
			log.Println("[API] Routing request to Phase 5 Edge architecture")
		} else {
			log.Println("[API] Routing request to Phase 4 Core architecture")
		}

		dep, err := svc.TriggerDeployment(r.Context(), projectID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"status": "accepted", "deployment_id": "%s", "architecture": "%s"}`, dep.ID, workloadType)
	})

	log.Println("Listening on :8088...")
	log.Fatal(http.ListenAndServe(":8088", nil))
}
