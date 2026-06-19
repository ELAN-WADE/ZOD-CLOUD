package main

import (
	"archive/zip"
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"

	"github.com/go-git/go-git/v5"
	"github.com/gorilla/websocket"

	"github.com/ncloud/platform/adapters/docker"
	"github.com/ncloud/platform/adapters/edgekv"
	"github.com/ncloud/platform/adapters/localdisk"
	"github.com/ncloud/platform/adapters/mesh"
	"github.com/ncloud/platform/adapters/sqlite"
	"github.com/ncloud/platform/adapters/tunnel"
	ncloudws "github.com/ncloud/platform/adapters/websocket"
	"github.com/ncloud/platform/internal/domain"
	"github.com/ncloud/platform/internal/ports"
	"github.com/ncloud/platform/internal/services"

	_ "modernc.org/sqlite"
)

// ─────────────────────────────────────────────────────────────────────────────
// DOMAIN EVENTS
// ─────────────────────────────────────────────────────────────────────────────

type DatabaseProvisionEvent struct {
	ProjectID string `json:"project_id"`
	DBType    string `json:"db_type"`
}

func (e DatabaseProvisionEvent) EventName() string { return "database.provision" }

type DeploymentReadyEvent struct {
	DeploymentID string `json:"deployment_id"`
	URL          string `json:"url"`
	Named        bool   `json:"named"`
	ProjectName  string `json:"project_name"`
}

func (e DeploymentReadyEvent) EventName() string { return "deployments.ready" }

// ─────────────────────────────────────────────────────────────────────────────
// LOG INFRASTRUCTURE
// ─────────────────────────────────────────────────────────────────────────────

// LogType categorizes the origin of a log entry.
type LogType = domain.LogType

const (
	LogTypeBuild   = domain.LogTypeBuild
	LogTypeDeploy  = domain.LogTypeDeploy
	LogTypeHTTP    = domain.LogTypeHTTP
	LogTypeNetwork = domain.LogTypeNetwork
	LogTypeVolume  = domain.LogTypeVolume
	LogTypeSystem  = domain.LogTypeSystem
	LogTypeMetrics = domain.LogTypeMetrics
)

// LogLevel represents severity.
type LogLevel = domain.LogLevel

const (
	LogLevelInfo  = domain.LogLevelInfo
	LogLevelWarn  = domain.LogLevelWarn
	LogLevelError = domain.LogLevelError
	LogLevelDebug = domain.LogLevelDebug
)

// LogEntry is the core logging structure written to SQLite and streamed via SSE.
type LogEntry = domain.LogEntry

// SSEBroker manages Server-Sent Events subscriptions per deployment.
type SSEBroker struct {
	mu          sync.RWMutex
	subscribers map[string][]chan LogEntry // key: deploymentID
}

func newSSEBroker() *SSEBroker {
	return &SSEBroker{
		subscribers: make(map[string][]chan LogEntry),
	}
}

func (b *SSEBroker) Subscribe(deploymentID string) chan LogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan LogEntry, 256)
	b.subscribers[deploymentID] = append(b.subscribers[deploymentID], ch)
	return ch
}

func (b *SSEBroker) Unsubscribe(deploymentID string, ch chan LogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subs := b.subscribers[deploymentID]
	for i, s := range subs {
		if s == ch {
			b.subscribers[deploymentID] = append(subs[:i], subs[i+1:]...)
			close(ch)
			return
		}
	}
}

func (b *SSEBroker) Broadcast(entry LogEntry) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subscribers[entry.DeploymentID] {
		select {
		case ch <- entry:
		default:
			// Drop if consumer is too slow
		}
	}
	// Also broadcast to "all" channel
	for _, ch := range b.subscribers["all"] {
		select {
		case ch <- entry:
		default:
		}
	}
}

// Global log DB handle and SSE broker
var (
	globalDB        *sql.DB
	sseBroker       = newSSEBroker()
	publicURLs      = make(map[string]string)
	publicURLsMu    sync.RWMutex
	publicURLsNamed = make(map[string]bool)
	internalPorts   = make(map[string]string) // key: ProjectID (subdomain), value: local port
)

// writeLog persists a log entry to SQLite and broadcasts it over SSE.
func writeLog(deploymentID string, logType LogType, level LogLevel, message string) {
	entry := LogEntry{
		DeploymentID: deploymentID,
		LogType:      logType,
		Level:        level,
		Message:      message,
		Timestamp:    time.Now().UTC(),
	}

	if globalDB != nil {
		res, err := globalDB.Exec(
			`INSERT INTO logs (deployment_id, log_type, level, message, timestamp) VALUES (?, ?, ?, ?, ?)`,
			deploymentID, string(logType), string(level), message, entry.Timestamp,
		)
		if err != nil {
			log.Printf("[LogWriter] Failed to write log: %v", err)
		} else {
			entry.ID, _ = res.LastInsertId()
		}
	}

	sseBroker.Broadcast(entry)
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP LOGGING MIDDLEWARE
// ─────────────────────────────────────────────────────────────────────────────

type responseWriter struct {
	http.ResponseWriter
	status int
	size   int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.size += n
	return n, err
}

func (rw *responseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := rw.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("hijack not supported")
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lrw := &responseWriter{ResponseWriter: w, status: 200}

		next.ServeHTTP(lrw, r)

		duration := time.Since(start)
		ip := r.RemoteAddr
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			ip = xff
		}

		// Determine log level by status code
		level := LogLevelInfo
		if lrw.status >= 400 && lrw.status < 500 {
			level = LogLevelWarn
		} else if lrw.status >= 500 {
			level = LogLevelError
		}

		msg := fmt.Sprintf("%s %s %d %dms %s %dbytes",
			r.Method, r.URL.Path, lrw.status,
			duration.Milliseconds(), ip, lrw.size)

		writeLog("system", LogTypeHTTP, level, msg)
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	log.Println("🚀 Starting ZOD CLOUD Platform (Standalone Single-Node Mode)...")

	// 1. Open Database FIRST so logging works from the start
	db, err := sql.Open("sqlite", "./ncloud.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		log.Fatalf("Failed to open SQLite database: %v", err)
	}
	globalDB = db
	initSchema(db)

	// Restore in-memory URL map from DB (so deployments survive server restarts)
	{
		rows, err := db.Query(`SELECT id, COALESCE(public_url,'') FROM deployments WHERE status = 'running' AND public_url != ''`)
		if err == nil {
			for rows.Next() {
				var depID, pubURL string
				rows.Scan(&depID, &pubURL)
				if depID != "" && pubURL != "" {
					publicURLs[depID] = pubURL
					publicURLsNamed[depID] = true
				}
			}
			rows.Close()
			log.Printf("[Startup] Restored %d deployment URLs from database", len(publicURLs))
		}
	}

	// 2. Shared Infrastructure
	globalMesh := mesh.NewMeshEventBus("global-mesh")
	objectStorage, err := localdisk.NewLocalDiskStorage("./ncloud-s3-bucket")
	if err != nil {
		log.Fatalf("Failed to initialize object storage: %v", err)
	}
	scheduler := docker.NewLocalDockerScheduler()

	writeLog("system", LogTypeSystem, LogLevelInfo, "ZOD CLOUD Platform initialized. Build Worker starting...")

	// 3. Start Build Worker
	log.Println("[Standalone] Starting Build Worker...")
	_, err = globalMesh.Subscribe(context.Background(), domain.EventDeploymentCreated, func(ctx context.Context, payload []byte) error {
		return handleDeploymentCreated(ctx, payload, objectStorage, globalMesh)
	})
	if err != nil {
		log.Fatalf("Failed to start build worker: %v", err)
	}
	writeLog("system", LogTypeSystem, LogLevelInfo, "Build Worker subscribed to deployments.created")

	// 4. Start Deploy Worker
	log.Println("[Standalone] Starting Deploy Worker...")
	_, err = globalMesh.Subscribe(context.Background(), domain.EventBuildCompleted, func(ctx context.Context, payload []byte) error {
		var event domain.BuildCompletedEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			writeLog(event.DeploymentID, LogTypeDeploy, LogLevelError, fmt.Sprintf("Failed to unmarshal build event: %v", err))
			return err
		}

		writeLog(event.DeploymentID, LogTypeDeploy, LogLevelInfo, fmt.Sprintf("Build complete. Deploying image: %s", event.ImageName))
		log.Printf("[DeployWorker] Received build.completed for %s. Triggering Docker...", event.DeploymentID)
		if globalDB != nil {
			globalDB.Exec("UPDATE deployments SET status = 'deploying' WHERE id = ?", event.DeploymentID)
		}

		// Fire deployment.started
		_ = globalMesh.Publish(ctx, domain.DeploymentStartedEvent{
			BaseEvent: domain.BaseEvent{DeploymentID: event.DeploymentID, ProjectID: event.ProjectID, Timestamp: time.Now().UTC()},
		})

		spec := ports.DeploymentSpec{
			DeploymentID: event.DeploymentID,
			Image:        event.ImageName,
		}

		if err := scheduler.Deploy(ctx, spec); err != nil {
			writeLog(event.DeploymentID, LogTypeDeploy, LogLevelError, fmt.Sprintf("Docker deployment FAILED: %v", err))
			log.Printf("[DeployWorker] Deployment FAILED: %v", err)
			return err
		}

		writeLog(event.DeploymentID, LogTypeDeploy, LogLevelInfo, fmt.Sprintf("Container deployed successfully: %s", event.DeploymentID))
		log.Printf("[DeployWorker] Successfully deployed %s via Docker Scheduler!", event.DeploymentID)

		// Get mapped port — some containers may not expose a port (static sites)
		cmdPort := exec.Command("docker", "port", event.DeploymentID)
		portOut, err := cmdPort.Output()
		localPort := ""
		if err != nil {
			writeLog(event.DeploymentID, LogTypeNetwork, LogLevelWarn, fmt.Sprintf("No port mapping found (may be a static site): %v", err))
		} else {
			// Parse port (e.g. "3000/tcp -> 0.0.0.0:32768")
			portStr := strings.TrimSpace(string(portOut))
			writeLog(event.DeploymentID, LogTypeNetwork, LogLevelInfo, fmt.Sprintf("Container port mapping: %s", portStr))
			parts := strings.Split(portStr, ":")
			if len(parts) >= 2 {
				localPort = strings.TrimSpace(parts[len(parts)-1])
			}
		}

		publicURL := ""
		if localPort != "" {
			// Start Cloudflare Tunnel
			writeLog(event.DeploymentID, LogTypeNetwork, LogLevelInfo, fmt.Sprintf("Starting Cloudflare tunnel for port %s (project: %s)...", localPort, event.ProjectID))
			cf := tunnel.NewCloudflareTunnel()
			result, tunnelErr := cf.StartTunnel(localPort, event.ProjectID)
			if tunnelErr != nil {
				writeLog(event.DeploymentID, LogTypeNetwork, LogLevelWarn, fmt.Sprintf("Tunnel failed (deployment still running locally): %v", tunnelErr))
				log.Printf("[DeployWorker] Tunnel failed: %v — marking as running without public URL", tunnelErr)
				// Deployment is still running locally even without tunnel
				publicURL = fmt.Sprintf("http://localhost:%s", localPort)
			} else {
				writeLog(event.DeploymentID, LogTypeNetwork, LogLevelInfo, fmt.Sprintf("✅ Tunnel established: %s", result.PublicURL))
				log.Printf("[DeployWorker] LIVE! Public URL: %s", result.PublicURL)
				publicURL = result.PublicURL
				publicURLsMu.Lock()
				publicURLs[event.DeploymentID] = result.PublicURL
				publicURLsNamed[event.DeploymentID] = true
				internalPorts[event.ProjectID] = localPort
				publicURLsMu.Unlock()
			}
		} else {
			writeLog(event.DeploymentID, LogTypeNetwork, LogLevelInfo, "Container has no port mapping — marking as running")
			publicURL = ""
		}

		internalURL := ""
		if localPort != "" {
			internalURL = "http://127.0.0.1:" + localPort
		}

		// Fire DomainAssignedEvent
		_ = globalMesh.Publish(ctx, domain.DomainAssignedEvent{
			BaseEvent:   domain.BaseEvent{DeploymentID: event.DeploymentID, ProjectID: event.ProjectID, Timestamp: time.Now().UTC()},
			InternalURL: internalURL,
			PublicURL:   publicURL,
			TunnelID:    "",
		})

		// Fire DeploymentRunningEvent
		_ = globalMesh.Publish(ctx, domain.DeploymentRunningEvent{
			BaseEvent: domain.BaseEvent{DeploymentID: event.DeploymentID, ProjectID: event.ProjectID, Timestamp: time.Now().UTC()},
		})

		// Update Database with ALL fields
		if globalDB != nil {
			_, dbErr := globalDB.Exec(`
				UPDATE deployments 
				SET status = 'running', 
					image_name = ?, 
					container_id = ?, 
					public_url = ?, 
					internal_url = ?, 
					tunnel_id = ? 
				WHERE id = ?`,
				event.ImageName, event.DeploymentID, publicURL, internalURL, "", event.DeploymentID,
			)
			if dbErr != nil {
				log.Printf("[DB] Failed to update full deployment record: %v", dbErr)
			}
			// Also update in-memory map from DB
			if publicURL != "" {
				publicURLsMu.Lock()
				publicURLs[event.DeploymentID] = publicURL
				publicURLsMu.Unlock()
			}
		}

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to start deploy worker: %v", err)
	}

	// 5. Start Database Worker
	log.Println("[Standalone] Starting Database Worker...")
	_, err = globalMesh.Subscribe(context.Background(), "database.provision", func(ctx context.Context, payload []byte) error {
		var event DatabaseProvisionEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return err
		}
		writeLog(event.ProjectID, LogTypeDeploy, LogLevelInfo, fmt.Sprintf("Provisioning %s database for project %s...", event.DBType, event.ProjectID))
		log.Printf("[DatabaseWorker] Provisioning %s database for project %s...", event.DBType, event.ProjectID)

		containerName := fmt.Sprintf("ncloud-db-%s", event.ProjectID)
		cmd := exec.Command("docker", "run", "-d", "-P", "--name", containerName,
			"-e", "POSTGRES_PASSWORD=ncloud_secure_pass", "postgres:15-alpine")
		if err := cmd.Run(); err != nil {
			writeLog(event.ProjectID, LogTypeDeploy, LogLevelError, fmt.Sprintf("Failed to provision database: %v", err))
			log.Printf("[DatabaseWorker] Failed to provision: %v", err)
			if globalDB != nil {
				globalDB.Exec("UPDATE databases SET status = 'Failed' WHERE name = ?", event.ProjectID)
			}
			return err
		}
		writeLog(event.ProjectID, LogTypeDeploy, LogLevelInfo, "PostgreSQL container provisioned successfully on port 5432")
		log.Printf("[DatabaseWorker] Database successfully provisioned!")
		if globalDB != nil {
			// Typically, you would run docker port here to get the mapped port.
			// Since postgres always uses 5432 internally, we'll mark it as running.
			globalDB.Exec("UPDATE databases SET status = 'Running', ports = '5432/tcp' WHERE name = ?", event.ProjectID)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to start database worker: %v", err)
	}

	// 6. Repository & Service Setup
	coreRepo := sqlite.NewSQLiteProjectRepository(db)
	coreDeployRepo := sqlite.NewSQLiteDeploymentRepository(db)
	_ = coreRepo.Create(context.Background(), &domain.Project{ID: "proj_core", Name: "Core Project"})
	coreDeploySvc := services.NewDeploymentService(coreRepo, coreDeployRepo, globalMesh)

	edgeEventBus := ncloudws.NewWebSocketEventBus()
	edgeRepo := edgekv.NewEdgeKVProjectRepository("global-edge")
	edgeDeployRepo := edgekv.NewEdgeKVDeploymentRepository(edgeRepo)
	_ = edgeRepo.Create(context.Background(), &domain.Project{ID: "proj_edge", Name: "Edge Project"})
	edgeDeploySvc := services.NewDeploymentService(edgeRepo, edgeDeployRepo, edgeEventBus)

	// Start Metrics Polling Loop
	go func() {
		for {
			time.Sleep(3 * time.Second) // Poll faster for the UI
			cmd := exec.Command("docker", "stats", "--no-stream", "--format", `{"container":"{{.Container}}","cpu":"{{.CPUPerc}}","mem":"{{.MemUsage}}"}`)
			out, err := cmd.Output()
			if err == nil {
				lines := strings.Split(strings.TrimSpace(string(out)), "\n")
				for _, line := range lines {
					if line == "" { continue }
					// For simplicity, broadcast to "all" deployments for the metrics demo
					writeLog("all", LogTypeMetrics, LogLevelInfo, line)
				}
			}
		}
	}()

	// Note: We removed the `deployments.ready` subscriber because DeployWorker now updates the DB directly and fires domain.DeploymentRunningEvent.

	// ─────────────────────────────────────────────────────────────────────────
	// API ROUTES
	// ─────────────────────────────────────────────────────────────────────────

	mux := http.NewServeMux()

	// --- Health Check ---
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// Check DB
		dbStatus := "ok"
		if err := db.Ping(); err != nil {
			dbStatus = fmt.Sprintf("error: %v", err)
		}

		// Count deployments
		var depCount int
		db.QueryRow("SELECT COUNT(*) FROM deployments").Scan(&depCount)

		resp := map[string]interface{}{
			"status":      "healthy",
			"version":     "1.0.0",
			"platform":    "ZOD CLOUD",
			"timestamp":   time.Now().UTC(),
			"db":          dbStatus,
			"deployments": depCount,
			"uptime":      time.Since(startTime).String(),
		}
		json.NewEncoder(w).Encode(resp)
	})


	// --- Trigger Deployment ---
	mux.HandleFunc("/api/v1/deployments/", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		// Route sub-paths
		path := r.URL.Path
		if strings.HasSuffix(path, "/status") {
			handleDeploymentStatus(w, r)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		workloadType := r.URL.Query().Get("type")
		projectID := r.URL.Query().Get("project_id")
		if projectID == "" {
			projectID = "proj_core"
		}
		region, svc := "lagos", coreDeploySvc
		if workloadType == "edge" {
			svc, region = edgeDeploySvc, "edge-global"
			if r.URL.Query().Get("project_id") == "" {
				projectID = "proj_edge"
			}
		}

		// Ensure project exists in DB before triggering deployment
		if _, err := coreRepo.GetByID(r.Context(), projectID); err != nil {
			_ = coreRepo.Create(r.Context(), &domain.Project{ID: projectID, Name: projectID})
		}

		dep, err := svc.TriggerDeployment(r.Context(), projectID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeLog(dep.ID, LogTypeDeploy, LogLevelInfo, fmt.Sprintf("Deployment triggered for project %s in region %s", projectID, region))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"status": "accepted", "deployment_id": "%s", "architecture": "%s"}`, dep.ID, workloadType)
	})

	// --- Upload Endpoint ---
	mux.HandleFunc("/api/v1/projects/upload", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		projectID := r.URL.Query().Get("project_id")
		if projectID == "" {
			http.Error(w, "project_id query param is required", http.StatusBadRequest)
			return
		}

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

		key := fmt.Sprintf("projects/%s/source.zip", projectID)
		err = objectStorage.Put(r.Context(), key, file)
		if err != nil {
			http.Error(w, "Failed to store file", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status": "uploaded", "key": "%s"}`, key)
	})

	// --- Deployment Status ---
	mux.HandleFunc("/api/v1/deployments/status", handleDeploymentStatus)

	// --- Deployment Rollback ---
	mux.HandleFunc("/api/v1/deployments/rollback", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		depID := r.URL.Query().Get("deployment_id")
		writeLog(depID, LogTypeSystem, LogLevelInfo, "Rollback initiated. Restarting previous container...")
		exec.Command("docker", "restart", depID).Run()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status": "rollback_initiated"}`)
	})

	// --- List & Delete Deployments ---
	mux.HandleFunc("/api/v1/deployments", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		if r.Method == http.MethodDelete {
			id := r.URL.Query().Get("id")
			if id != "" {
				exec.Command("docker", "rm", "-f", id).Run()
				db.Exec("DELETE FROM deployments WHERE id = ?", id)
				db.Exec("DELETE FROM logs WHERE deployment_id = ?", id)
				publicURLsMu.Lock()
				delete(publicURLs, id)
				delete(publicURLsNamed, id)
				publicURLsMu.Unlock()
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status": "deleted"}`)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		// Read public_url directly from DB (survives server restarts)
		query := `SELECT id, project_id, status, created_at, COALESCE(public_url,'') FROM deployments ORDER BY created_at DESC LIMIT 50`
		rows, err := db.QueryContext(r.Context(), query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type DepResponse struct {
			ID        string `json:"id"`
			ProjectID string `json:"project_id"`
			State     string `json:"status"`
			CreatedAt string `json:"created_at"`
			URL       string `json:"public_url"`
			Named     bool   `json:"named_url"`
		}
		var deps []DepResponse
		for rows.Next() {
			var d DepResponse
			rows.Scan(&d.ID, &d.ProjectID, &d.State, &d.CreatedAt, &d.URL)
			// Also check in-memory map for live deployments (may be more up to date)
			publicURLsMu.RLock()
			if liveURL := publicURLs[d.ID]; liveURL != "" && d.URL == "" {
				d.URL = liveURL
			}
			d.Named = publicURLsNamed[d.ID]
			publicURLsMu.RUnlock()
			deps = append(deps, d)
		}
		if deps == nil {
			deps = []DepResponse{}
		}
		json.NewEncoder(w).Encode(deps)
	})

	// --- List Databases ---
	mux.HandleFunc("/api/v1/databases", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		
		cmd := exec.Command("docker", "ps", "--filter", "name=ncloud-db-", "--format", `{"id":"{{.ID}}", "name":"{{.Names}}","status":"{{.Status}}","ports":"{{.Ports}}"}`)
		out, err := cmd.Output()
		if err != nil {
			http.Error(w, `[]`, http.StatusInternalServerError)
			return
		}
		
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		w.Write([]byte("["))
		first := true
		for _, line := range lines {
			if line == "" { continue }
			if !first { w.Write([]byte(",")) }
			first = false
			w.Write([]byte(line))
		}
		w.Write([]byte("]"))
	})

	// --- Database Provision ---
	mux.HandleFunc("/api/v1/databases/provision", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		projectID := r.URL.Query().Get("project_id")
		if projectID == "" {
			projectID = "proj_core"
		}
		event := DatabaseProvisionEvent{ProjectID: projectID, DBType: "postgres"}
		_ = globalMesh.Publish(r.Context(), event)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"status": "provisioning", "db_type": "postgres", "project_id": "%s"}`, projectID)
	})

	// --- Web Terminal WebSocket Proxy ---
	var upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	mux.HandleFunc("/api/v1/deployments/terminal", func(w http.ResponseWriter, r *http.Request) {
		depID := r.URL.Query().Get("deployment_id")
		if depID == "" {
			http.Error(w, "missing deployment_id", http.StatusBadRequest)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WebSocket upgrade error:", err)
			return
		}
		defer conn.Close()

		// Run docker exec
		cmd := exec.Command("docker", "exec", "-i", depID, "/bin/sh")
		stdin, _ := cmd.StdinPipe()
		stdout, _ := cmd.StdoutPipe()
		stderr, _ := cmd.StderrPipe()

		if err := cmd.Start(); err != nil {
			conn.WriteMessage(websocket.TextMessage, []byte("Error starting terminal: "+err.Error()))
			return
		}

		// Read from WebSocket -> Write to Docker Stdin
		go func() {
			for {
				_, msg, err := conn.ReadMessage()
				if err != nil {
					break
				}
				stdin.Write(msg)
			}
			stdin.Close()
		}()

		// Read from Docker Stdout -> Write to WebSocket
		go func() {
			buf := make([]byte, 1024)
			for {
				n, err := stdout.Read(buf)
				if n > 0 {
					conn.WriteMessage(websocket.TextMessage, buf[:n])
				}
				if err != nil {
					break
				}
			}
		}()

		// Read from Docker Stderr -> Write to WebSocket
		go func() {
			buf := make([]byte, 1024)
			for {
				n, err := stderr.Read(buf)
				if n > 0 {
					conn.WriteMessage(websocket.TextMessage, buf[:n])
				}
				if err != nil {
					break
				}
			}
		}()

		cmd.Wait()
		conn.WriteMessage(websocket.TextMessage, []byte("\r\n[Process Exited]\r\n"))
	})

	// --- GitHub Webhook ---
	mux.HandleFunc("/api/v1/webhooks/github", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Verify signature if secret is set
		secret := os.Getenv("GITHUB_WEBHOOK_SECRET")
		if secret != "" {
			signature := r.Header.Get("X-Hub-Signature-256")
			bodyBytes, _ := io.ReadAll(r.Body)
			
			mac := hmac.New(sha256.New, []byte(secret))
			mac.Write(bodyBytes)
			expectedMAC := hex.EncodeToString(mac.Sum(nil))
			
			if !strings.HasSuffix(signature, expectedMAC) {
				http.Error(w, "Invalid signature", http.StatusUnauthorized)
				return
			}
			// Restore body
			r.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))
		}

		var payload struct {
			Ref        string `json:"ref"`
			Repository struct {
				Name     string `json:"name"`
				CloneURL string `json:"clone_url"`
			} `json:"repository"`
			HeadCommit struct {
				ID string `json:"id"`
			} `json:"head_commit"`
		}

		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		projectID := payload.Repository.Name
		commitSHA := payload.HeadCommit.ID
		if commitSHA == "" {
			commitSHA = "latest"
		}

		writeLog(projectID, LogTypeSystem, LogLevelInfo, fmt.Sprintf("Received GitHub push event for %s (commit: %s)", projectID, commitSHA))

		// We need to trigger the deploy worker. Instead of passing an S3 zip, we'll clone it.
		// For simplicity, we clone it, zip it, and put it in S3, then trigger standard build.
		go func() {
			tmpDir, _ := os.MkdirTemp("", "git-clone-*")
			defer os.RemoveAll(tmpDir)

			writeLog(projectID, LogTypeBuild, LogLevelInfo, fmt.Sprintf("Cloning repo %s...", payload.Repository.CloneURL))
			_, err := git.PlainClone(tmpDir, false, &git.CloneOptions{
				URL:      payload.Repository.CloneURL,
				Progress: os.Stdout,
			})
			if err != nil {
				writeLog(projectID, LogTypeBuild, LogLevelError, fmt.Sprintf("Git clone failed: %v", err))
				return
			}

			// Zip it
			zipPath := filepath.Join(os.TempDir(), fmt.Sprintf("%s-source.zip", projectID))
			zipFile, _ := os.Create(zipPath)
			archive := zip.NewWriter(zipFile)
			filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
				if info.IsDir() { return nil }
				rel, _ := filepath.Rel(tmpDir, path)
				if strings.HasPrefix(rel, ".git") { return nil }
				w, _ := archive.Create(rel)
				f, _ := os.Open(path)
				io.Copy(w, f)
				f.Close()
				return nil
			})
			archive.Close()
			zipFile.Close()

			// Upload to S3
			f, _ := os.Open(zipPath)
			key := fmt.Sprintf("projects/%s/source.zip", projectID)
			_ = objectStorage.Put(context.Background(), key, f)
			f.Close()
			os.Remove(zipPath)

			// Trigger Deployment
			coreDeploySvc.TriggerDeployment(context.Background(), projectID)
		}()

		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"status": "accepted"}`)
	})

	// --- SSE Log Stream ---
	// GET /api/v1/logs/stream?deployment_id=X&type=build
	mux.HandleFunc("/api/v1/logs/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "SSE not supported", http.StatusInternalServerError)
			return
		}

		deploymentID := r.URL.Query().Get("deployment_id")
		if deploymentID == "" {
			deploymentID = "all"
		}
		logTypeFilter := LogType(r.URL.Query().Get("type"))

		// Send existing logs first (historical backfill)
		var histQuery string
		var histArgs []interface{}
		if deploymentID == "all" {
			histQuery = `SELECT id, deployment_id, log_type, level, message, timestamp FROM logs ORDER BY timestamp DESC LIMIT 200`
		} else {
			histQuery = `SELECT id, deployment_id, log_type, level, message, timestamp FROM logs WHERE deployment_id = ? ORDER BY timestamp DESC LIMIT 200`
			histArgs = []interface{}{deploymentID}
		}
		histRows, err := db.QueryContext(r.Context(), histQuery, histArgs...)
		if err == nil {
			var pastLogs []LogEntry
			for histRows.Next() {
				var e LogEntry
				var ts string
				histRows.Scan(&e.ID, &e.DeploymentID, &e.LogType, &e.Level, &e.Message, &ts)
				e.Timestamp, _ = time.Parse(time.RFC3339Nano, ts)
				if logTypeFilter != "" && e.LogType != logTypeFilter {
					continue
				}
				pastLogs = append(pastLogs, e)
			}
			histRows.Close() // EXPLICITLY CLOSE to release SQLite read lock before infinite loop!
			
			// Reverse back to chronological order (since we fetched DESC)
			for i := len(pastLogs)-1; i >= 0; i-- {
				data, _ := json.Marshal(pastLogs[i])
				fmt.Fprintf(w, "data: %s\n\n", data)
			}
			flusher.Flush()
		}

		// Subscribe for new events
		ch := sseBroker.Subscribe(deploymentID)
		defer sseBroker.Unsubscribe(deploymentID, ch)

		// Send heartbeat every 15s to keep connection alive
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case entry, ok := <-ch:
				if !ok {
					return
				}
				if logTypeFilter != "" && entry.LogType != logTypeFilter {
					continue
				}
				data, _ := json.Marshal(entry)
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			case <-ticker.C:
				fmt.Fprintf(w, ": heartbeat\n\n")
				flusher.Flush()
			}
		}
	})

	// --- Historical Logs Query ---
	// GET /api/v1/logs?deployment_id=X&type=build&limit=100&level=error
	mux.HandleFunc("/api/v1/logs", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")
		deploymentID := r.URL.Query().Get("deployment_id")
		logType := r.URL.Query().Get("type")
		level := r.URL.Query().Get("level")
		limit := 200

		conditions := []string{}
		args := []interface{}{}

		if deploymentID != "" && deploymentID != "all" {
			conditions = append(conditions, "deployment_id = ?")
			args = append(args, deploymentID)
		}
		if logType != "" {
			conditions = append(conditions, "log_type = ?")
			args = append(args, logType)
		}
		if level != "" {
			conditions = append(conditions, "level = ?")
			args = append(args, level)
		}

		query := "SELECT id, deployment_id, log_type, level, message, timestamp FROM logs"
		if len(conditions) > 0 {
			query += " WHERE " + strings.Join(conditions, " AND ")
		}
		query += fmt.Sprintf(" ORDER BY timestamp DESC LIMIT %d", limit)

		rows, err := db.QueryContext(r.Context(), query, args...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var logs []LogEntry
		for rows.Next() {
			var e LogEntry
			var ts string
			rows.Scan(&e.ID, &e.DeploymentID, &e.LogType, &e.Level, &e.Message, &ts)
			e.Timestamp, _ = time.Parse(time.RFC3339Nano, ts)
			logs = append(logs, e)
		}
		if logs == nil {
			logs = []LogEntry{}
		}
		json.NewEncoder(w).Encode(logs)
	})

	// --- Volume / Storage API ---
	// GET /api/v1/volumes
	mux.HandleFunc("/api/v1/volumes", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")

		bucketPath := "./ncloud-s3-bucket"
		type VolumeEntry struct {
			Key       string    `json:"key"`
			Size      int64     `json:"size"`
			ModTime   time.Time `json:"mod_time"`
			ProjectID string    `json:"project_id"`
		}

		var entries []VolumeEntry
		var totalSize int64

		filepath.Walk(bucketPath, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			relPath, _ := filepath.Rel(bucketPath, path)
			relPath = filepath.ToSlash(relPath)

			// Extract project ID from path "projects/<id>/..."
			projectID := "unknown"
			parts := strings.Split(relPath, "/")
			if len(parts) >= 2 && parts[0] == "projects" {
				projectID = parts[1]
			}

			entries = append(entries, VolumeEntry{
				Key:       relPath,
				Size:      info.Size(),
				ModTime:   info.ModTime(),
				ProjectID: projectID,
			})
			totalSize += info.Size()
			return nil
		})

		if entries == nil {
			entries = []VolumeEntry{}
		}

		resp := map[string]interface{}{
			"files":      entries,
			"count":      len(entries),
			"total_size": totalSize,
			"bucket":     "ncloud-s3-bucket",
		}
		json.NewEncoder(w).Encode(resp)
	})

	// --- Volume Stats ---
	// GET /api/v1/volumes/stats
	mux.HandleFunc("/api/v1/volumes/stats", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")
		var totalSize int64
		var fileCount int
		filepath.Walk("./ncloud-s3-bucket", func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				totalSize += info.Size()
				fileCount++
			}
			return nil
		})
		json.NewEncoder(w).Encode(map[string]interface{}{
			"total_bytes": totalSize,
			"total_mb":    fmt.Sprintf("%.2f", float64(totalSize)/(1024*1024)),
			"file_count":  fileCount,
			"limit_gb":    10,
			"used_pct":    fmt.Sprintf("%.4f", float64(totalSize)/(10*1024*1024*1024)*100),
		})
	})

	// --- Projects List ---
	mux.HandleFunc("/api/v1/projects", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")
		proj, err := coreRepo.GetByID(r.Context(), "proj_core")
		if err != nil {
			http.Error(w, `{"error": "not found"}`, http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode([]*domain.Project{proj})
	})

	// --- Auth Mock ---
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		var creds struct {
			Email string `json:"email"`
			Pass  string `json:"pass"`
		}
		json.NewDecoder(r.Body).Decode(&creds)

		if creds.Email == "demo@zod.cloud" && creds.Pass == "password123" {
			// In a real app, generate JWT and set HttpOnly cookie
			http.SetCookie(w, &http.Cookie{
				Name:     "zod_session",
				Value:    "mock_token_123",
				Path:     "/",
				HttpOnly: true,
				Expires:  time.Now().Add(24 * time.Hour),
			})
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status": "ok"}`)
			return
		}
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	})

	// --- Context (Teams) ---
	mux.HandleFunc("/api/v1/user/context", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		// Return mock user and team contexts from seed data
		type ContextData struct {
			Personal struct { ID string `json:"id"`; Name string `json:"name"` } `json:"personal"`
			Teams    []struct { ID string `json:"id"`; Name string `json:"name"` } `json:"teams"`
		}
		var data ContextData
		data.Personal.ID = "user_1"
		data.Personal.Name = "Personal Account"
		data.Teams = make([]struct { ID string `json:"id"`; Name string `json:"name"` }, 0)
		
		rows, err := db.QueryContext(r.Context(), "SELECT t.id, t.name FROM teams t JOIN team_members tm ON t.id = tm.team_id WHERE tm.user_id = 'user_1'")
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var id, name string
				rows.Scan(&id, &name)
				data.Teams = append(data.Teams, struct { ID string `json:"id"`; Name string `json:"name"` }{ID: id, Name: name})
			}
		}
		json.NewEncoder(w).Encode(data)
	})

	// --- Billing ---
	mux.HandleFunc("/api/v1/billing", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		contextID := r.URL.Query().Get("context_id")
		isTeam := r.URL.Query().Get("is_team") == "true"
		
		var plan, status string
		var query string
		if isTeam {
			query = "SELECT plan, status FROM billing_subscriptions WHERE team_id = ?"
		} else {
			query = "SELECT plan, status FROM billing_subscriptions WHERE user_id = ?"
		}
		
		err := db.QueryRowContext(r.Context(), query, contextID).Scan(&plan, &status)
		if err != nil {
			// Fallback mock if not found
			plan = "hobby"
			status = "active"
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"plan": plan,
			"status": status,
		})
	})

	mux.HandleFunc("/api/v1/payment-methods", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		contextID := r.URL.Query().Get("context_id")
		
		if r.Method == http.MethodGet {
			rows, err := db.QueryContext(r.Context(), "SELECT id, brand, last4, exp, is_default FROM payment_methods WHERE context_id = ?", contextID)
			if err != nil {
				json.NewEncoder(w).Encode([]map[string]interface{}{})
				return
			}
			defer rows.Close()
			var pms []map[string]interface{}
			for rows.Next() {
				var id, brand, last4, exp string
				var isDefault bool
				rows.Scan(&id, &brand, &last4, &exp, &isDefault)
				pms = append(pms, map[string]interface{}{
					"id": id, "brand": brand, "last4": last4, "exp": exp, "is_default": isDefault,
				})
			}
			if pms == nil {
				pms = []map[string]interface{}{}
			}
			json.NewEncoder(w).Encode(pms)
			return
		}
		
		if r.Method == http.MethodPost {
			var payload struct {
				Brand     string `json:"brand"`
				Last4     string `json:"last4"`
				Exp       string `json:"exp"`
				IsDefault bool   `json:"is_default"`
			}
			json.NewDecoder(r.Body).Decode(&payload)
			
			id := "pm_" + time.Now().Format("20060102150405")
			db.ExecContext(r.Context(), "INSERT INTO payment_methods (id, context_id, brand, last4, exp, is_default, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
				id, contextID, payload.Brand, payload.Last4, payload.Exp, payload.IsDefault, time.Now())
			json.NewEncoder(w).Encode(map[string]string{"status": "ok", "id": id})
			return
		}
	})

	mux.HandleFunc("/api/v1/invoices", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		contextID := r.URL.Query().Get("context_id")
		
		rows, err := db.QueryContext(r.Context(), "SELECT id, amount, status, date FROM invoices WHERE context_id = ? ORDER BY date DESC", contextID)
		if err != nil {
			json.NewEncoder(w).Encode([]map[string]interface{}{})
			return
		}
		defer rows.Close()
		
		var invs []map[string]interface{}
		for rows.Next() {
			var id, status string
			var amount float64
			var date time.Time
			rows.Scan(&id, &amount, &status, &date)
			invs = append(invs, map[string]interface{}{
				"id": id, "amount": amount, "status": status, "date": date.Format(time.RFC3339),
			})
		}
		if invs == nil {
			invs = []map[string]interface{}{}
		}
		json.NewEncoder(w).Encode(invs)
	})

	// --- Environment Variables ---
	mux.HandleFunc("/api/v1/env", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		if r.Method == http.MethodGet {
			rows, err := db.QueryContext(r.Context(), "SELECT key, value FROM env_vars WHERE project_id = 'global'")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer rows.Close()
			var envs []map[string]string
			for rows.Next() {
				var k, v string
				rows.Scan(&k, &v)
				envs = append(envs, map[string]string{"key": k, "value": v})
			}
			if envs == nil {
				envs = []map[string]string{}
			}
			json.NewEncoder(w).Encode(envs)
			return
		}

		if r.Method == http.MethodPost {
			var payload struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}
			json.NewDecoder(r.Body).Decode(&payload)
			_, err := db.ExecContext(r.Context(), "INSERT OR REPLACE INTO env_vars (id, project_id, key, value, created_at) VALUES (?, ?, ?, ?, ?)", fmt.Sprintf("env_%d", time.Now().UnixNano()), "global", payload.Key, payload.Value, time.Now().UTC())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			writeLog("system", LogTypeSystem, LogLevelInfo, fmt.Sprintf("Environment variable added: %s", payload.Key))
			json.NewEncoder(w).Encode(map[string]string{"status": "saved"})
			return
		}

		if r.Method == http.MethodDelete {
			key := r.URL.Query().Get("key")
			db.ExecContext(r.Context(), "DELETE FROM env_vars WHERE project_id = 'global' AND key = ?", key)
			writeLog("system", LogTypeSystem, LogLevelInfo, fmt.Sprintf("Environment variable deleted: %s", key))
			json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
			return
		}
	})

	// --- Custom Domains ---
	mux.HandleFunc("/api/v1/domains", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		var payload struct {
			Domain string `json:"domain"`
		}
		json.NewDecoder(r.Body).Decode(&payload)
		writeLog("system", LogTypeSystem, LogLevelInfo, fmt.Sprintf("Custom domain mapping requested: %s (Pending DNS validation)", payload.Domain))
		json.NewEncoder(w).Encode(map[string]string{"status": "pending", "domain": payload.Domain})
	})

	// --- Database Studio (Query Execution) ---
	mux.HandleFunc("/api/v1/databases/query", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		var payload struct {
			Query    string `json:"query"`
			Database string `json:"database"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		
		targetDB := payload.Database
		if targetDB == "" {
			targetDB = "ncloud-db-core"
		}

		// Execute query in Docker container via psql
		cmd := exec.CommandContext(r.Context(), "docker", "exec", targetDB, "psql", "-U", "postgres", "-d", "postgres", "-c", payload.Query)
		out, err := cmd.CombinedOutput()
		
		resp := map[string]string{"output": string(out)}
		if err != nil {
			resp["error"] = err.Error()
		}
		
		writeLog("system", LogTypeSystem, LogLevelInfo, fmt.Sprintf("Executed DB query: %s", payload.Query))
		json.NewEncoder(w).Encode(resp)
	})


	// 7. Serve Dashboard UI (with logging middleware)
	fs := http.FileServer(http.Dir("./public"))
	mux.Handle("/", fs)

	// Wrap everything in the logging middleware
	baseHandler := loggingMiddleware(mux)

	// True API Gateway wrapper
	gatewayHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := strings.Split(r.Host, ":")[0]
		// If accessing localhost, 127.0.0.1, or main platform domain directly, serve platform UI/API
		if host == "localhost" || host == "127.0.0.1" || host == "zod.cloud" || host == "app.zod.cloud" {
			baseHandler.ServeHTTP(w, r)
			return
		}

		// Otherwise, extract project ID (subdomain.zod.cloud)
		parts := strings.Split(host, ".")
		projectID := parts[0]
		
		publicURLsMu.RLock()
		localPort, exists := internalPorts[projectID]
		publicURLsMu.RUnlock()

		if !exists {
			http.Error(w, "Project not found, not running, or domain not mapped.", http.StatusNotFound)
			return
		}

		targetURL, err := url.Parse("http://127.0.0.1:" + localPort)
		if err != nil {
			http.Error(w, "Internal Gateway Error", http.StatusInternalServerError)
			return
		}

		proxy := httputil.NewSingleHostReverseProxy(targetURL)
		writeLog("system", LogTypeNetwork, LogLevelDebug, fmt.Sprintf("Gateway proxying %s -> %s", r.Host, targetURL.String()))
		proxy.ServeHTTP(w, r)
	})

	writeLog("system", LogTypeSystem, LogLevelInfo, "ZOD CLOUD API Gateway listening on :8088")
	log.Println("[API] Listening on :8088...")
	log.Fatal(http.ListenAndServe(":8088", gatewayHandler))
}

// ─────────────────────────────────────────────────────────────────────────────
// HANDLERS
// ─────────────────────────────────────────────────────────────────────────────

var startTime = time.Now()

func handleDeploymentStatus(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	w.Header().Set("Content-Type", "application/json")
	id := r.URL.Query().Get("id")

	var status, projectID, publicURL string
	if globalDB != nil {
		err := globalDB.QueryRow(
			"SELECT status, project_id, COALESCE(public_url,'') FROM deployments WHERE id = ?", id,
		).Scan(&status, &projectID, &publicURL)
		if err != nil {
			status = "queued"
		}
	} else {
		status = "queued"
	}

	// Fall back to in-memory map if DB doesn't have URL yet
	if publicURL == "" {
		publicURLsMu.RLock()
		publicURL = publicURLs[id]
		publicURLsMu.RUnlock()
	}

	// Also derive local URL if running and no public URL
	if publicURL == "" && status == "running" && projectID != "" {
		publicURLsMu.RLock()
		if port := internalPorts[projectID]; port != "" {
			publicURL = fmt.Sprintf("http://localhost:%s", port)
		}
		publicURLsMu.RUnlock()
	}

	resp := map[string]interface{}{
		"status":     status,
		"public_url": publicURL,
		"named":      publicURL != "",
	}
	json.NewEncoder(w).Encode(resp)
}

func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
}

// ─────────────────────────────────────────────────────────────────────────────
// BUILD WORKER — captures stdout/stderr per line into the log DB
// ─────────────────────────────────────────────────────────────────────────────

func handleDeploymentCreated(ctx context.Context, payload []byte, storage ports.ObjectStorage, bus ports.EventBus) error {
	var event domain.DeploymentCreatedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}

	depID := event.DeploymentID
	writeLog(depID, LogTypeBuild, LogLevelInfo, fmt.Sprintf("Build started for project %s", event.ProjectID))
	log.Printf("[BuildWorker] Starting REAL build for Deployment %s", depID)
	
	if globalDB != nil {
		globalDB.Exec("UPDATE deployments SET status = 'building' WHERE id = ?", depID)
	}

	// Fire build.started
	_ = bus.Publish(ctx, domain.BuildStartedEvent{
		BaseEvent: domain.BaseEvent{DeploymentID: depID, ProjectID: event.ProjectID, Timestamp: time.Now().UTC()},
	})

	// Step 1: Download source from object storage
	key := fmt.Sprintf("projects/%s/source.zip", event.ProjectID)
	writeLog(depID, LogTypeBuild, LogLevelInfo, fmt.Sprintf("Fetching source from object storage: %s", key))
	readCloser, err := storage.Get(ctx, key)
	if err != nil {
		writeLog(depID, LogTypeBuild, LogLevelError, fmt.Sprintf("Failed to fetch source: %v", err))
		if globalDB != nil {
			globalDB.Exec("UPDATE deployments SET status = 'failed' WHERE id = ?", depID)
		}
		return err
	}
	defer readCloser.Close()

	// Step 2: Extract ZIP to temp dir
	tmpDir, err := os.MkdirTemp("", "ncloud-build-*")
	if err != nil {
		writeLog(depID, LogTypeBuild, LogLevelError, fmt.Sprintf("Failed to create temp dir: %v", err))
		if globalDB != nil {
			globalDB.Exec("UPDATE deployments SET status = 'failed' WHERE id = ?", depID)
		}
		return err
	}
	defer os.RemoveAll(tmpDir)
	writeLog(depID, LogTypeBuild, LogLevelDebug, fmt.Sprintf("Extracting source to %s", tmpDir))

	zipPath := filepath.Join(tmpDir, "source.zip")
	outFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	_, _ = io.Copy(outFile, readCloser)
	outFile.Close()

	if err := extractZip(zipPath, tmpDir); err != nil {
		writeLog(depID, LogTypeBuild, LogLevelError, fmt.Sprintf("Failed to extract zip: %v", err))
		if globalDB != nil {
			globalDB.Exec("UPDATE deployments SET status = 'failed' WHERE id = ?", depID)
		}
		return err
	}
	writeLog(depID, LogTypeBuild, LogLevelInfo, "Source extracted successfully")

	buildDir := tmpDir
	entries, err := os.ReadDir(tmpDir)
	if err == nil {
		var dirs []os.DirEntry
		for _, e := range entries {
			if e.Name() != "source.zip" && e.Name() != "__MACOSX" {
				dirs = append(dirs, e)
			}
		}
		if len(dirs) == 1 && dirs[0].IsDir() {
			buildDir = filepath.Join(tmpDir, dirs[0].Name())
			writeLog(depID, LogTypeBuild, LogLevelInfo, fmt.Sprintf("Found wrapper directory %s, using as build root", dirs[0].Name()))
		}
	}

	imageName := fmt.Sprintf("ncloud-project-%s:latest", event.ProjectID)

	// Ensure nixpacks
	nixpacksPath := filepath.Join(".", "bin", "nixpacks.exe")
	if _, err := os.Stat(nixpacksPath); os.IsNotExist(err) {
		writeLog(depID, LogTypeBuild, LogLevelInfo, "nixpacks.exe not found — downloading from GitHub...")
		os.MkdirAll(filepath.Dir(nixpacksPath), os.ModePerm)
		resp, err := http.Get("https://github.com/railwayapp/nixpacks/releases/download/v1.31.0/nixpacks-v1.31.0-x86_64-pc-windows-msvc.zip")
		if err != nil {
			writeLog(depID, LogTypeBuild, LogLevelError, fmt.Sprintf("Failed to download nixpacks: %v", err))
			if globalDB != nil {
				globalDB.Exec("UPDATE deployments SET status = 'failed' WHERE id = ?", depID)
			}
			return fmt.Errorf("failed to download nixpacks: %w", err)
		}
		defer resp.Body.Close()
		nzipPath := filepath.Join(".", "bin", "nixpacks.zip")
		noutFile, _ := os.Create(nzipPath)
		_, _ = io.Copy(noutFile, resp.Body)
		noutFile.Close()
		extractZip(nzipPath, filepath.Join(".", "bin"))
		os.Remove(nzipPath)
		writeLog(depID, LogTypeBuild, LogLevelInfo, "nixpacks.exe downloaded successfully")
	}

	args := []string{"build", buildDir, "--name", imageName}

	// Framework detection
	_ = filepath.Walk(buildDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && info.Name() == "package.json" {
			tomlPath := filepath.Join(filepath.Dir(path), "nixpacks.toml")
			if _, err := os.Stat(tomlPath); os.IsNotExist(err) {
				_ = os.WriteFile(tomlPath, []byte("[variables]\nNODE_OPTIONS = \"--openssl-legacy-provider\"\n"), 0644)
				writeLog(depID, LogTypeBuild, LogLevelInfo, "Detected Node.js project — applied nixpacks.toml fix (ERR_OSSL_EVP_UNSUPPORTED)")
			}
			return filepath.SkipAll
		}
		return nil
	})

	writeLog(depID, LogTypeBuild, LogLevelInfo, fmt.Sprintf("Running: nixpacks build --name %s", imageName))

	// Execute nixpacks and capture output per-line
	cmd := exec.CommandContext(ctx, nixpacksPath, args...)

	stdoutPipe, _ := cmd.StdoutPipe()
	stderrPipe, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		writeLog(depID, LogTypeBuild, LogLevelError, fmt.Sprintf("Failed to start nixpacks: %v", err))
		if globalDB != nil {
			globalDB.Exec("UPDATE deployments SET status = 'failed' WHERE id = ?", depID)
		}
		return err
	}

	// Stream stdout
	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				writeLog(depID, LogTypeBuild, LogLevelInfo, line)
			}
		}
	}()

	// Stream stderr (nixpacks writes build progress to stderr)
	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				level := LogLevelInfo
				if strings.Contains(strings.ToLower(line), "error") {
					level = LogLevelError
				} else if strings.Contains(strings.ToLower(line), "warn") {
					level = LogLevelWarn
				}
				writeLog(depID, LogTypeBuild, level, line)
			}
		}
	}()

	if err := cmd.Wait(); err != nil {
		writeLog(depID, LogTypeBuild, LogLevelError, fmt.Sprintf("nixpacks build FAILED: %v", err))
		log.Printf("[BuildWorker] Docker build FAILED: %v", err)
		if globalDB != nil {
			globalDB.Exec("UPDATE deployments SET status = 'failed' WHERE id = ?", depID)
		}
		return err
	}

	writeLog(depID, LogTypeBuild, LogLevelInfo, fmt.Sprintf("✅ Image built successfully: %s", imageName))
	log.Printf("[BuildWorker] Successfully built image %s", imageName)

	completedEvent := domain.BuildCompletedEvent{
		BaseEvent: domain.BaseEvent{
			DeploymentID: event.DeploymentID,
			ProjectID:    event.ProjectID, // FIXED: pass ProjectID so deploy worker knows the project name
			Timestamp:    time.Now().UTC(),
		},
		ImageName: imageName,
	}

	_ = bus.Publish(ctx, completedEvent)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

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
		_, _ = io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()
	}
	return nil
}

func initSchema(db *sql.DB) {
	schemaPath := filepath.Join("migrations", "001_initial_schema.sql")
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		log.Fatalf("Failed to read schema from %s: %v", schemaPath, err)
	}

	_, err = db.Exec(string(schemaBytes))
	if err != nil {
		log.Fatalf("Failed to initialize SQLite schema: %v", err)
	}

	log.Println("[DB] Phase 3 Schema initialized")

	// Seed Mock Data for UI interaction
	db.Exec(`INSERT OR IGNORE INTO users (id, email, password_hash, tier) VALUES ('user_1', 'admin@zod.cloud', 'hash', 'pro')`)
	db.Exec(`INSERT OR IGNORE INTO teams (id, name) VALUES ('team_1', 'Zod Corp')`)
	db.Exec(`INSERT OR IGNORE INTO team_members (team_id, user_id, role) VALUES ('team_1', 'user_1', 'owner')`)
	
	// Personal Billing
	db.Exec(`INSERT OR IGNORE INTO billing_subscriptions (id, user_id, plan, status) VALUES ('sub_user1', 'user_1', 'hobby', 'active')`)
	// Team Billing
	db.Exec(`INSERT OR IGNORE INTO billing_subscriptions (id, team_id, plan, status) VALUES ('sub_team1', 'team_1', 'ultra', 'active')`)
}
