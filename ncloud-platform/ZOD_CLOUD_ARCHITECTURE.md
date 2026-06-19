# ZOD CLOUD: Technical Defense & Code Walkthrough

This document is designed to help you completely understand and **defend** how ZOD CLOUD is programmed. It breaks down the exact code structure, the execution flow from the moment the program starts, the core Go paradigms used, and the data flow across the entire system.

---

## 1. How the Code is Structured (The Directory Layout)

The system is built using **Hexagonal Architecture** (also known as Ports and Adapters). This is an enterprise design pattern that ensures the core logic of the application never directly imports external libraries (like databases or HTTP frameworks).

Here is how the code is physically separated:

*   **`internal/domain/` (The Core Entities)**
    *   This folder contains pure Go structs. For example, `deployment.go` defines what a deployment *is*. There is no logic here, just data definitions (`Project`, `LogEntry`).
*   **`internal/ports/` (The Interfaces / Ports)**
    *   This folder contains Go interfaces. It defines *contracts*. For example, `EventBus` is defined here as an interface with `Publish()` and `Subscribe()` methods. It doesn't care *how* messages are sent.
*   **`internal/services/` (The Orchestrators)**
    *   This contains the business logic, like `deployment_service.go`. It brings the domain and ports together. When a user requests a deploy, this service creates the domain object and calls `EventBus.Publish()`.
*   **`adapters/` (The Concrete Implementations)**
    *   This is where the actual messy technology lives.
    *   `adapters/sqlite`: Implements the database ports using SQL queries.
    *   `adapters/docker`: Implements the scheduling port by talking to the Docker daemon.
    *   `adapters/tunnel`: Contains the code to launch the `cloudflared` binary.
*   **`cmd/standalone/main.go` (The Wiring / Entrypoint)**
    *   This is the *only* file that imports everything. It connects the `adapters` to the `ports` and boots up the system.

**Why defend this structure?** If someone asks why you didn't just put everything in `main.go`, you can explain that Hexagonal Architecture allows you to swap out SQLite for PostgreSQL, or Local Docker for Kubernetes, by simply writing a new adapter in the `adapters/` folder without touching a single line of the core `internal/` business logic.

---

## 2. How the Code Executes (The Start-Up Flow)

If you trace the code from the very first line of `main()` in `cmd/standalone/main.go`, this is exactly how it is programmed to execute:

1.  **Database Bootstrapping**:
    The code runs `sql.Open("sqlite", "./ncloud.db")` first. It instantly runs `initSchema()` which executes `CREATE TABLE IF NOT EXISTS` queries for projects, deployments, logs, env_vars, etc.
2.  **Adapter Wiring**:
    It instantiates the adapters: `mesh.NewMeshEventBus()` (an in-memory channel-based event bus), `localdisk.NewLocalDiskStorage()` (the S3 equivalent), and `docker.NewLocalDockerScheduler()`.
3.  **Spawning the Workers (Goroutines)**:
    The code spawns background workers using `globalMesh.Subscribe()`. 
    *   It creates a **Build Worker** listening for `"deployments.created"`.
    *   It creates a **Deploy Worker** listening for `"builds.completed"`.
    These workers are background processes that sleep until an event wakes them up.
4.  **Starting the Metrics Polling**:
    A `go func()` is spawned containing an infinite `for { ... }` loop. Every 5 seconds, it executes `docker stats --format json`, parses the CPU/Mem, and broadcasts it.
5.  **Binding the HTTP Server**:
    Finally, `http.NewServeMux()` is created. All the API endpoints (`/api/v1/...`) are registered. The entire Mux is wrapped in a `loggingMiddleware` to track requests. Then `http.ListenAndServe(":8088")` is called, which blocks forever, keeping the server alive.

---

## 3. Data Flow: Defending the Deployment Lifecycle

When an interviewer or reviewer asks: *"Walk me through exactly what happens in the code when I deploy an app,"* use this exact flow:

### Phase 1: The API Gateway (HTTP to Event)
*   **Code**: `mux.HandleFunc("/api/v1/webhooks/github", ...)`
*   **Action**: A POST request hits the Go server. The Go server uses the `go-git` library to clone the remote repository into a temporary folder (`os.MkdirTemp`). It zips the folder, saves it to the `localdisk` adapter, and then calls `EventBus.Publish("deployments.created", payload)`.
*   **Paradigm**: The HTTP request instantly returns a `202 Accepted`. The user isn't waiting for the build to finish to get a response. This is asynchronous processing.

### Phase 2: The Build Worker (Event to Docker Image)
*   **Code**: `handleDeploymentCreated(ctx, payload)`
*   **Action**: The Build Worker wakes up. It reads the source code zip from disk. It creates an `exec.Command` to run the `nixpacks build` binary.
*   **Log Streaming Magic**: To stream logs live, the code hooks into `cmd.StdoutPipe()`. It spins up a Goroutine with a `bufio.NewScanner` that reads the terminal output line-by-line as it compiles. For every line, it calls `writeLog()`, pushing the text to the UI instantly.
*   **End**: The image is built. It publishes `"builds.completed"`.

### Phase 3: The Deploy Worker (Docker Image to Live URL)
*   **Code**: Subscribed to `"builds.completed"`
*   **Action**: The worker calls the `docker` adapter to run the image. It uses `exec.Command("docker", "port")` to find out which random port Docker assigned to the container.
*   **Networking**: It then calls the `tunnel` adapter. The Go code launches `cloudflared tunnel --url http://localhost:[port] --name [project-name]`.
*   **End**: It gets the generated `trycloudflare.com` URL (or custom domain) and saves it to a global `publicURLs` map protected by a `sync.RWMutex` (to prevent race conditions when the API reads it).

---

## 4. How the Frontend is Coded (Vanilla SPA)

The frontend is built entirely in `index.html` without React, Vue, or Node.js.

*   **Architecture**: It is a Single Page Application (SPA). All the HTML for every view (Dashboard, Settings, Terminal) exists in the file but is hidden using CSS (`display: none`).
*   **Routing**: The JavaScript `switchMainView(viewId)` function simply removes the `.hidden` class from the requested view and adds it to the others.
*   **State Management**: Real-time data is handled by the browser's native `EventSource` API (Server-Sent Events). 
    *   When you click a deployment, JS calls `new EventSource('/api/v1/logs/stream')`.
    *   The Go backend holds the connection open and sends `data: {...}` packets.
    *   The JS `.onmessage` event parses the JSON and dynamically appends `<div>` elements to the log console or updates the `Chart.js` data arrays.

---

## 5. Key Go Paradigms to Defend (Interview Cheatsheet)

If asked about advanced programming concepts used in ZOD CLOUD, highlight these:

1.  **Concurrency via Goroutines & Channels (`go func()`)**
    *   *Usage*: The entire Event Bus, Metrics Poller, and Log Streamer use goroutines.
    *   *Defense*: Goroutines are incredibly lightweight threads multiplexed onto OS threads. We use them so the web server can handle thousands of simultaneous SSE connections without blocking the main CPU thread.
2.  **Race Condition Prevention (`sync.RWMutex`)**
    *   *Usage*: Used in the `SSEBroker` and the `publicURLs` map.
    *   *Defense*: Because multiple goroutines (HTTP requests) might try to read the public URL of a deployment at the exact same time the Deploy Worker is writing to it, a data race could crash the server. We use `RWMutex.Lock()` when writing, and `RWMutex.RLock()` when reading to guarantee memory safety.
3.  **Context Propagation (`context.Context`)**
    *   *Usage*: Passed through all `TriggerDeployment` and `handleDeploymentCreated` functions.
    *   *Defense*: Contexts allow us to gracefully cancel operations. If a user deletes a project while it's building, we can cancel the context, which instantly kills the running `nixpacks` OS process via `exec.CommandContext`.
4.  **WebSocket Hijacking (`gorilla/websocket`)**
    *   *Usage*: The Interactive Terminal.
    *   *Defense*: We upgrade the HTTP connection to a raw TCP WebSocket. We then take the IO pipes of `docker exec` and asynchronously pipe them directly into the WebSocket connection, allowing bidirectional binary streaming (essential for terminal keystrokes).
