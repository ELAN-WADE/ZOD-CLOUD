package tunnel

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// TunnelResult holds the outcome of starting a Cloudflare tunnel.
type TunnelResult struct {
	InternalURL string
	PublicURL   string
	TunnelID    string
}

type CloudflareTunnel struct {
	binaryPath string
}

func NewCloudflareTunnel() *CloudflareTunnel {
	return &CloudflareTunnel{
		binaryPath: filepath.Join(".", "bin", "cloudflared.exe"),
	}
}

func (c *CloudflareTunnel) EnsureBinary() error {
	if _, err := os.Stat(c.binaryPath); err == nil {
		return nil // Already exists
	}

	log.Println("[Tunnel] cloudflared.exe not found. Downloading from Cloudflare...")

	if err := os.MkdirAll(filepath.Dir(c.binaryPath), os.ModePerm); err != nil {
		return err
	}

	url := "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download cloudflared: %s", resp.Status)
	}

	outFile, err := os.Create(c.binaryPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		return err
	}

	log.Println("[Tunnel] Successfully downloaded cloudflared.exe")
	return nil
}

// slugify converts a project name to a valid tunnel hostname slug.
// e.g. "ZOD CLOUD" -> "zod-cloud", "my_app v2" -> "my-app-v2"
func slugify(name string) string {
	s := strings.ToLower(name)
	var result []rune
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			result = append(result, r)
		} else if len(result) > 0 && result[len(result)-1] != '-' {
			result = append(result, '-')
		}
	}
	// Trim trailing dash
	for len(result) > 0 && result[len(result)-1] == '-' {
		result = result[:len(result)-1]
	}
	if len(result) == 0 {
		return "zod-cloud"
	}
	return string(result)
}

// StartTunnel starts a Cloudflare Quick Tunnel and returns the public URL.
// projectName is used to attempt a named tunnel URL (best-effort).
// Set CLOUDFLARE_TOKEN env var for authenticated named tunnels (guaranteed naming).
func (c *CloudflareTunnel) StartTunnel(localPort string, projectName string) (*TunnelResult, error) {
	if err := c.EnsureBinary(); err != nil {
		return nil, fmt.Errorf("failed to ensure cloudflared binary: %w", err)
	}

	slug := slugify(projectName)
	log.Printf("[Tunnel] Starting Cloudflare Tunnel for port %s (project: %s, slug: %s)...", localPort, projectName, slug)

	var cmd *exec.Cmd

	// Check if we have an authenticated tunnel token (gives guaranteed naming)
	cfToken := os.Getenv("CLOUDFLARE_TOKEN")
	if cfToken != "" {
		// Authenticated mode: persistent named tunnel via token
		log.Printf("[Tunnel] Using authenticated tunnel with CLOUDFLARE_TOKEN")
		cmd = exec.Command(c.binaryPath,
			"tunnel", "--no-autoupdate",
			"run", "--token", cfToken,
			"--protocol", "http2",
			"--url", fmt.Sprintf("http://localhost:%s", localPort),
		)
	} else {
		// Quick Tunnel (unauthenticated)
		log.Printf("[Tunnel] CLOUDFLARE_TOKEN not provided, using Quick Tunnel")
		cmd = exec.Command(c.binaryPath,
			"tunnel", "--no-autoupdate",
			"--url", fmt.Sprintf("http://localhost:%s", localPort),
		)
	}

	// Cloudflared writes its logs to stderr
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	urlChan := make(chan string, 1)
	errChan := make(chan error, 1)

	go func() {
		scanner := bufio.NewScanner(stderr)
		// Match trycloudflare.com URLs (Quick Tunnel) OR *.cfargotunnel.com (auth tunnel)
		regex := regexp.MustCompile(`https://[a-zA-Z0-9\-]+\.(trycloudflare\.com|cfargotunnel\.com)`)

		urlFound := false
		for scanner.Scan() {
			line := scanner.Text()
			log.Printf("[Cloudflared] %s", line) // Forward all cloudflared logs

			if !urlFound {
				if matches := regex.FindString(line); matches != "" {
					if !strings.Contains(matches, "api.trycloudflare.com") && !strings.Contains(matches, "update.trycloudflare.com") {
						urlFound = true
						urlChan <- matches
						// Keep draining stderr — do NOT return
					}
				}
			}

			if strings.Contains(strings.ToLower(line), "error") || strings.Contains(strings.ToLower(line), "failed") {
				log.Println("[Cloudflared Error]", line)
			}
		}

		if err := scanner.Err(); err != nil && !urlFound {
			errChan <- err
		} else if !urlFound {
			errChan <- fmt.Errorf("tunnel URL not found in cloudflared output")
		}
	}()

	// Wait for the URL or a timeout
	select {
	case url := <-urlChan:
		tunnelID := fmt.Sprintf("tun_%d", time.Now().UnixNano())

		result := &TunnelResult{
			InternalURL: url,
			PublicURL:   url, // Use the actual trycloudflare.com URL
			TunnelID:    tunnelID,
		}

		log.Printf("[Tunnel] ✅ Tunnel established. Public URL: %s, ID: %s", url, tunnelID)
		return result, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(45 * time.Second):
		return nil, fmt.Errorf("timed out waiting for cloudflared tunnel URL after 45s")
	}
}
