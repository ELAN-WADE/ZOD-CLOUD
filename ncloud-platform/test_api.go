package main
import (
	"fmt"
	"io"
	"net/http"
)
func main() {
	resp, err := http.Get("http://localhost:8088/api/v1/deployments")
	if err != nil { panic(err) }
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("Status: %d\nBody: %s\n", resp.StatusCode, string(b))
}
