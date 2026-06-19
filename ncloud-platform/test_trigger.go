package main
import (
	"fmt"
	"io"
	"net/http"
)
func main() {
	req, _ := http.NewRequest("POST", "http://localhost:8088/api/v1/deployments/?project_id=proj-123", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil { panic(err) }
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("Status: %d\nBody: %s\n", resp.StatusCode, string(b))
}
