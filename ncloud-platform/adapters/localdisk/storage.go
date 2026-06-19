package localdisk

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

// LocalDiskStorage implements ports.ObjectStorage by writing files to a local directory.
// This simulates an S3 bucket or Google Cloud Storage.
type LocalDiskStorage struct {
	baseDir string
}

func NewLocalDiskStorage(baseDir string) (*LocalDiskStorage, error) {
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}
	return &LocalDiskStorage{baseDir: baseDir}, nil
}

func (s *LocalDiskStorage) Put(ctx context.Context, key string, data io.Reader) error {
	fullPath := filepath.Join(s.baseDir, key)
	
	// Ensure parent directories exist
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create directories for key %s: %w", key, err)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file for key %s: %w", key, err)
	}
	defer file.Close()

	written, err := io.Copy(file, data)
	if err != nil {
		return fmt.Errorf("failed to write data for key %s: %w", key, err)
	}

	log.Printf("[LocalDisk S3] Successfully wrote %d bytes to %s", written, key)
	return nil
}

func (s *LocalDiskStorage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	fullPath := filepath.Join(s.baseDir, key)
	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("object not found: %s", key) // Replace with domain.ErrNotFound in real app
		}
		return nil, fmt.Errorf("failed to open file for key %s: %w", key, err)
	}
	
	log.Printf("[LocalDisk S3] Reading object %s", key)
	return file, nil
}

func (s *LocalDiskStorage) Delete(ctx context.Context, key string) error {
	fullPath := filepath.Join(s.baseDir, key)
	err := os.Remove(fullPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file for key %s: %w", key, err)
	}
	log.Printf("[LocalDisk S3] Deleted object %s", key)
	return nil
}
