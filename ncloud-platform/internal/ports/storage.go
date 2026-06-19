package ports

import (
	"context"
	"io"
)

// ObjectStorage defines the contract for storing and retrieving files.
// In a real cloud, this would be implemented by an S3 or GCS adapter.
type ObjectStorage interface {
	// Put stores a file stream into the object storage at the given key.
	Put(ctx context.Context, key string, data io.Reader) error
	
	// Get retrieves a file stream from the object storage.
	// The caller is responsible for closing the ReadCloser.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	
	// Delete removes a file from the object storage.
	Delete(ctx context.Context, key string) error
}
