package domain

import "errors"

// ErrNotFound is returned when a requested entity cannot be found in the repository.
var ErrNotFound = errors.New("not found")
