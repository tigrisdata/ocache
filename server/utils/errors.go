package utils

import "fmt"

// WrapError wraps an error with an operation and path.
func WrapError(op string, path string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s %s: %w", op, path, err)
}
