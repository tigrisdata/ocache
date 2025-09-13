package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddressValidator_ValidateClusterAddress(t *testing.T) {
	tests := []struct {
		name          string
		address       string
		wantErr       bool
		errorContains string
	}{
		{
			name:    "valid localhost with port",
			address: "localhost:7000",
			wantErr: false,
		},
		{
			name:    "valid IP with port",
			address: "192.168.1.1:7000",
			wantErr: false,
		},
		{
			name:    "valid IPv6 with port",
			address: "[::1]:7000",
			wantErr: false,
		},
		{
			name:    "listen on all interfaces",
			address: ":7000",
			wantErr: false,
		},
		{
			name:          "missing port",
			address:       "localhost",
			wantErr:       true,
			errorContains: "must include port",
		},
		{
			name:          "invalid port",
			address:       "localhost:99999",
			wantErr:       true,
			errorContains: "port must be between",
		},
		{
			name:          "empty address",
			address:       "",
			wantErr:       true,
			errorContains: "cannot be empty",
		},
		{
			name:    "valid hostname",
			address: "node1.cluster.local:7000",
			wantErr: false,
		},
		{
			name:          "invalid hostname characters",
			address:       "node@1:7000",
			wantErr:       true,
			errorContains: "invalid host",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateClusterAddress(tt.address)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAddressValidator_ValidateNodeAddress(t *testing.T) {
	tests := []struct {
		name          string
		address       string
		wantErr       bool
		errorContains string
	}{
		{
			name:    "valid node with port",
			address: "node1.cluster.local:7000",
			wantErr: false,
		},
		{
			name:    "valid localhost node",
			address: "localhost:7000",
			wantErr: false,
		},
		{
			name:          "missing port",
			address:       "node1.cluster.local",
			wantErr:       true,
			errorContains: "address must include port",
		},
		{
			name:          "empty host",
			address:       ":7000",
			wantErr:       true,
			errorContains: "host cannot be empty",
		},
		{
			name:          "invalid port",
			address:       "node1:0",
			wantErr:       true,
			errorContains: "port must be between",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNodeAddress(tt.address)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIsDNSName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "simple hostname",
			input:    "hostname",
			expected: true,
		},
		{
			name:     "fully qualified domain name",
			input:    "node1.cluster.local",
			expected: true,
		},
		{
			name:     "kubernetes service",
			input:    "myservice.default.svc.cluster.local",
			expected: true,
		},
		{
			name:     "with underscore",
			input:    "my_service",
			expected: true,
		},
		{
			name:     "with hyphen",
			input:    "my-service",
			expected: true,
		},
		{
			name:     "starting with hyphen",
			input:    "-service",
			expected: false,
		},
		{
			name:     "ending with hyphen",
			input:    "service-",
			expected: false,
		},
		{
			name:     "with spaces",
			input:    "my service",
			expected: false,
		},
		{
			name:     "empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "with special characters",
			input:    "service@cluster",
			expected: false,
		},
		{
			name:     "numeric",
			input:    "12345",
			expected: true,
		},
		{
			name:     "mixed alphanumeric",
			input:    "node1",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDNSName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
