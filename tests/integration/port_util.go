package integration

import (
	"fmt"
	"net"
)

// getFreePorts returns n free ports by asking the OS to assign them.
// Uses port 0 to let the OS pick available ephemeral ports.
// All listeners are opened simultaneously to ensure unique ports,
// then closed to release them for actual use.
func getFreePorts(n int) ([]int, error) {
	ports := make([]int, n)
	listeners := make([]net.Listener, n)

	// Open all listeners first to ensure we get unique ports
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			// Close any listeners we already opened
			for j := 0; j < i; j++ {
				listeners[j].Close()
			}
			return nil, fmt.Errorf("failed to get free port: %w", err)
		}
		listeners[i] = l
		ports[i] = l.Addr().(*net.TCPAddr).Port
	}

	// Close all listeners to release the ports for actual use
	for _, l := range listeners {
		l.Close()
	}

	return ports, nil
}
