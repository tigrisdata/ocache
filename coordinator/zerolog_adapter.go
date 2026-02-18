package coordinator

import (
	"fmt"

	zlog "github.com/rs/zerolog/log"
)

// zerologAdapter adapts zerolog to the go-kit/log interface used by dskit.
// This allows dskit's memberlist and ring components to log through zerolog.
type zerologAdapter struct{}

// Log implements the go-kit/log.Logger interface
func (z *zerologAdapter) Log(keyvals ...interface{}) error {
	// Convert key-value pairs to a zerolog event
	event := zlog.Info()

	// Process key-value pairs
	for i := 0; i < len(keyvals)-1; i += 2 {
		key, ok := keyvals[i].(string)
		if !ok {
			continue
		}

		// Check for "level" key to set appropriate log level.
		// Handle both fmt.Stringer (go-kit level.Value) and plain string
		// (dskit's memberlist logger fallback for unrecognized levels).
		if key == "level" {
			var lvl string
			switch v := keyvals[i+1].(type) {
			case fmt.Stringer:
				lvl = v.String()
			case string:
				lvl = v
			}

			switch lvl {
			case "error":
				event = zlog.Error()
			case "warn":
				event = zlog.Warn()
			case "info":
				event = zlog.Info()
			case "debug":
				// Map dskit debug to zerolog Trace to reduce noise.
				event = zlog.Trace()
			default:
				event = zlog.Debug()
			}
			continue
		}

		// Add the key-value pair to the event
		val := keyvals[i+1]
		switch v := val.(type) {
		case string:
			event = event.Str(key, v)
		case int:
			event = event.Int(key, v)
		case int64:
			event = event.Int64(key, v)
		case float64:
			event = event.Float64(key, v)
		case bool:
			event = event.Bool(key, v)
		case error:
			event = event.Err(v)
		default:
			event = event.Interface(key, v)
		}
	}

	event.Msg("[dskit]")
	return nil
}
