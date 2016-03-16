package avalanche

// LineProtocolWriter is the interface used to write InfluxDB Line Protocol to a remote server.
type LineProtocolWriter interface {
	// WriteLineProtocol writes the given byte slice containing line protocol data
	// to an implementation-specific remote server.
	// Returns the latency, in nanoseconds, of executing the write against the remote server and applicable errors.
	// Implementers must return errors returned by the underlying transport but are free to return
	// other, context-specific errors.
	WriteLineProtocol([]byte) (latencyNs int64, err error)
}
