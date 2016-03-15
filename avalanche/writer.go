package avalanche

// LineProtocolWriter is the interface used to write InfluxDB Line Protocol to a remote server.
type LineProtocolWriter interface {
	// WriteLineProtocol writes the given byte slice containing line protocol data
	// to an implementation-specific remote server.
	// Implementers should return errors returned by the underlying transport.
	WriteLineProtocol([]byte) error
}
