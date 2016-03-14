package avalanche

type LineProtocolWriter interface {
	WriteLineProtocol([]byte) error
}
