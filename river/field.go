package river

import (
	"bytes"
	"strconv"
)

// Field represents an InfluxDB field to be serialized by river.WriteLine.
// The Bool, Int, Float, and String types implement this interface.
type Field interface {
	writeToBuf(*bytes.Buffer)
}

// Bool represents a boolean InfluxDB field.
type Bool struct {
	Name  []byte
	Value bool
}

var (
	eqTrue  = []byte("=T")
	eqFalse = []byte("=F")
)

func (b Bool) writeToBuf(buf *bytes.Buffer) {
	buf.Write(b.Name)

	if b.Value {
		buf.Write(eqTrue)
	} else {
		buf.Write(eqFalse)
	}
}

// Int represents an integer InfluxDB field.
type Int struct {
	Name  []byte
	Value int64
}

func (i Int) writeToBuf(buf *bytes.Buffer) {
	buf.Write(i.Name)

	// Max int64 fits in 19 base-10 digits;
	// plus 1 for the leading =, plus 1 for the trailing i required for ints.
	iBuf := make([]byte, 1, 21)
	iBuf[0] = '='
	iBuf = strconv.AppendInt(iBuf, i.Value, 10)
	iBuf = append(iBuf, 'i')

	buf.Write(iBuf)
}

// Float represents a floating point InfluxDB field.
type Float struct {
	Name  []byte
	Value float64
}

func (f Float) writeToBuf(buf *bytes.Buffer) {
	buf.Write(f.Name)
	buf.WriteByte('=')

	// Max int64 fits in 19 base-10 digits
	var fBuf []byte
	fBuf = strconv.AppendFloat(fBuf, f.Value, 'f', -1, 64)
	buf.Write(fBuf)
}

// String represents a string InfluxDB field.
type String struct {
	Name  []byte
	Value []byte
}

func (s String) writeToBuf(buf *bytes.Buffer) {
	buf.Write(s.Name)
	buf.WriteByte('=')
	buf.Write(s.Value)
}
