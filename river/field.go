package river

import (
	"bytes"
	"strconv"
)

type Field interface {
	writeToBuf(*bytes.Buffer)
}

type Bool struct {
	Name  []byte
	Value bool
}

func (b Bool) writeToBuf(buf *bytes.Buffer) {
	buf.Write(b.Name)
	buf.WriteByte('=')

	if b.Value {
		buf.WriteByte('T')
	} else {
		buf.WriteByte('F')
	}
}

type Int struct {
	Name  []byte
	Value int64
}

func (i Int) writeToBuf(buf *bytes.Buffer) {
	buf.Write(i.Name)
	buf.WriteByte('=')

	// Max int64 fits in 19 base-10 digits
	iBuf := make([]byte, 0, 19)
	iBuf = strconv.AppendInt(iBuf, i.Value, 10)
	buf.Write(iBuf)
	buf.WriteByte('i')
}

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

type String struct {
	Name  []byte
	Value []byte
}

func (s String) writeToBuf(buf *bytes.Buffer) {
	buf.Write(s.Name)
	buf.WriteByte('=')
	buf.Write(s.Value)
}
