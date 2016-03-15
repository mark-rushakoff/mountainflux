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
