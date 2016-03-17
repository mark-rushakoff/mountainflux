package river

import (
	"io"
	"strconv"
)

// Field represents an InfluxDB field to be serialized by river.WriteLine.
// The Bool, Int, Float, and String types implement this interface.
//
// Implementers of Field should print out the key=value portion of the field.
// river.WriteLine will take care of the rest of commas and spaces.
type Field io.WriterTo

var (
	_ Field = Bool{}
	_ Field = Int{}
	_ Field = Float{}
	_ Field = String{}
)

// Bool represents a boolean InfluxDB field.
type Bool struct {
	Name  []byte
	Value bool
}

var (
	equalSign = []byte("=")
	eqTrue    = []byte("=T")
	eqFalse   = []byte("=F")
)

func (b Bool) WriteTo(w io.Writer) (int64, error) {
	if n, err := w.Write(b.Name); err != nil {
		return int64(n), err
	}

	var eqVal []byte
	if b.Value {
		eqVal = eqTrue
	} else {
		eqVal = eqFalse
	}

	n, err := w.Write(eqVal)
	return int64(n), err
}

// Int represents an integer InfluxDB field.
type Int struct {
	Name  []byte
	Value int64
}

func (i Int) WriteTo(w io.Writer) (int64, error) {
	if n, err := w.Write(i.Name); err != nil {
		return int64(n), err
	}

	// Max int64 fits in 19 base-10 digits;
	// plus 1 for the leading =, plus 1 for the trailing i required for ints.
	iBuf := make([]byte, 1, 21)
	iBuf[0] = '='
	iBuf = strconv.AppendInt(iBuf, i.Value, 10)
	iBuf = append(iBuf, 'i')

	n, err := w.Write(iBuf)
	return int64(n), err
}

// Float represents a floating point InfluxDB field.
type Float struct {
	Name  []byte
	Value float64
}

func (f Float) WriteTo(w io.Writer) (int64, error) {
	if n, err := w.Write(f.Name); err != nil {
		return int64(n), err
	}

	// Taking a total guess here at what size a float might fit in
	var fBuf = make([]byte, 1, 32)
	fBuf[0] = '='
	fBuf = strconv.AppendFloat(fBuf, f.Value, 'f', -1, 64)
	n, err := w.Write(fBuf)
	return int64(n), err
}

// String represents a string InfluxDB field.
type String struct {
	Name  []byte
	Value []byte
}

func (s String) WriteTo(w io.Writer) (int64, error) {
	if n, err := w.Write(s.Name); err != nil {
		return int64(n), err
	}
	if n, err := w.Write(equalSign); err != nil {
		return int64(n), err
	}
	n, err := w.Write(s.Value)
	return int64(n), err
}
