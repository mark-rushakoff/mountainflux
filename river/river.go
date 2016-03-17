// Package river carves through the mountains to write your points.
package river

import (
	"io"
	"strconv"
)

var (
	space = []byte(" ")
	comma = []byte(",")
)

// WriteLine writes the line represented by seriesKey, fields, and time, to w.
// Returns any error returned during write.
func WriteLine(w io.Writer, seriesKey []byte, fields []Field, time int64) error {
	// Series key of form `cpu,host=h1,region=west`
	if _, err := w.Write(seriesKey); err != nil {
		return err
	}

	// Space before fields
	if _, err := w.Write(space); err != nil {
		return err
	}

	for i, field := range fields {
		// Leading comma on every field except the first.
		if i != 0 {
			if _, err := w.Write(comma); err != nil {
				return err
			}
		}

		// Write out the field, of form abc=xyz.
		field.WriteTo(w)
	}

	// Timestamp in nanoseconds, formatted in base 10, should fit in exactly 19 bytes for the foreseeable future.
	// Plus one byte for the leading space, plus one for the trailing newline.
	tsBuf := make([]byte, 1, 21)
	tsBuf[0] = ' '
	tsBuf = strconv.AppendInt(tsBuf, time, 10)
	tsBuf = append(tsBuf, '\n')

	if _, err := w.Write(tsBuf); err != nil {
		return err
	}

	return nil
}
