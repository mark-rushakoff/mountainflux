package river

import (
	"bytes"
	"sort"
)

// SeriesKey is a non-optimized way to create a series key for the line protocol,
// i.e. a measurement with tag keys and values.
func SeriesKey(measurement string, tags map[string]string) []byte {
	var b bytes.Buffer
	b.WriteString(measurement)

	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		b.WriteByte(',')

		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(tags[k])
	}

	return b.Bytes()
}
