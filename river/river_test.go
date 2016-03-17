package river_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/mark-rushakoff/mountainflux/river"
)

func TestRiver_WriteLine(t *testing.T) {
	var b bytes.Buffer

	sk := river.SeriesKey("rooms", map[string]string{"room": "r1", "building": "b1"})

	river.WriteLine(&b, sk, []river.Field{
		river.Bool{Name: []byte("lights"), Value: true},
		river.Int{Name: []byte("occupants"), Value: int64(3)},
		river.Float{Name: []byte("temp_f"), Value: 72.5},
		river.String{Name: []byte("meeting_name"), Value: []byte("bikeshed")},
	}, int64(1435362189575692182))

	// Tag keys are alphabetized, fields are taken in order, time as entered
	exp := `rooms,building=b1,room=r1 lights=T,occupants=3i,temp_f=72.5,meeting_name=bikeshed 1435362189575692182` + "\n"

	got := string(b.Bytes())
	if got != exp {
		t.Fatalf("got: %s, exp: %s", got, exp)
	}
}

func benchmarkPointsWithTypes(b *testing.B, useBool, useInt, useFloat, useString bool) {
	// Build up the series key just once
	sk := []byte("rooms,building=b1,room=r1")

	// "Random" values to use in the fields
	lights := []bool{false, true}
	names := [][]byte{[]byte("bikeshed"), []byte("reactor"), []byte("refreshments"), []byte("ducks")}

	// Allocate fields only once and modify in-place
	boolField := river.Bool{Name: []byte("lights")}
	intField := river.Int{Name: []byte("occupants")}
	floatField := river.Float{Name: []byte("temp_f")}
	stringField := river.String{Name: []byte("meeting_name")}

	// Hold collection of fields for later call to WriteLine
	fields := make([]river.Field, 0, 4)
	if useBool {
		fields = append(fields, &boolField)
	}
	if useInt {
		fields = append(fields, &intField)
	}
	if useFloat {
		fields = append(fields, &floatField)
	}
	if useString {
		fields = append(fields, &stringField)
	}

	var cw CountingWriter
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if useBool {
			boolField.Value = lights[i&0x01]
		}
		if useInt {
			intField.Value = int64(i)
		}
		if useFloat {
			floatField.Value = float64(i) * 2.5
		}
		if useString {
			stringField.Value = names[i&0x03]
		}

		river.WriteLine(&cw, sk, fields, time.Now().UnixNano())
	}

	b.SetBytes(cw.N)
}

///////////////////////////// bool, int, float, string

func BenchmarkRiver_WriteLine_EachType(b *testing.B) {
	benchmarkPointsWithTypes(b, true, true, true, true)
}

func BenchmarkRiver_WriteLine_JustBool(b *testing.B) {
	benchmarkPointsWithTypes(b, true, false, false, false)
}

func BenchmarkRiver_WriteLine_JustInt(b *testing.B) {
	benchmarkPointsWithTypes(b, false, true, false, false)
}

func BenchmarkRiver_WriteLine_JustFloat(b *testing.B) {
	benchmarkPointsWithTypes(b, false, false, true, false)
}

func BenchmarkRiver_WriteLine_JustString(b *testing.B) {
	benchmarkPointsWithTypes(b, false, false, false, true)
}

type CountingWriter struct {
	N int64
}

func (w *CountingWriter) Write(b []byte) (int, error) {
	w.N += int64(len(b))
	return len(b), nil
}
