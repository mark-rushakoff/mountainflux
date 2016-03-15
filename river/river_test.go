package river_test

import (
	"bytes"
	"testing"

	"github.com/mark-rushakoff/mountainflux/river"
)

func TestCairn_WriteLine(t *testing.T) {
	var b bytes.Buffer

	sk := river.SeriesKey("rooms", map[string]string{"room": "r1", "building": "b1"})

	river.WriteLine(&b, sk, []river.Field{
		river.Bool{Name: []byte("lights"), Value: true},
		river.Int{Name: []byte("occupants"), Value: int64(3)},
		river.Float{Name: []byte("temp_f"), Value: 72.5},
		river.String{Name: []byte("meeting_name"), Value: []byte("bikeshed")},
	}, int64(1435362189575692182))

	// Tag keys are alphabetized, fields are taken in order, time as entered
	exp := `rooms,building=b1,room=r1 lights=T,occupants=3i,temp_f=72.5,meeting_name=bikeshed 1435362189575692182`

	got := string(b.Bytes())
	if got != exp {
		t.Fatalf("got: %s, exp: %s", got, exp)
	}
}
