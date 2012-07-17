package godb

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestDBMake(t *testing.T) {
	t.Parallel()

	f, err := ioutil.TempFile(os.TempDir(), "godbtest")
	defer os.Remove(f.Name())
	if err != nil {
		t.Error(err)
	}

	db := Open(f.Name())

	q, err := db.Query(nil)
	if err != nil {
		t.Error(err)
	}
	if len(q) != 0 {
		t.Error("Empty database has ", len(q), " rows!")
		t.Log(q)
	}
}

func TestDBInsert(t *testing.T) {
	t.Parallel()

	f, err := ioutil.TempFile(os.TempDir(), "godbtest")
	defer os.Remove(f.Name())
	if err != nil {
		t.Error(err)
	}

	db := Open(f.Name())

	row := M{
		"a": 123,
		"b": "TEST",
		"c": []byte{123, 45, 67, 89},
	}

	err = db.Insert(row)
	if err != nil {
		t.Error(err)
	}

	q, err := db.Query(nil)
	if err != nil {
		t.Error(err)
	}
	if len(q) != 1 || !matches(row, q[0]) {
		t.Error("Got(", q, ") Expected(", []M{row}, ")")
	}

	q, err = db.Query(row)
	if err != nil {
		t.Error(err)
	}
	if len(q) != 1 || !matches(row, q[0]) {
		t.Error("Got(", q, ") Expected(", []M{row}, ")")
	}
}

func BenchmarkQuery(b *testing.B) {
	b.StopTimer()

	f, _ := ioutil.TempFile(os.TempDir(), "godbbench")
	defer os.Remove(f.Name())

	db := Open(f.Name())

	row := M{
		"a": 123,
		"b": "TEST",
		"c": []byte{123, 45, 67, 89},
	}

	b.SetBytes(SECTION_SIZE * 100)
	for i := 0; i < 100; i++ {
		db.Insert(row)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		db.Query(nil)
	}
}

func BenchmarkInsert(b *testing.B) {
	b.StopTimer()

	f, _ := ioutil.TempFile(os.TempDir(), "godbbench")
	defer os.Remove(f.Name())

	db := Open(f.Name())

	row := M{
		"a": 123,
		"b": "TEST",
		"c": []byte{123, 45, 67, 89},
	}

	b.SetBytes(SECTION_SIZE)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		db.Insert(row)
	}
}
