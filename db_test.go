package godb

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func cleanupDB(db *GoDB, f *os.File) {
	db.Close()
	os.Remove(f.Name())
	os.Remove(f.Name() + ".dbv")
	os.Remove(f.Name() + ".dbn")
}

func makeDB() (db *GoDB, f *os.File) {
	f, _ = ioutil.TempFile(os.TempDir(), "godb_test_")
	db, _ = Open(f.Name())
	return
}

func TestInsert(t *testing.T) {
	t.Parallel()
	db, f := makeDB()
	defer cleanupDB(db, f)

	id := db.Insert(M{
		"a":   0xbc,
		"one": "two three",
	})

	if id != 1 {
		t.Error("Expected id = 1, but id = ", id)
	}
}

func TestGet(t *testing.T) {
	t.Parallel()
	db, f := makeDB()
	defer cleanupDB(db, f)

	db.Insert(M{
		"value": "too early",
	})
	db.Insert(M{
		"a":   0xbc,
		"one": "two three",
	})
	db.Insert(M{
		"value": "too late",
	})

	val := db.Get(2)
	expected := M{"a": 0xbc, "one": "two three"}
	if !reflect.DeepEqual(val, expected) {
		t.Log("Expected: ", expected)
		t.Log("Found:    ", val)
		t.Fail()
	}
}

func TestFindNoIndex(t *testing.T) {
	t.Parallel()
	db, f := makeDB()
	defer cleanupDB(db, f)

	db.Insert(M{
		"a": "bc",
	})
	db.Insert(M{
		"a": "de",
	})
	db.Insert(M{
		"a": "fg",
	})

	id := db.Find(M{"a": "de"})
	if len(id) != 1 || id[0] != 2 {
		t.Error("Expected id = [2], but id = ", id)
	}
}

func TestFindWithIndex(t *testing.T) {
	t.Parallel()
	db, f := makeDB()
	defer cleanupDB(db, f)

	db.Insert(M{
		"a": "bc",
	})
	db.Insert(M{
		"a": "de",
	})
	db.Insert(M{
		"a": "fg",
	})

	db.IndexString("a")

	id := db.Find(M{"a": "de"})
	if len(id) != 1 || id[0] != 2 {
		t.Error("Expected id = [2], but id = ", id)
	}
}
