package bplus

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestSearchExist(t *testing.T) {
	t.Parallel()

	nodes, _ := ioutil.TempFile(os.TempDir(), "bplus_search_test")
	defer os.Remove(nodes.Name())

	values, _ := ioutil.TempFile(os.TempDir(), "bplus_search_test")
	defer os.Remove(values.Name())

	var invalid, val BPlusValue

	invalid = append(invalid, []byte("INVALID INVALID BACON INVALID")...)
	val = append(val, []byte("This is a valid value.")...)

	for i := 0; i < 5; i++ {
		Insert(nodes, values, BPlusKey(i), invalid)
	}
	Insert(nodes, values, 5, val)
	for i := 6; i < 10; i++ {
		Insert(nodes, values, BPlusKey(i), invalid)
	}

	found := Search(nodes, values, 5)
	if found == nil {
		t.Error("found == nil")
	}
	if !found.Equal(val) {
		t.Log("found  ", found)
		t.Log("wanted ", val)
		t.Fail()
	}
}

func TestSearchNotExist(t *testing.T) {
	t.Parallel()

	nodes, _ := ioutil.TempFile(os.TempDir(), "bplus_search_test")
	defer os.Remove(nodes.Name())

	values, _ := ioutil.TempFile(os.TempDir(), "bplus_search_test")
	defer os.Remove(values.Name())

	var val BPlusValue

	val = append(val, []byte("Test123")...)

	for i := 0; i < 10; i++ {
		Insert(nodes, values, BPlusKey(i), val)
	}

	found := Search(nodes, values, 10)
	if found != nil {
		t.Error("found ", found)
	}
}

func BenchmarkSearch(b *testing.B) {
	b.StopTimer()

	nodes, _ := ioutil.TempFile(os.TempDir(), "bplus_search_benchmark")
	defer os.Remove(nodes.Name())

	values, _ := ioutil.TempFile(os.TempDir(), "bplus_search_benchmark")
	defer os.Remove(values.Name())

	var val BPlusValue

	val = append(val, []byte("Test123")...)

	for i := 0; i < 1000; i++ {
		Insert(nodes, values, BPlusKey(i), val)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		Search(nodes, values, BPlusKey(i%1000))
	}
}

func BenchmarkGetAll(b *testing.B) {
	b.StopTimer()

	nodes, _ := ioutil.TempFile(os.TempDir(), "bplus_search_benchmark")
	defer os.Remove(nodes.Name())

	values, _ := ioutil.TempFile(os.TempDir(), "bplus_search_benchmark")
	defer os.Remove(values.Name())

	var val BPlusValue

	val = append(val, []byte("Test123")...)

	for i := 0; i < 1000; i++ {
		Insert(nodes, values, BPlusKey(i), val)
	}

	b.StartTimer()

	// One iteration of BenchmarkGetAll = one Search call in BenchmarkSearch. This is done so the values are comparable.
	for i := 0; i < b.N/1000+1; i++ {
		for it := GetAll(nodes, values); it.Valid(); it.Next() {
			it.Value()
		}
	}
}
