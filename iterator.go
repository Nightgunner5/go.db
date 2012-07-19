package godb

import "github.com/Nightgunner5/go.db/bplus"

type Iterator interface {
	Valid() bool
	Skip(int)
	Next()
	Value() M
	Key() K
}

type iterator struct {
	it bplus.Iterator
}

var _ Iterator = new(iterator)

func (it *iterator) Valid() bool {
	return it.it.Valid()
}

func (it *iterator) Skip(n int) {
	for i := 0; i < n; i++ {
		it.it.Next()
	}
}

func (it *iterator) Next() {
	it.it.Next()
}

func (it *iterator) Value() M {
	return decode(it.it.Value())
}

func (it *iterator) Key() K {
	return K(it.it.Key())
}
