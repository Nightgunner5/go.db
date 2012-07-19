package godb

import "github.com/Nightgunner5/go.db/bplus"

type Iterator interface {
	// Returns true if the Iterator is at a valid location (within the bounds of the database).
	Valid() bool

	// Equivelent to calling Next() n times, but faster due to less mutex locking and unlocking.
	Skip(n int)

	// Advance the Iterator by one, changing Key() and Value() to the next key and value or
	// setting Valid() to false if the end of the database is reached.
	Next()

	// Returns the key of the current entry. Panics if !Valid().
	Key() K

	// Returns the value of the current entry. Panics if !Valid().
	Value() M
}

type iterator struct {
	it bplus.Iterator
	db *GoDB
}

var _ Iterator = new(iterator)

func (it *iterator) Valid() bool {
	return it.it.Valid()
}

func (it *iterator) Skip(n int) {
	it.db.mtx.RLock()
	defer it.db.mtx.RUnlock()

	for i := 0; i < n; i++ {
		it.it.Next()
	}
}

func (it *iterator) Next() {
	it.db.mtx.RLock()
	defer it.db.mtx.RUnlock()

	it.it.Next()
}

func (it *iterator) Value() M {
	if !it.Valid() {
		panic("Value() called on invalid Iterator")
	}

	it.db.mtx.RLock()
	defer it.db.mtx.RUnlock()

	return decode(it.it.Value())
}

func (it *iterator) Key() K {
	if !it.Valid() {
		panic("Key() called on invalid Iterator")
	}
	return K(it.it.Key())
}
