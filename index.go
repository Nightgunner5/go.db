package godb

import "github.com/Nightgunner5/go.db/bplus"

type index struct {
	field    string
	elements []index_element
	convert  func(K, M) index_element
}

type index_element interface {
	key() K
	element() interface{}
	equal(interface{}) bool
	less(interface{}) bool
}

func (idx *index) insert(key K, value M) {
	el := idx.convert(key, value)

	min, max := 0, len(idx.elements)-1
	index := max / 2

	for min < max {
		if idx.elements[index].equal(el.element()) {
			break
		}
		if idx.elements[index].less(el.element()) {
			min = index + 1
		} else {
			max = index - 1
		}
		index = (max-min)/2 + min
	}

	idx.elements = append(idx.elements[:index], append([]index_element{el}, idx.elements[index:]...)...)
}

func (idx *index) remove(key K, value M) {
	el := idx.convert(key, value)

	min, max := 0, len(idx.elements)-1
	index := max / 2

	for min < max {
		if idx.elements[index].equal(el.element()) {
			break
		}
		if idx.elements[index].less(el.element()) {
			min = index + 1
		} else {
			max = index - 1
		}
		index = (max-min)/2 + min
	}

	for index > 0 && idx.elements[index-1].equal(el.element()) {
		index--
	}

	for index < len(idx.elements) && idx.elements[index].equal(el.element()) {
		if idx.elements[index].key() == key {
			idx.elements = append(idx.elements[:index], idx.elements[index+1:]...)
			return
		}
		index++
	}
}

func (idx *index) find(value interface{}) []K {
	min, max := 0, len(idx.elements)-1
	index := max / 2

	for min < max {
		if idx.elements[index].equal(value) {
			break
		}
		if idx.elements[index].less(value) {
			min = index + 1
		} else {
			max = index - 1
		}
		index = (max-min)/2 + min
	}

	for index > 0 && idx.elements[index-1].equal(value) {
		index--
	}

	keys := make([]K, 0)
	for index < len(idx.elements) && idx.elements[index].equal(value) {
		keys = append(keys, idx.elements[index].key())
		index++
	}

	return keys
}

type index_element_string struct {
	_key     K
	_element string
}

var _ index_element = new(index_element_string)

func (idx *index_element_string) key() K {
	return idx._key
}

func (idx *index_element_string) element() interface{} {
	return idx._element
}

func (idx *index_element_string) equal(in interface{}) bool {
	return idx._element == in
}

func (idx *index_element_string) less(in interface{}) bool {
	return idx._element < in.(string)
}

type index_element_int struct {
	_key     K
	_element int64
}

var _ index_element = new(index_element_int)

func (idx *index_element_int) key() K {
	return idx._key
}

func (idx *index_element_int) element() interface{} {
	return idx._element
}

func (idx *index_element_int) equal(in interface{}) bool {
	return idx._element == in
}

func (idx *index_element_int) less(in interface{}) bool {
	switch i := in.(type) {
	case int:
		return idx._element == int64(i)
	case int8:
		return idx._element == int64(i)
	case int16:
		return idx._element == int64(i)
	case int32:
		return idx._element == int64(i)
	case int64:
		return idx._element == int64(i)
	}
	panic("Non-int in int-indexed field")
}

type index_element_uint struct {
	_key     K
	_element uint64
}

var _ index_element = new(index_element_uint)

func (idx *index_element_uint) key() K {
	return idx._key
}

func (idx *index_element_uint) element() interface{} {
	return idx._element
}

func (idx *index_element_uint) equal(in interface{}) bool {
	return idx._element == in
}

func (idx *index_element_uint) less(in interface{}) bool {
	switch i := in.(type) {
	case uint:
		return idx._element == uint64(i)
	case uint8:
		return idx._element == uint64(i)
	case uint16:
		return idx._element == uint64(i)
	case uint32:
		return idx._element == uint64(i)
	case uint64:
		return idx._element == uint64(i)
	}
	panic("Non-uint in uint-indexed field")
}

func (db *GoDB) indexgeneric(field string, convert func(K, M) index_element) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	idx := &index{field, nil, convert}

	for it := bplus.GetAll(db.nodes, db.values); it.Valid(); it.Next() {
		idx.insert(K(it.Key()), decode(it.Value()))
	}

	db.indices[field] = idx
}

// Adds a string index. !!! Indexes are not currently saved, so they must be rebuilt on startup. This will change.
func (db *GoDB) IndexString(field string) {
	db.indexgeneric(field, func(key K, value M) index_element {
		return &index_element_string{key, value[field].(string)}
	})
}

// Adds a signed integer index.
func (db *GoDB) IndexInt(field string) {
	db.indexgeneric(field, func(key K, value M) index_element {
		var val int64
		switch v := value[field].(type) {
		case int:
			val = int64(v)
		case int8:
			val = int64(v)
		case int16:
			val = int64(v)
		case int32:
			val = int64(v)
		case int64:
			val = int64(v)
		default:
			panic("Non-int value in int-indexed field " + field)
		}
		return &index_element_int{key, val}
	})
}

// Adds an unsigned integer index.
func (db *GoDB) IndexUint(field string) {
	db.indexgeneric(field, func(key K, value M) index_element {
		var val uint64
		switch v := value[field].(type) {
		case uint:
			val = uint64(v)
		case uint8:
			val = uint64(v)
		case uint16:
			val = uint64(v)
		case uint32:
			val = uint64(v)
		case uint64:
			val = uint64(v)
		default:
			panic("Non-uint value in uint-indexed field " + field)
		}
		return &index_element_uint{key, val}
	})
}

func (db *GoDB) HasIndex(field string) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	_, ok := db.indices[field]

	return ok
}
