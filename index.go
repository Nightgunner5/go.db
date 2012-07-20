package godb

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
	if s, ok := in.(string); ok {
		return idx._element == s
	}
	return false
}

func (idx *index_element_string) less(in interface{}) bool {
	if s, ok := in.(string); ok {
		return idx._element < s
	}
	return false
}

// Adds a string index. !!! Indexes are not currently saved, so they must be rebuilt on startup. This will change.
func (db *GoDB) IndexString(field string) {
	idx := &index{
		field,
		nil,
		func(key K, value M) index_element {
			return &index_element_string{key, value[field].(string)}
		},
	}
	for it := db.First(); it.Valid(); it.Next() {
		idx.insert(it.Key(), it.Value())
	}
	db.indices[field] = idx
}
