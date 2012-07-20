// A frontend to GoDB following the lead of the NoSQL movement.
package godb

import (
	"bytes"
	"encoding/gob"
	"github.com/Nightgunner5/go.db/bplus"
	"os"
	"reflect"
	"sync"
)

// The value type used by GoDB
type M map[string]interface{}

// The key type used by GoDB
type K bplus.BPlusKey

func Open(filename string) (*GoDB, error) {
	db := new(GoDB)
	var err error

	db.nodes, err = os.OpenFile(filename+".dbn", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	db.values, err = os.OpenFile(filename+".dbv", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	db.indices = make(map[string]*index)

	return db, nil
}

type GoDB struct {
	nodes   *os.File
	values  *os.File
	indices map[string]*index
	mtx     sync.RWMutex
}

// Closes the underlying os.File.
func (db *GoDB) Close() error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if err := db.nodes.Close(); err != nil {
		return err
	}

	if err := db.values.Close(); err != nil {
		return err
	}

	return nil
}

func decode(val bplus.BPlusValue) M {
	if val == nil {
		return nil
	}

	r := bytes.NewReader(val)
	decoder := gob.NewDecoder(r)

	m := make(M)
	decoder.Decode(&m)

	return m
}

func encode(val M) bplus.BPlusValue {
	if val == nil {
		return nil
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	encoder.Encode(val)

	return bplus.BPlusValue(buf.Bytes())
}

// Returns the value for the given key, or nil if the key does not appear in the database.
func (db *GoDB) Get(key K) M {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	val := bplus.Search(db.nodes, db.values, bplus.BPlusKey(key))

	return decode(val)
}

// Returns an Iterator pointing to the first value in the data set.
func (db *GoDB) First() Iterator {
	return &iterator{bplus.GetAll(db.nodes, db.values), db}
}

func (db *GoDB) Insert(value M) K {
	key := K(bplus.InsertAtEnd(db.nodes, db.values, encode(value)))

	for _, idx := range db.indices {
		idx.insert(key, value)
	}

	return key
}

func reduceKeys(left, right []K) []K {
	keys := make([]K, 0, len(left))
	for _, l := range left {
		for _, r := range right {
			if l == r {
				keys = append(keys, l)
				break
			}
		}
	}
	return keys
}

func (db *GoDB) Find(query M) []K {
	keys := make([]K, 0)
	indexed := make([]string, 0)
	notIndexed := make(M)
	for field, value := range query {
		if _, ok := db.indices[field]; ok {
			indexed = append(indexed, field)
		} else {
			notIndexed[field] = value
		}
	}
	if len(indexed) > 0 {
		keys = db.indices[indexed[0]].find(query[indexed[0]])
		for i := 1; i < len(indexed); i++ {
			keys = reduceKeys(keys, db.indices[indexed[0]].find(query[indexed[0]]))
		}
		for field, value := range notIndexed {
			filtered := make([]K, 0, len(keys))
			for _, key := range keys {
				if reflect.DeepEqual(db.Get(key)[field], value) {
					filtered = append(filtered, key)
				}
			}
			keys = filtered
		}
	} else {
		for it := db.First(); it.Valid(); it.Next() {
			match := true
			for field, value := range query {
				if !reflect.DeepEqual(it.Value()[field], value) {
					match = false
					break
				}
			}
			if match {
				keys = append(keys, it.Key())
			}
		}
	}
	return keys
}
