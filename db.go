package godb

import (
	"bytes"
	"encoding/gob"
	"github.com/Nightgunner5/go.db/bplus"
	"os"
	"sync"
)

// Shortcut for less ugly code
type M map[string]interface{}
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
	return db, nil
}

type GoDB struct {
	nodes  *os.File
	values *os.File
	mtx    sync.RWMutex
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

	encoder.Encode(m)
	encoder.Close()

	return bplus.BPlusValue(buf.Bytes())
}

func (db *GoDB) Get(key K) M {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	val := bplus.Search(db.nodes, db.values, bplus.BPlusKey(key))

	return decode(val)
}

func (db *GoDB) First() Iterator {
	return &iterator{btree.GetAll(db.nodes, db.values)}
}

func (db *GoDB) Insert(value M) K {
	panic("TODO")
}
