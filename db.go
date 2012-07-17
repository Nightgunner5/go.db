package godb

import (
	"io"
	"os"
	"reflect"
	"sync"
)

// Shortcut for less ugly code
type M map[string]interface{}

func Open(filename string) *GoDB {
	db := new(GoDB)
	db.filename = filename
	return db
}

type GoDB struct {
	filename string
	mtx      sync.RWMutex
}

func (db *GoDB) open(mode int) (*os.File, error) {
	return os.OpenFile(db.filename, mode|os.O_CREATE, 0666)
}

func (db *GoDB) readSection(sectionID int64) (M, error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	f, err := db.open(os.O_RDWR)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var b [SECTION_SIZE]byte
	_, err = f.ReadAt(b[:], sectionID*SECTION_SIZE)
	if err != nil {
		return nil, err
	}

	section, err := makeSection(b)
	var i int64
	for err == errReadNextSection {
		i++
		_, err = f.ReadAt(b[:], (sectionID+i)*SECTION_SIZE)
		if err != nil {
			return nil, err
		}
		section, err = appendSection(section, b)
	}
	if err != nil {
		return nil, err
	}

	return section.data, nil
}

func (db *GoDB) Iter() *GoDBIter {
	iter := new(GoDBIter)
	iter.db = db
	iter.Next()
	return iter
}

func matches(query, data M) bool {
	for k, v := range query {
		if !reflect.DeepEqual(data[k], v) {
			return false
		}
	}
	return true
}

// This code is horrible. It will get a rewrite soon. I hope.
func (db *GoDB) Query(query M) ([]M, error) {
	ret := make([]M, 0)
	it := db.Iter()
	for ; it.Valid(); it.Next() {
		val := it.Get()
		if matches(query, val) {
			ret = append(ret, val)
		}
	}
	if it.LastError() == io.EOF {
		return ret, nil
	}
	return ret, it.LastError()
}

func (db *GoDB) QuerySingle(query M) (M, error) {
	q, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	if len(q) == 0 {
		return nil, nil
	}
	return q[0], nil
}

func (db *GoDB) Insert(row M) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	f, err := db.open(os.O_WRONLY | os.O_APPEND)
	if err != nil {
		return err
	}
	defer f.Close()

	return writeSection(f, row)
}
