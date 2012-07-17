package godb

type GoDBIter struct {
	db          *GoDB
	sectionID   int64
	sectionData M
	lastError   error
}

func (iter *GoDBIter) Next() {
	iter.sectionData, iter.lastError = iter.db.readSection(iter.sectionID)
	iter.sectionID++
	for iter.lastError == ErrInvalidLocation {
		iter.sectionData, iter.lastError = iter.db.readSection(iter.sectionID)
		iter.sectionID++
	}
}

func (iter *GoDBIter) LastError() error {
	return iter.lastError
}

func (iter *GoDBIter) Valid() bool {
	return iter.lastError == nil
}

func (iter *GoDBIter) Get() M {
	if iter.lastError != nil {
		panic(iter.lastError)
	}
	return iter.sectionData
}
