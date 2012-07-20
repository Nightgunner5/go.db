package godbsql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/Nightgunner5/go.db"
)

type goDBInsertStmt struct {
	goDBStmt
	fields []string
}

var _ driver.Stmt = new(goDBInsertStmt)

func (stmt *goDBInsertStmt) Exec(args []driver.Value) (result driver.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("Index type check failed: %s", r)
		}
	}()

	m := make(godb.M)

	for i, field := range stmt.fields {
		m[field] = args[i]
	}

	return goDBInsertResult(stmt.conn.internal.Insert(m)), nil
}

func (goDBInsertStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Not a query statement")
}

type goDBInsertResult int64

func (res goDBInsertResult) LastInsertId() (int64, error) {
	return int64(res), nil
}

func (goDBInsertResult) RowsAffected() (int64, error) {
	return 1, nil
}
