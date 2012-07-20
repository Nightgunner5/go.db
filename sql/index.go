// Example queries:
//
// (Spaces are important)
//
//     SELECT myField, some_other_field WHERE ID = ?
//
//     INDEX STRING myField
package godbsql

import (
	"database/sql/driver"
	"errors"
	"strings"
)

type goDBIndexStmtString struct {
	goDBStmt
	field string
}

func (stmt *goDBIndexStmtString) Exec([]driver.Value) (result driver.Result, err error) {
	defer func() {
		if recover() != nil {
			result = nil
			err = errors.New("Invalid type in indexed field")
		}
	}()
	stmt.conn.internal.IndexString(stmt.field)
	return goDBResult(0), nil
}

func (goDBIndexStmtString) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("Not a query statement")
}

type goDBIndexStmtInt struct {
	goDBStmt
	field string
}

func (stmt *goDBIndexStmtInt) Exec([]driver.Value) (result driver.Result, err error) {
	defer func() {
		if recover() != nil {
			result = nil
			err = errors.New("Invalid type in indexed field")
		}
	}()
	stmt.conn.internal.IndexInt(stmt.field)
	return goDBResult(0), nil
}

func (goDBIndexStmtInt) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("Not a query statement")
}

type goDBIndexStmtUint struct {
	goDBStmt
	field string
}

func (stmt *goDBIndexStmtYUint) Exec([]driver.Value) (result driver.Result, err error) {
	defer func() {
		if recover() != nil {
			result = nil
			err = errors.New("Invalid type in indexed field")
		}
	}()
	stmt.conn.internal.IndexUint(stmt.field)
	return goDBResult(0), nil
}

func (goDBIndexStmtUint) Query([]driver.Value) (driver.Rows, error) {
	return nil, errors.New("Not a query statement")
}
