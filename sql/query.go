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

func parseQuery(conn *goDBConn, query string) (driver.Stmt, error) {
	words := strings.Split(query, " ")
	if len(words) < 1 {
		return nil, errors.New("No command given")
	}
	switch strings.ToUpper(words[0]) {
	case "SELECT":
		return nil, errors.New("TODO")
	case "INSERT":
		return nil, errors.New("TODO")
	case "INDEX":
		if len(words) < 3 {
			return nil, errors.New("Expected type after INDEX")
		}
		if words[2] == "ID" {
			return nil, errors.New("ID cannot be affected by user-supplied indexes.")
		}
		switch strings.ToUpper(words[1]) {
		case "STRING":
			return &goDBIndexStmtString{goDBStmt{conn, 0}, words[2]}, nil
		case "SIGNED":
			if len(words) > 3 && strings.ToUpper(words[2]) == "INTEGER" || strings.ToUpper(words[2]) == "INT" {
				return &goDBIndexStmtInt{goDBStmt{conn, 0}, words[3]}, nil
			}
		case "INTEGER", "INT":
			return &goDBIndexStmtInt{goDBStmt{conn, 0}, words[2]}, nil
		case "UNSIGNED":
			if len(words) > 3 && strings.ToUpper(words[2]) == "INTEGER" || strings.ToUpper(words[2]) == "INT" {
				return &goDBIndexStmtUint{goDBStmt{conn, 0}, words[3]}, nil
			}
		case "UINT":
			return &goDBIndexStmtUint{goDBStmt{conn, 0}, words[2]}, nil
		}
		return nil, errors.New("Unknown type after INDEX")
	}
	return nil, errors.New("Unknown command")
}

type goDBParameter uint

type goDBStmt struct {
	conn *goDBConn
	args int
}

func (goDBStmt) Close() error {
	return nil
}
func (stmt *goDBStmt) NumInput() int {
	return stmt.args
}

type goDBSearchStmt struct {
}

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

type goDBResult int64

func (res goDBResult) RowsAffected() (int64, error) {
	return int64(res), nil
}

func (goDBResult) LastInsertId() (int64, error) {
	return 0, errors.New("Not an insert query")
}
