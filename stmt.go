package godb

import "database/sql/driver"

type godbStmt struct {
	conn  *godbConn
	query godbQuery
}

func (godbStmt) Close() error {
	return nil
}

func (stmt *godbStmt) Exec(args []driver.Value) (driver.Result, error) {
	return stmt.query.exec(stmt.conn, args)
}

func (stmt *godbStmt) NumInput() int {
	return stmt.query.numargs()
}

func (stmt *godbStmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.query.query(stmt.conn, args)
}
