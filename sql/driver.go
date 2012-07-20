package godbsql

import (
	"github.com/Nightgunner5/go.db"
	"database/sql"
	"database/sql/driver"
)

type goDBDriver byte
func init() {
	sql.Register("godb", goDBDriver(0))
}

func (goDBDriver) Open(name string) (driver.Conn, error) {
	db, err := godb.Open(name)
	if err != nil {
		return nil, err
	}
	return &goDBConn{db}, nil
}

type goDBConn struct {
	internal *godb.GoDB
}

func (conn *goDBConn) Close() error {
	return conn.internal.Close()
}

func (conn *goDBConn) Prepare(query string) (driver.Stmt, error) {
	return parseQuery(conn, query)
}

func (conn *goDBConn) Begin() (driver.Tx, error) {
	panic("TODO")
}

