package godb

import (
	"database/sql/driver"
	"os"
)

type godbDriver int

var theDriver driver.Driver = godbDriver(0)

func (godbDriver) Open(name string) (driver.Conn, error) {
	var file *os.File
	var err error
	if name != ":memory:" {
		file, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
	}
	return &godbConn{file, make(map[string][]map[string]driver.Value)}, nil
}

type godbConn struct {
	file  *os.File
	cache map[string][]map[string]driver.Value
}

func (conn *godbConn) Prepare(query string) (driver.Stmt, error) {
	parsed, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	return &godbStmt{conn, parsed}, nil
}

func (conn *godbConn) Close() error {
	if conn.file == nil {
		return nil
	}
	return conn.file.Close()
}

func (conn *godbConn) Begin() (driver.Tx, error) {
	panic("TODO")
	//return &godbTx{conn}, nil
}

type godbTx struct {
	conn *godbConn
}
