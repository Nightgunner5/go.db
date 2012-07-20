package godbsql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/Nightgunner5/go.db"
	"io/ioutil"
	"os"
)

type goDBDriver byte
type goDBDriverTesting byte

func init() {
	sql.Register("godb", goDBDriver(0))
	sql.Register("godb__testing", goDBDriverTesting(0))
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
	return nil, errors.New("GoDB does not support transactions.")
}

type goDBTestingConn struct {
	db driver.Conn
	f  *os.File
}

func (conn *goDBTestingConn) Close() error {
	defer os.Remove(conn.f.Name())
	defer os.Remove(conn.f.Name() + ".dbv")
	defer os.Remove(conn.f.Name() + ".dbn")
	return conn.db.Close()
}

func (conn *goDBTestingConn) Prepare(query string) (driver.Stmt, error) {
	return conn.db.Prepare(query)
}

func (conn *goDBTestingConn) Begin() (driver.Tx, error) {
	return conn.db.Begin()
}

func (goDBDriverTesting) Open(name string) (driver.Conn, error) {
	f, _ := ioutil.TempFile(os.TempDir(), "godb_sql_test_")
	db, _ := goDBDriver(0).Open(f.Name())
	return &goDBTestingConn{db, f}, nil
}

func openForTesting() *sql.DB {
	db, _ := sql.Open("godb__testing", "")
	return db
}
