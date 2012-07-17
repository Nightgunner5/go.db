// Adapted from https://github.com/bradfitz/go-sql-test/blob/master/src/sqltest/sql_test.go

package godb

import (
	"fmt"
	"testing"
	"time"
)

func TestBlobStorage(t *testing.T) {
	t.Parallel()

	var blob = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	db, _ := Open(":memory:")

	_, err := db.Exec("CREATE TABLE foo (id uint,bar [16]byte)")
	if err != nil {
		t.Error(err)
	}
	_, err = db.Exec("INSERT INTO foo (id,bar)", 0, blob)
	if err != nil {
		t.Error(err)
	}

	rows, err := db.Query("SELECT bar FROM foo WHERE id=?", 0)
	if err != nil {
		t.Error(err)
	}
	var b [16]byte
	rowcount := 0
	for rows.Next() {
		rowcount++
		rows.Scan(&b)
	}

	if rowcount != 1 {
		t.Error("Row count is ", rowcount, " (should be 1)")
	}

	want := fmt.Sprintf("%x", blob)
	got := fmt.Sprintf("%x", b)
	if err != nil {
		t.Error(err)
	} else if want != got {
		t.Error("Expected(", want, ") but Got(", got, ")")
	}
}

func BenchmarkQueryPrepared(b *testing.B) {
	b.StopTimer()
	db, _ := Open(":memory:")
	db.Exec("CREATE TABLE foo (id uint,name string)")
	stmt, _ := db.Prepare("INSERT INTO foo (id,name)")
	stmt.Exec(1, "bob")
	stmt.Exec(2, "larry")
	stmt.Exec(5, "george")

	stmt, _ = db.Prepare("SELECT name FROM foo WHERE id=?")
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		stmt.Query(1)
	}
}

func BenchmarkQueryUnprepared(b *testing.B) {
	b.StopTimer()
	db, _ := Open(":memory:")
	db.Exec("CREATE TABLE foo (id uint,name string)")
	stmt, _ := db.Prepare("INSERT INTO foo (id,name)")
	stmt.Exec(1, "bob")
	stmt.Exec(2, "larry")
	stmt.Exec(5, "george")
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		db.Query("SELECT name FROM foo WHERE id=?", 1)
	}
}

func TestTransaction(t *testing.T) {
	t.Parallel()

	db, _ := Open(":memory:")
	_, err := db.Exec("CREATE TABLE foo (id uint,bar string,baz time)")
	if err != nil {
		t.Error(err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Error(err)
	}

	_, err = tx.Exec("INSERT INTO foo (id,bar,baz)", 1, "test", time.Time{})
	if err != nil {
		t.Error(err)
	}

	rows, err := tx.Query("SELECT bar FROM foo WHERE id=?", 1)
	if err != nil {
		t.Error(err)
	}
	rowcount := 0
	for rows.Next() {
		rowcount++
		var bar string
		rows.Scan(&bar)
		if bar != "test" {
			t.Error("Expected(", "test", ") but Got(", bar, ")")
		}
	}
	if rowcount != 1 {
		t.Error("Row count is ", rowcount, " (should be 1)")
	}

	tx.Rollback()

	rows, err = tx.Query("SELECT bar FROM foo WHERE id=?", 1)
	if err != nil {
		t.Error(err)
	}
	rowcount = 0
	for rows.Next() {
		rowcount++
	}
	if rowcount != 0 {
		t.Error("Row count is ", rowcount, " (should be 0)")
	}
}
