package godb

import (
	"database/sql/driver"
	"errors"
	"regexp"
	"strings"
)

type godbQuery interface {
	numargs() int
	exec(*godbConn, []driver.Value) (driver.Result, error)
	query(*godbConn, []driver.Value) (driver.Rows, error)
}

func parseQuery(query string) (godbQuery, error) {
	if query[:7] == "SELECT " {
		return parseSelectQuery(query[7:])
	}
	if query[:12] == "INSERT INTO " {
		return parseInsertQuery(query[12:])
	}
	return nil, errors.New("Syntax error")
}

type godbOperator uint8

const (
	_ godbOperator = iota
	opEq
	opNe
	opLt
	opGt
	opLte
	opGte
	opAnd
	opOr
	opXor
	opNot // Left is ignored for opNot
)

type _wildcard struct{}

var wildcard _wildcard

type godbWhereClause struct {
	left     interface{}
	operator godbOperator
	right    interface{}
}

func (where *godbWhereClause) numargs() int {
	args := 0
	if where.left == wildcard {
		args++
	} else if l, ok := where.left.(*godbWhereClause); ok {
		args += l.numargs()
	}
	if where.right == wildcard {
		args++
	} else if r, ok := where.right.(*godbWhereClause); ok {
		args += r.numargs()
	}
	return args
}

type godbSelectQuery struct {
	table    string
	fields   []string
	where    *godbWhereClause
	orderBy  string // field name
	orderAsc bool
	limit    int64
}

// TODO: WHERE syntax
var selectQueryRegexp = regexp.MustCompile(`^([\pL\pN]+(?:,[\pL\pN]+)*) FROM ([\pL\pN]+)( WHERE){0}( ORDER BY (?:[\pL\pN]+) (?:ASC|DESC))?( LIMIT \d+)?$`)

func parseSelectQuery(query string) (godbQuery, error) {
	data := selectQueryRegexp.FindStringSubmatch(query)
	if data == nil {
		return nil, errors.New("Syntax error")
	}
	q := new(godbSelectQuery)
	q.fields = strings.Split(data[1], ",")
	q.table = data[2]
	return q, nil
}

func (q *godbSelectQuery) numargs() int {
	return q.where.numargs()
}

func (q *godbSelectQuery) exec(conn *godbConn, args []driver.Value) (driver.Result, error) {
	return nil, errors.New("Exec called on SELECT statement")
}

func (q *godbSelectQuery) query(conn *godbConn, args []driver.Value) (driver.Rows, error) {
	panic("TODO")
}

type godbInsertQuery struct {
	table  string
	fields []string
}

var insertQueryRegexp = regexp.MustCompile(`^([\pL\pN]+) \(([\pL\pN]+(?:,[\pL\pN]+)*)\)$`)

func parseInsertQuery(query string) (godbQuery, error) {
	data := insertQueryRegexp.FindStringSubmatch(query)
	if data == nil {
		return nil, errors.New("Syntax error")
	}
	q := new(godbInsertQuery)
	q.table = data[1]
	q.fields = strings.Split(data[2], ",")
	return q, nil
}

func (q *godbInsertQuery) numargs() int {
	return len(q.fields)
}

func (q *godbInsertQuery) exec(conn *godbConn, args []driver.Value) (driver.Result, error) {
	panic("TODO")
}

func (q *godbInsertQuery) query(conn *godbConn, args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("Query called on INSERT INTO statement")
}
