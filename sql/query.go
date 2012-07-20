// Example queries:
//
// (Spaces are important)
//
//     SELECT ID, some_other_field WHERE myField = ?
//
//     INSERT myField, some_other_field
//
//     INDEX STRING myField
//
// To prevent SQL injection attacks, all values must be parameters. The INSERT query is much simpler than the usual SQL way.
package godbsql

import (
	"database/sql/driver"
	"errors"
	"strings"
)

func parseFields(words []string) (remaining, fields []string) {
	for i, word := range words {
		if word[len(word)-1] == ',' {
			fields = append(fields, string(word[:len(word)-1]))
		} else {
			fields = append(fields, word)
			remaining = words[i+1:]
			return
		}
	}
	return
}

func parseQuery(conn *goDBConn, query string) (driver.Stmt, error) {
	words := strings.Split(query, " ")
	if len(words) < 1 {
		return nil, errors.New("No command given")
	}
	switch strings.ToUpper(words[0]) {
	case "SELECT":
		return nil, errors.New("TODO")
	case "INSERT":
		words, fields := parseFields(words[1:])
		if len(words) != 0 {
			return nil, errors.New("Extra garbage after end of query")
		}
		for _, field := range fields {
			if field == "ID" {
				return nil, errors.New("ID is not a valid field name for INSERT.")
			}
		}

		return &goDBInsertStmt{goDBStmt{conn, len(fields)}, fields}, nil
	case "INDEX":
		if len(words) < 3 {
			return nil, errors.New("Expected type after INDEX")
		}
		if len(words) > 3 {
			if words[1] == "SIGNED" || words[1] == "UNSIGNED" {
				if len(words) != 4 {
					return nil, errors.New("Extra garbage after end of query")
				}
			} else {
				return nil, errors.New("Extra garbage after end of query")
			}
		}
		if words[2] == "ID" || (len(words) > 3 && words[3] == "ID") {
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

type goDBResult int64

func (res goDBResult) RowsAffected() (int64, error) {
	return int64(res), nil
}

func (goDBResult) LastInsertId() (int64, error) {
	return 0, errors.New("Not an insert query")
}
