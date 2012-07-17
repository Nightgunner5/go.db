package godb

import (
	"database/sql"
)

func init() {
	sql.Register("go.db", theDriver)
}

func Open(dataSourceName string) (*sql.DB, error) {
	return sql.Open("go.db", dataSourceName)
}
