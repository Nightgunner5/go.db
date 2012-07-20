package godbsql

import "testing"

func TestInvalidSyntax(t *testing.T) {
	t.Parallel()

	conn := openForTesting()
	defer conn.Close()

	_, err := conn.Exec("HI MOM")
	if err == nil {
		t.Error("Obviously invalid syntax (HI MOM) did not cause an error.")
	}
}

// TODO: Uncomment this when SELECT queries are implemented.
/*
func TestRequiredIndex(t *testing.T) {
	t.Parallel()

	conn := openForTesting()
	defer conn.Close()

	conn.Exec("INSERT foo", "bar")
	conn.Exec("INSERT foo", "baz")

	_, err := conn.Prepare("SELECT ID WHERE foo = ?")
	if err == nil {
		t.Error("Non-indexed query did not return an error.")
	}

	conn.Exec("INDEX STRING foo")

	_, err = conn.Prepare("SELECT ID WHERE foo = ?")
	if err != nil {
		t.Error("Indexed query unexpectedly returned error: ", err)
	}
}
*/
