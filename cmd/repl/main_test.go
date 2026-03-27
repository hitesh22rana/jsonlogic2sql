package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/h22rana/jsonlogic2sql"
)

func TestUnescapeSQLString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"plain unquoted string", "hello", "hello"},
		{"unquoted with double single quotes", "it''s", "it''s"},
		{"quoted simple", "'hello'", "hello"},
		{"quoted with escaped quote", "'it''s'", "it's"},
		{"quoted with multiple escaped quotes", "'it''s a ''test'''", "it's a 'test'"},
		{"single quote char", "'", "'"},
		{"two single quotes (empty SQL string)", "''", ""},
		{"quoted empty string", "''", ""},
		{"column identifier", "my_column", "my_column"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := unescapeSQLString(tt.input)
			if got != tt.want {
				t.Errorf("unescapeSQLString(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestEscapeLikePattern(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"plain text", "hello", "hello"},
		{"single quote", "it's", "it''s"},
		{"percent", "100%", "100\\%"},
		{"underscore", "a_b", "a\\_b"},
		{"backslash", "a\\b", "a\\\\b"},
		{"mixed special chars", "it's 100% done_now", "it''s 100\\% done\\_now"},
		{"empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := escapeLikePattern(tt.input)
			if got != tt.want {
				t.Errorf("escapeLikePattern(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestExtractFromArrayString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"not an array", "hello", "hello"},
		{"array with unquoted value", "[T]", "T"},
		{"array with quoted value", "['hello']", "hello"},
		{"array with quoted and escaped value", "['it''s']", "it's"},
		{"empty array", "[]", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractFromArrayString(tt.input)
			if got != tt.want {
				t.Errorf("extractFromArrayString(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseContainsArgs(t *testing.T) {
	tests := []struct {
		name       string
		args       []interface{}
		wantColumn string
		wantPat    string
	}{
		{
			name:       "normal order with quoted pattern",
			args:       []interface{}{"col", "'hello'"},
			wantColumn: "col",
			wantPat:    "'hello'",
		},
		{
			name:       "normal order with escaped quote",
			args:       []interface{}{"col", "'it''s'"},
			wantColumn: "col",
			wantPat:    "'it''s'",
		},
		{
			name:       "reversed order (quoted pattern first)",
			args:       []interface{}{"'it''s'", "col"},
			wantColumn: "col",
			wantPat:    "'it''s'",
		},
		{
			name:       "array pattern",
			args:       []interface{}{"col", "['it''s']"},
			wantColumn: "col",
			wantPat:    "'it''s'",
		},
		{
			name:       "placeholder arg (parameterized mode)",
			args:       []interface{}{"col", "@p1"},
			wantColumn: "col",
			wantPat:    "@p1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, pat := parseContainsArgs(tt.args)
			if col != tt.wantColumn || pat != tt.wantPat {
				t.Errorf("parseContainsArgs() = (%q, %q), want (%q, %q)",
					col, pat, tt.wantColumn, tt.wantPat)
			}
		})
	}
}

// setupTestTranspiler creates a transpiler with the same custom operators as the REPL.
func setupTestTranspiler(t *testing.T) *jsonlogic2sql.Transpiler {
	t.Helper()
	tr, err := jsonlogic2sql.NewTranspiler(jsonlogic2sql.DialectBigQuery)
	if err != nil {
		t.Fatalf("NewTranspiler: %v", err)
	}
	registerCustomOperators(tr)
	return tr
}

func TestLikeOperatorsQuoteEscaping(t *testing.T) {
	tr := setupTestTranspiler(t)

	tests := []struct {
		name     string
		jsonExpr string
		want     string
	}{
		{
			name:     "startsWith with apostrophe",
			jsonExpr: `{"startsWith": [{"var": "column"}, "it's"]}`,
			want:     "WHERE column LIKE 'it''s%'",
		},
		{
			name:     "!startsWith with apostrophe",
			jsonExpr: `{"!startsWith": [{"var": "column"}, "it's"]}`,
			want:     "WHERE column NOT LIKE 'it''s%'",
		},
		{
			name:     "endsWith with apostrophe",
			jsonExpr: `{"endsWith": [{"var": "column"}, "it's"]}`,
			want:     "WHERE column LIKE '%it''s'",
		},
		{
			name:     "!endsWith with apostrophe",
			jsonExpr: `{"!endsWith": [{"var": "column"}, "it's"]}`,
			want:     "WHERE column NOT LIKE '%it''s'",
		},
		{
			name:     "contains with apostrophe",
			jsonExpr: `{"contains": [{"var": "column"}, "it's"]}`,
			want:     "WHERE column LIKE '%it''s%'",
		},
		{
			name:     "!contains with apostrophe",
			jsonExpr: `{"!contains": [{"var": "column"}, "it's"]}`,
			want:     "WHERE column NOT LIKE '%it''s%'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tr.Transpile(tt.jsonExpr)
			if err != nil {
				t.Fatalf("Transpile error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Transpile(%s)\n  got:  %s\n  want: %s", tt.jsonExpr, got, tt.want)
			}
		})
	}
}

func TestLikeOperatorsPlainStrings(t *testing.T) {
	tr := setupTestTranspiler(t)

	tests := []struct {
		name     string
		jsonExpr string
		want     string
	}{
		{
			name:     "startsWith plain",
			jsonExpr: `{"startsWith": [{"var": "name"}, "Alice"]}`,
			want:     "WHERE name LIKE 'Alice%'",
		},
		{
			name:     "endsWith plain",
			jsonExpr: `{"endsWith": [{"var": "email"}, "@company.com"]}`,
			want:     "WHERE email LIKE '%@company.com'",
		},
		{
			name:     "contains plain",
			jsonExpr: `{"contains": [{"var": "desc"}, "hello"]}`,
			want:     "WHERE desc LIKE '%hello%'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tr.Transpile(tt.jsonExpr)
			if err != nil {
				t.Fatalf("Transpile error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Transpile(%s)\n  got:  %s\n  want: %s", tt.jsonExpr, got, tt.want)
			}
		})
	}
}

func TestLikeOperatorsWildcardEscaping(t *testing.T) {
	tr := setupTestTranspiler(t)

	tests := []struct {
		name     string
		jsonExpr string
		want     string
	}{
		{
			name:     "contains percent",
			jsonExpr: `{"contains": [{"var": "col"}, "100%"]}`,
			want:     `WHERE col LIKE '%100\%%'`,
		},
		{
			name:     "startsWith underscore",
			jsonExpr: `{"startsWith": [{"var": "col"}, "_private"]}`,
			want:     `WHERE col LIKE '\_private%'`,
		},
		{
			name:     "endsWith mixed apostrophe and wildcard",
			jsonExpr: `{"endsWith": [{"var": "col"}, "it's 100%"]}`,
			want:     `WHERE col LIKE '%it''s 100\%'`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tr.Transpile(tt.jsonExpr)
			if err != nil {
				t.Fatalf("Transpile error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Transpile(%s)\n  got:  %s\n  want: %s", tt.jsonExpr, got, tt.want)
			}
		})
	}
}

func TestContainsReversedWithApostrophe(t *testing.T) {
	tr := setupTestTranspiler(t)

	got, err := tr.Transpile(`{"contains": ["it's", {"var": "column"}]}`)
	if err != nil {
		t.Fatalf("Transpile error: %v", err)
	}
	want := "WHERE column LIKE '%it''s%'"
	if got != want {
		t.Errorf("reversed contains\n  got:  %s\n  want: %s", got, want)
	}
}

func TestPrintParams(t *testing.T) {
	tests := []struct {
		name   string
		params []jsonlogic2sql.QueryParam
		want   string
	}{
		{
			name:   "no params",
			params: nil,
			want:   "Params: (none)\n",
		},
		{
			name:   "empty slice",
			params: []jsonlogic2sql.QueryParam{},
			want:   "Params: (none)\n",
		},
		{
			name: "single string param",
			params: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: "active"},
			},
			want: `Params: [{p1: "active"}]` + "\n",
		},
		{
			name: "single numeric param",
			params: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: float64(1000)},
			},
			want: "Params: [{p1: 1000}]\n",
		},
		{
			name: "multiple mixed params",
			params: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: "active"},
				{Name: "p2", Value: float64(1000)},
			},
			want: `Params: [{p1: "active"}, {p2: 1000}]` + "\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			printParams(tt.params)

			w.Close()
			os.Stdout = old

			var buf bytes.Buffer
			_, _ = buf.ReadFrom(r)
			got := buf.String()
			if got != tt.want {
				t.Errorf("printParams() output:\n  got:  %q\n  want: %q", got, tt.want)
			}
		})
	}
}

func TestParamsModeToggle(t *testing.T) {
	origMode := paramsMode
	defer func() { paramsMode = origMode }()

	paramsMode = false
	paramsMode = !paramsMode
	if !paramsMode {
		t.Error("expected paramsMode to be true after toggle")
	}
	paramsMode = !paramsMode
	if paramsMode {
		t.Error("expected paramsMode to be false after second toggle")
	}
}

func TestTranspileParameterized_BigQuery(t *testing.T) {
	tr := setupTestTranspiler(t)

	tests := []struct {
		name       string
		jsonExpr   string
		wantSQL    string
		wantParams []jsonlogic2sql.QueryParam
	}{
		{
			name:     "simple equality",
			jsonExpr: `{"==": [{"var": "status"}, "active"]}`,
			wantSQL:  "WHERE status = @p1",
			wantParams: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: "active"},
			},
		},
		{
			name:     "numeric comparison",
			jsonExpr: `{">": [{"var": "amount"}, 1000]}`,
			wantSQL:  "WHERE amount > @p1",
			wantParams: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: float64(1000)},
			},
		},
		{
			name:     "AND with mixed types",
			jsonExpr: `{"and": [{"==": [{"var": "status"}, "pending"]}, {">": [{"var": "amount"}, 5000]}]}`,
			wantSQL:  "WHERE (status = @p1 AND amount > @p2)",
			wantParams: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: "pending"},
				{Name: "p2", Value: float64(5000)},
			},
		},
		{
			name:     "IN array",
			jsonExpr: `{"in": [{"var": "country"}, ["US", "CA"]]}`,
			wantSQL:  "WHERE country IN (@p1, @p2)",
			wantParams: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: "US"},
				{Name: "p2", Value: "CA"},
			},
		},
		{
			name:       "null comparison produces no params",
			jsonExpr:   `{"==": [{"var": "deleted_at"}, null]}`,
			wantSQL:    "WHERE deleted_at IS NULL",
			wantParams: nil,
		},
		{
			name:       "boolean comparison produces no params",
			jsonExpr:   `{"==": [{"var": "active"}, true]}`,
			wantSQL:    "WHERE active = TRUE",
			wantParams: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, params, err := tr.TranspileParameterized(tt.jsonExpr)
			if err != nil {
				t.Fatalf("TranspileParameterized error: %v", err)
			}
			if sql != tt.wantSQL {
				t.Errorf("SQL:\n  got:  %s\n  want: %s", sql, tt.wantSQL)
			}
			if tt.wantParams == nil {
				if len(params) != 0 {
					t.Errorf("Params: got %v, want nil/empty", params)
				}
			} else {
				if len(params) != len(tt.wantParams) {
					t.Fatalf("Params length: got %d, want %d", len(params), len(tt.wantParams))
				}
				for i, want := range tt.wantParams {
					if params[i].Name != want.Name {
						t.Errorf("Param[%d].Name: got %q, want %q", i, params[i].Name, want.Name)
					}
					if fmt.Sprintf("%v", params[i].Value) != fmt.Sprintf("%v", want.Value) {
						t.Errorf("Param[%d].Value: got %v, want %v", i, params[i].Value, want.Value)
					}
				}
			}
		})
	}
}

func TestTranspileParameterized_PostgreSQL(t *testing.T) {
	tr, err := jsonlogic2sql.NewTranspiler(jsonlogic2sql.DialectPostgreSQL)
	if err != nil {
		t.Fatalf("NewTranspiler: %v", err)
	}

	sql, params, err := tr.TranspileParameterized(`{"==": [{"var": "email"}, "alice@example.com"]}`)
	if err != nil {
		t.Fatalf("TranspileParameterized error: %v", err)
	}

	wantSQL := "WHERE email = $1"
	if sql != wantSQL {
		t.Errorf("SQL: got %q, want %q", sql, wantSQL)
	}
	if len(params) != 1 || params[0].Name != "p1" || params[0].Value != "alice@example.com" {
		t.Errorf("Params: got %v, want [{p1 alice@example.com}]", params)
	}
}

func TestTranspileParameterized_CustomOperator(t *testing.T) {
	tr := setupTestTranspiler(t)

	sql, _, err := tr.TranspileParameterized(`{"toLower": [{"var": "name"}]}`)
	if err != nil {
		t.Fatalf("TranspileParameterized error: %v", err)
	}

	wantSQL := "WHERE LOWER(name)"
	if sql != wantSQL {
		t.Errorf("SQL: got %q, want %q", sql, wantSQL)
	}
}

func TestTranspileParameterized_Error(t *testing.T) {
	tr := setupTestTranspiler(t)

	_, _, err := tr.TranspileParameterized(`{invalid json}`)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}

	_, _, err = tr.TranspileParameterized(`{"unknownOp": [1, 2]}`)
	if err == nil {
		t.Fatal("expected error for unknown operator")
	}
}

func TestTranspileParameterized_LikeOperators(t *testing.T) {
	tests := []struct {
		name       string
		jsonExpr   string
		wantSQL    string
		wantParams []jsonlogic2sql.QueryParam
	}{
		{
			name:     "startsWith parameterized",
			jsonExpr: `{"startsWith": [{"var": "name"}, "Al"]}`,
			wantSQL:  "WHERE name LIKE CONCAT(@p1, '%')",
			wantParams: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: "Al"},
			},
		},
		{
			name:     "!startsWith parameterized",
			jsonExpr: `{"!startsWith": [{"var": "name"}, "Al"]}`,
			wantSQL:  "WHERE name NOT LIKE CONCAT(@p1, '%')",
			wantParams: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: "Al"},
			},
		},
		{
			name:     "endsWith parameterized",
			jsonExpr: `{"endsWith": [{"var": "email"}, "@example.com"]}`,
			wantSQL:  "WHERE email LIKE CONCAT('%', @p1)",
			wantParams: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: "@example.com"},
			},
		},
		{
			name:     "!endsWith parameterized",
			jsonExpr: `{"!endsWith": [{"var": "email"}, "@example.com"]}`,
			wantSQL:  "WHERE email NOT LIKE CONCAT('%', @p1)",
			wantParams: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: "@example.com"},
			},
		},
		{
			name:     "contains parameterized",
			jsonExpr: `{"contains": [{"var": "desc"}, "hello"]}`,
			wantSQL:  "WHERE desc LIKE CONCAT('%', @p1, '%')",
			wantParams: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: "hello"},
			},
		},
		{
			name:     "!contains parameterized",
			jsonExpr: `{"!contains": [{"var": "desc"}, "hello"]}`,
			wantSQL:  "WHERE desc NOT LIKE CONCAT('%', @p1, '%')",
			wantParams: []jsonlogic2sql.QueryParam{
				{Name: "p1", Value: "hello"},
			},
		},
	}

	tr := setupTestTranspiler(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, params, err := tr.TranspileParameterized(tt.jsonExpr)
			if err != nil {
				t.Fatalf("TranspileParameterized error: %v", err)
			}
			if sql != tt.wantSQL {
				t.Errorf("SQL:\n  got:  %s\n  want: %s", sql, tt.wantSQL)
			}
			if len(params) != len(tt.wantParams) {
				t.Fatalf("Params length: got %d, want %d", len(params), len(tt.wantParams))
			}
			for i, want := range tt.wantParams {
				if params[i].Name != want.Name {
					t.Errorf("Param[%d].Name: got %q, want %q", i, params[i].Name, want.Name)
				}
				if fmt.Sprintf("%v", params[i].Value) != fmt.Sprintf("%v", want.Value) {
					t.Errorf("Param[%d].Value: got %v, want %v", i, params[i].Value, want.Value)
				}
			}
		})
	}
}

func TestTranspileParameterized_LikeOperators_PlaceholderNotQuoted(t *testing.T) {
	tests := []struct {
		name         string
		dialect      jsonlogic2sql.Dialect
		jsonExpr     string
		quotedShould string
	}{
		{
			name:         "bigquery startsWith placeholder not quoted",
			dialect:      jsonlogic2sql.DialectBigQuery,
			jsonExpr:     `{"startsWith": [{"var": "name"}, "Al"]}`,
			quotedShould: "'@p1%",
		},
		{
			name:         "postgres startsWith placeholder not quoted",
			dialect:      jsonlogic2sql.DialectPostgreSQL,
			jsonExpr:     `{"startsWith": [{"var": "name"}, "Al"]}`,
			quotedShould: "'$1%",
		},
		{
			name:         "duckdb contains placeholder not quoted",
			dialect:      jsonlogic2sql.DialectDuckDB,
			jsonExpr:     `{"contains": [{"var": "name"}, "Al"]}`,
			quotedShould: "'%$1%",
		},
		{
			name:         "clickhouse contains placeholder not quoted",
			dialect:      jsonlogic2sql.DialectClickHouse,
			jsonExpr:     `{"contains": [{"var": "name"}, "Al"]}`,
			quotedShould: "'%@p1%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, err := jsonlogic2sql.NewTranspiler(tt.dialect)
			if err != nil {
				t.Fatalf("NewTranspiler: %v", err)
			}
			registerCustomOperators(tr)

			sql, _, err := tr.TranspileParameterized(tt.jsonExpr)
			if err != nil {
				t.Fatalf("TranspileParameterized error: %v", err)
			}

			if strings.Contains(sql, tt.quotedShould) {
				t.Fatalf("placeholder appears quoted in LIKE pattern, SQL: %s", sql)
			}
		})
	}
}

func TestLikeOperatorsNonParamStillWork(t *testing.T) {
	tr := setupTestTranspiler(t)

	tests := []struct {
		name     string
		jsonExpr string
		want     string
	}{
		{
			name:     "startsWith non-param",
			jsonExpr: `{"startsWith": [{"var": "name"}, "Alice"]}`,
			want:     "WHERE name LIKE 'Alice%'",
		},
		{
			name:     "endsWith non-param",
			jsonExpr: `{"endsWith": [{"var": "email"}, "@company.com"]}`,
			want:     "WHERE email LIKE '%@company.com'",
		},
		{
			name:     "contains non-param",
			jsonExpr: `{"contains": [{"var": "desc"}, "hello"]}`,
			want:     "WHERE desc LIKE '%hello%'",
		},
		{
			name:     "startsWith with apostrophe non-param",
			jsonExpr: `{"startsWith": [{"var": "column"}, "it's"]}`,
			want:     "WHERE column LIKE 'it''s%'",
		},
		{
			name:     "contains with apostrophe non-param",
			jsonExpr: `{"contains": [{"var": "column"}, "it's"]}`,
			want:     "WHERE column LIKE '%it''s%'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tr.Transpile(tt.jsonExpr)
			if err != nil {
				t.Fatalf("Transpile error: %v", err)
			}
			if got != tt.want {
				t.Errorf("Transpile(%s)\n  got:  %s\n  want: %s", tt.jsonExpr, got, tt.want)
			}
		})
	}
}
