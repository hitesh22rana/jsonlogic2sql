package main

import (
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
			wantPat:    "hello",
		},
		{
			name:       "normal order with escaped quote",
			args:       []interface{}{"col", "'it''s'"},
			wantColumn: "col",
			wantPat:    "it's",
		},
		{
			name:       "reversed order (quoted pattern first)",
			args:       []interface{}{"'it''s'", "col"},
			wantColumn: "col",
			wantPat:    "it's",
		},
		{
			name:       "array pattern",
			args:       []interface{}{"col", "['it''s']"},
			wantColumn: "col",
			wantPat:    "it's",
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
