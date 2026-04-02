package jsonlogic2sql

import (
	"encoding/json"
	"strings"
	"testing"
)

func decodeLogicMapForArrayScopeIDTest(t *testing.T, logic string) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(logic), &m); err != nil {
		t.Fatalf("json.Unmarshal(map) failed: %v", err)
	}
	return m
}

func decodeLogicAnyForArrayScopeIDTest(t *testing.T, logic string) interface{} {
	t.Helper()
	var v interface{}
	if err := json.Unmarshal([]byte(logic), &v); err != nil {
		t.Fatalf("json.Unmarshal(any) failed: %v", err)
	}
	return v
}

func assertInvalidIdentifierError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected invalid identifier error, got nil")
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "invalid identifier") {
		t.Fatalf("expected invalid identifier error, got: %v", err)
	}
}

func TestArrayScopeIdentifierValidationRejectsMaliciousPaths_AllDialects(t *testing.T) {
	t.Parallel()

	dialects := []Dialect{
		DialectBigQuery,
		DialectSpanner,
		DialectPostgreSQL,
		DialectDuckDB,
		DialectClickHouse,
	}

	cases := []struct {
		name  string
		logic string
	}{
		{
			name:  "map rewritten item dotted payload",
			logic: `{"map":[{"var":"bag.numbers"},{"var":"item.x) OR 1=1 --"}]}`,
		},
		{
			name:  "filter rewritten current dotted payload",
			logic: `{"filter":[{"var":"bag.numbers"},{">=":[{"var":"current.x) OR 1=1 --"},0]}]}`,
		},
		{
			name:  "map direct elem dotted payload",
			logic: `{"map":[{"var":"bag.numbers"},{"var":"elem.x) OR 1=1 --"}]}`,
		},
		{
			name:  "map source internal elem dotted payload",
			logic: `{"map":[{"var":"elem.x) OR 1=1 --"},{"var":"item"}]}`,
		},
		{
			name:  "reduce initial internal elem dotted payload",
			logic: `{"reduce":[{"var":"bag.numbers"},{"+":[{"var":"accumulator"},{"var":"current"}]},{"var":"elem.x) OR 1=1 --"}]}`,
		},
		{
			name:  "map source internal elem dotted payload with default array-form var",
			logic: `{"map":[{"var":["elem.x) OR 1=1 --",[]]},{"var":"item"}]}`,
		},
	}

	modes := []struct {
		name   string
		schema *Schema
	}{
		{name: "schema-less", schema: nil},
		{name: "schema-aware", schema: NewSchema([]FieldSchema{{Name: "bag.numbers", Type: FieldTypeArray}})},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			t.Parallel()
			for _, d := range dialects {
				t.Run(d.String(), func(t *testing.T) {
					t.Parallel()
					tr, err := NewTranspilerWithConfig(&TranspilerConfig{Dialect: d, Schema: mode.schema})
					if err != nil {
						t.Fatalf("NewTranspilerWithConfig() error: %v", err)
					}

					for _, tc := range cases {
						t.Run(tc.name, func(t *testing.T) {
							m := decodeLogicMapForArrayScopeIDTest(t, tc.logic)
							logicAny := decodeLogicAnyForArrayScopeIDTest(t, tc.logic)

							sql, err := tr.Transpile(tc.logic)
							assertInvalidIdentifierError(t, err)
							if strings.Contains(sql, "OR 1=1") {
								t.Fatalf("unexpected injectable SQL emitted: %s", sql)
							}

							psql, _, err := tr.TranspileParameterized(tc.logic)
							assertInvalidIdentifierError(t, err)
							if strings.Contains(psql, "OR 1=1") {
								t.Fatalf("unexpected injectable SQL emitted in parameterized mode: %s", psql)
							}

							_, err = tr.TranspileFromMap(m)
							assertInvalidIdentifierError(t, err)

							_, err = tr.TranspileFromInterface(logicAny)
							assertInvalidIdentifierError(t, err)

							_, _, err = tr.TranspileParameterizedFromMap(m)
							assertInvalidIdentifierError(t, err)

							_, _, err = tr.TranspileParameterizedFromInterface(logicAny)
							assertInvalidIdentifierError(t, err)
						})
					}
				})
			}
		})
	}
}

func TestArrayScopeIdentifierValidationAllowsSafeDottedPaths(t *testing.T) {
	t.Parallel()

	tr, err := NewTranspiler(DialectBigQuery)
	if err != nil {
		t.Fatalf("NewTranspiler() error: %v", err)
	}
	logic := `{"map":[{"var":"bag.numbers"},{"var":"item.safe_field"}]}`

	sql, err := tr.Transpile(logic)
	if err != nil {
		t.Fatalf("unexpected inline error: %v", err)
	}
	if !strings.Contains(sql, "elem.safe_field") {
		t.Fatalf("expected rewritten safe element path, got: %s", sql)
	}

	psql, _, err := tr.TranspileParameterized(logic)
	if err != nil {
		t.Fatalf("unexpected parameterized error: %v", err)
	}
	if !strings.Contains(psql, "elem.safe_field") {
		t.Fatalf("expected rewritten safe element path in parameterized SQL, got: %s", psql)
	}
}
