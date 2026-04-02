package jsonlogic2sql

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

func decodeLogicMapLocal(t *testing.T, logic string) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(logic), &m); err != nil {
		t.Fatalf("json.Unmarshal(map) failed: %v", err)
	}
	return m
}

func decodeLogicAnyLocal(t *testing.T, logic string) interface{} {
	t.Helper()
	var v interface{}
	if err := json.Unmarshal([]byte(logic), &v); err != nil {
		t.Fatalf("json.Unmarshal(interface) failed: %v", err)
	}
	return v
}

func assertAccumulatorSchemaError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "accumulator") {
		t.Fatalf("expected error to mention accumulator, got: %v", err)
	}
	if !strings.Contains(msg, "schema") {
		t.Fatalf("expected schema validation error, got: %v", err)
	}
}

func assertAllAPIsErrorForAccumulatorOutsideReduce(t *testing.T, tr *Transpiler, logic string) {
	t.Helper()

	m := decodeLogicMapLocal(t, logic)
	logicAny := decodeLogicAnyLocal(t, logic)

	_, err := tr.Transpile(logic)
	assertAccumulatorSchemaError(t, err)

	_, err = tr.TranspileCondition(logic)
	assertAccumulatorSchemaError(t, err)

	_, _, err = tr.TranspileParameterized(logic)
	assertAccumulatorSchemaError(t, err)

	_, _, err = tr.TranspileConditionParameterized(logic)
	assertAccumulatorSchemaError(t, err)

	_, err = tr.TranspileFromMap(m)
	assertAccumulatorSchemaError(t, err)

	_, err = tr.TranspileFromInterface(logicAny)
	assertAccumulatorSchemaError(t, err)

	_, err = tr.TranspileConditionFromMap(m)
	assertAccumulatorSchemaError(t, err)

	_, err = tr.TranspileConditionFromInterface(logicAny)
	assertAccumulatorSchemaError(t, err)

	_, _, err = tr.TranspileParameterizedFromMap(m)
	assertAccumulatorSchemaError(t, err)

	_, _, err = tr.TranspileParameterizedFromInterface(logicAny)
	assertAccumulatorSchemaError(t, err)

	_, _, err = tr.TranspileConditionParameterizedFromMap(m)
	assertAccumulatorSchemaError(t, err)

	_, _, err = tr.TranspileConditionParameterizedFromInterface(logicAny)
	assertAccumulatorSchemaError(t, err)
}

func TestAccumulatorOutsideReduceRejectedWithSchema_AllDialects(t *testing.T) {
	t.Parallel()

	schema := NewSchema([]FieldSchema{
		{Name: "bag.numbers", Type: FieldTypeArray},
	})

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
			name:  "map expression",
			logic: `{"map":[{"var":"bag.numbers"},{"+":[{"var":"accumulator"},1]}]}`,
		},
		{
			name:  "filter predicate",
			logic: `{"filter":[{"var":"bag.numbers"},{">":[{"var":"accumulator"},0]}]}`,
		},
		{
			name:  "all predicate",
			logic: `{"all":[{"var":"bag.numbers"},{">=":[{"var":"accumulator"},0]}]}`,
		},
		{
			name:  "some predicate",
			logic: `{"some":[{"var":"bag.numbers"},{"==":[{"var":"accumulator"},1]}]}`,
		},
		{
			name:  "none predicate",
			logic: `{"none":[{"var":"bag.numbers"},{"==":[{"var":"accumulator"},0]}]}`,
		},
	}

	for _, d := range dialects {
		t.Run(d.String(), func(t *testing.T) {
			t.Parallel()

			tr, err := NewTranspilerWithConfig(&TranspilerConfig{
				Dialect: d,
				Schema:  schema,
			})
			if err != nil {
				t.Fatalf("NewTranspilerWithConfig() error: %v", err)
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					assertAllAPIsErrorForAccumulatorOutsideReduce(t, tr, tc.logic)
				})
			}
		})
	}
}

func TestReduceAccumulatorStillWorks_AllDialects(t *testing.T) {
	t.Parallel()

	dialects := []Dialect{
		DialectBigQuery,
		DialectSpanner,
		DialectPostgreSQL,
		DialectDuckDB,
		DialectClickHouse,
	}
	logic := `{"reduce":[{"var":"bag.numbers"},{"+":[{"var":"accumulator"},{"var":"current"}]},0]}`

	accWord := regexp.MustCompile(`\baccumulator\b`)

	for _, d := range dialects {
		t.Run(d.String(), func(t *testing.T) {
			t.Parallel()

			tr, err := NewTranspiler(d)
			if err != nil {
				t.Fatalf("NewTranspiler() error: %v", err)
			}

			sql, err := tr.Transpile(logic)
			if err != nil {
				t.Fatalf("Transpile() error: %v", err)
			}
			if accWord.MatchString(sql) {
				t.Fatalf("unexpected bare accumulator in inline SQL: %s", sql)
			}

			psql, params, err := tr.TranspileParameterized(logic)
			if err != nil {
				t.Fatalf("TranspileParameterized() error: %v", err)
			}
			if accWord.MatchString(psql) {
				t.Fatalf("unexpected bare accumulator in parameterized SQL: %s", psql)
			}
			if len(params) == 0 {
				t.Fatalf("expected parameterized reduce to emit params, got none")
			}
		})
	}
}
