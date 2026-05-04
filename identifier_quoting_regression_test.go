package jsonlogic2sql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestIdentifierQuotingRegression_NormalAndDeep_AllDialects(t *testing.T) {
	t.Parallel()

	schema := mustNewSchema([]FieldSchema{
		{Name: "profile.status", Type: FieldTypeString},
		{Name: "metrics.24h.count", Type: FieldTypeInteger},
		{Name: "fixture.windowed_metrics.24h.events.total", Type: FieldTypeInteger},
		{Name: "fixture.windowed_metrics.7d.events.count", Type: FieldTypeInteger},
	})

	modes := []struct {
		name   string
		schema *Schema
	}{
		{name: "schema-less", schema: nil},
		{name: "schema-aware", schema: schema},
	}

	dialects := []Dialect{
		DialectBigQuery,
		DialectSpanner,
		DialectPostgreSQL,
		DialectDuckDB,
		DialectClickHouse,
	}

	cases := []struct {
		name          string
		logic         string
		inlineSQL     map[Dialect]string
		paramSQL      map[Dialect]string
		expectedParam []QueryParam
	}{
		{
			name:  "normal nested identifier remains unquoted",
			logic: `{"==": [{"var": "profile.status"}, "active"]}`,
			inlineSQL: sameSQLAllDialects(
				"WHERE profile.status = 'active'",
			),
			paramSQL: map[Dialect]string{
				DialectBigQuery:   "WHERE profile.status = @p1",
				DialectSpanner:    "WHERE profile.status = @p1",
				DialectPostgreSQL: "WHERE profile.status = $1",
				DialectDuckDB:     "WHERE profile.status = $1",
				DialectClickHouse: "WHERE profile.status = @p1",
			},
			expectedParam: []QueryParam{{Name: "p1", Value: "active"}},
		},
		{
			name:  "shallow numeric-leading segment is quoted",
			logic: `{">=": [{"var": "metrics.24h.count"}, 50000]}`,
			inlineSQL: map[Dialect]string{
				DialectBigQuery:   "WHERE metrics.`24h`.count >= 50000",
				DialectSpanner:    "WHERE metrics.`24h`.count >= 50000",
				DialectPostgreSQL: `WHERE metrics."24h".count >= 50000`,
				DialectDuckDB:     `WHERE metrics."24h".count >= 50000`,
				DialectClickHouse: "WHERE metrics.`24h`.count >= 50000",
			},
			paramSQL: map[Dialect]string{
				DialectBigQuery:   "WHERE metrics.`24h`.count >= @p1",
				DialectSpanner:    "WHERE metrics.`24h`.count >= @p1",
				DialectPostgreSQL: `WHERE metrics."24h".count >= $1`,
				DialectDuckDB:     `WHERE metrics."24h".count >= $1`,
				DialectClickHouse: "WHERE metrics.`24h`.count >= @p1",
			},
			expectedParam: []QueryParam{{Name: "p1", Value: float64(50000)}},
		},
		{
			name:  "deeply nested numeric-leading segments are quoted independently",
			logic: `{"and":[{">=":[{"var":"fixture.windowed_metrics.24h.events.total"},50000]},{"<":[{"var":"fixture.windowed_metrics.7d.events.count"},100]}]}`,
			inlineSQL: map[Dialect]string{
				DialectBigQuery:   "WHERE (fixture.windowed_metrics.`24h`.events.total >= 50000 AND fixture.windowed_metrics.`7d`.events.count < 100)",
				DialectSpanner:    "WHERE (fixture.windowed_metrics.`24h`.events.total >= 50000 AND fixture.windowed_metrics.`7d`.events.count < 100)",
				DialectPostgreSQL: `WHERE (fixture.windowed_metrics."24h".events.total >= 50000 AND fixture.windowed_metrics."7d".events.count < 100)`,
				DialectDuckDB:     `WHERE (fixture.windowed_metrics."24h".events.total >= 50000 AND fixture.windowed_metrics."7d".events.count < 100)`,
				DialectClickHouse: "WHERE (fixture.windowed_metrics.`24h`.events.total >= 50000 AND fixture.windowed_metrics.`7d`.events.count < 100)",
			},
			paramSQL: map[Dialect]string{
				DialectBigQuery:   "WHERE (fixture.windowed_metrics.`24h`.events.total >= @p1 AND fixture.windowed_metrics.`7d`.events.count < @p2)",
				DialectSpanner:    "WHERE (fixture.windowed_metrics.`24h`.events.total >= @p1 AND fixture.windowed_metrics.`7d`.events.count < @p2)",
				DialectPostgreSQL: `WHERE (fixture.windowed_metrics."24h".events.total >= $1 AND fixture.windowed_metrics."7d".events.count < $2)`,
				DialectDuckDB:     `WHERE (fixture.windowed_metrics."24h".events.total >= $1 AND fixture.windowed_metrics."7d".events.count < $2)`,
				DialectClickHouse: "WHERE (fixture.windowed_metrics.`24h`.events.total >= @p1 AND fixture.windowed_metrics.`7d`.events.count < @p2)",
			},
			expectedParam: []QueryParam{
				{Name: "p1", Value: float64(50000)},
				{Name: "p2", Value: float64(100)},
			},
		},
		{
			name:  "deeply nested numeric-leading segments inside custom operators",
			logic: `{"and":[{"betweenInclusive":[{"var":"fixture.windowed_metrics.24h.events.total"},50000,100000]},{"isNonZero":[{"var":"fixture.windowed_metrics.7d.events.count"}]}]}`,
			inlineSQL: map[Dialect]string{
				DialectBigQuery:   "WHERE ((fixture.windowed_metrics.`24h`.events.total BETWEEN 50000 AND 100000) AND (fixture.windowed_metrics.`7d`.events.count != 0))",
				DialectSpanner:    "WHERE ((fixture.windowed_metrics.`24h`.events.total BETWEEN 50000 AND 100000) AND (fixture.windowed_metrics.`7d`.events.count != 0))",
				DialectPostgreSQL: `WHERE ((fixture.windowed_metrics."24h".events.total BETWEEN 50000 AND 100000) AND (fixture.windowed_metrics."7d".events.count != 0))`,
				DialectDuckDB:     `WHERE ((fixture.windowed_metrics."24h".events.total BETWEEN 50000 AND 100000) AND (fixture.windowed_metrics."7d".events.count != 0))`,
				DialectClickHouse: "WHERE ((fixture.windowed_metrics.`24h`.events.total BETWEEN 50000 AND 100000) AND (fixture.windowed_metrics.`7d`.events.count != 0))",
			},
			paramSQL: map[Dialect]string{
				DialectBigQuery:   "WHERE ((fixture.windowed_metrics.`24h`.events.total BETWEEN @p1 AND @p2) AND (fixture.windowed_metrics.`7d`.events.count != 0))",
				DialectSpanner:    "WHERE ((fixture.windowed_metrics.`24h`.events.total BETWEEN @p1 AND @p2) AND (fixture.windowed_metrics.`7d`.events.count != 0))",
				DialectPostgreSQL: `WHERE ((fixture.windowed_metrics."24h".events.total BETWEEN $1 AND $2) AND (fixture.windowed_metrics."7d".events.count != 0))`,
				DialectDuckDB:     `WHERE ((fixture.windowed_metrics."24h".events.total BETWEEN $1 AND $2) AND (fixture.windowed_metrics."7d".events.count != 0))`,
				DialectClickHouse: "WHERE ((fixture.windowed_metrics.`24h`.events.total BETWEEN @p1 AND @p2) AND (fixture.windowed_metrics.`7d`.events.count != 0))",
			},
			expectedParam: []QueryParam{
				{Name: "p1", Value: float64(50000)},
				{Name: "p2", Value: float64(100000)},
			},
		},
		{
			name:  "deeply nested numeric-leading segment inside dialect-aware custom operator",
			logic: `{"dialectMetricPresent":[{"var":"fixture.windowed_metrics.24h.events.total"}]}`,
			inlineSQL: map[Dialect]string{
				DialectBigQuery:   "WHERE IFNULL(fixture.windowed_metrics.`24h`.events.total, 0) > 0",
				DialectSpanner:    "WHERE IFNULL(fixture.windowed_metrics.`24h`.events.total, 0) > 0",
				DialectPostgreSQL: `WHERE COALESCE(fixture.windowed_metrics."24h".events.total, 0) > 0`,
				DialectDuckDB:     `WHERE COALESCE(fixture.windowed_metrics."24h".events.total, 0) > 0`,
				DialectClickHouse: "WHERE ifNull(fixture.windowed_metrics.`24h`.events.total, 0) > 0",
			},
			paramSQL: map[Dialect]string{
				DialectBigQuery:   "WHERE IFNULL(fixture.windowed_metrics.`24h`.events.total, 0) > 0",
				DialectSpanner:    "WHERE IFNULL(fixture.windowed_metrics.`24h`.events.total, 0) > 0",
				DialectPostgreSQL: `WHERE COALESCE(fixture.windowed_metrics."24h".events.total, 0) > 0`,
				DialectDuckDB:     `WHERE COALESCE(fixture.windowed_metrics."24h".events.total, 0) > 0`,
				DialectClickHouse: "WHERE ifNull(fixture.windowed_metrics.`24h`.events.total, 0) > 0",
			},
			expectedParam: nil,
		},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			t.Parallel()

			for _, d := range dialects {
				t.Run(d.String(), func(t *testing.T) {
					t.Parallel()

					tr, err := NewTranspilerWithConfig(&TranspilerConfig{
						Dialect: d,
						Schema:  mode.schema,
					})
					if err != nil {
						t.Fatalf("NewTranspilerWithConfig() error: %v", err)
					}
					registerIdentifierQuotingCustomOperators(t, tr)

					for _, tc := range cases {
						t.Run(tc.name, func(t *testing.T) {
							assertIdentifierQuotingSQL(t, tr, d, tc.logic, tc.inlineSQL[d], tc.paramSQL[d], tc.expectedParam)
						})
					}
				})
			}
		})
	}
}

func registerIdentifierQuotingCustomOperators(t *testing.T, tr *Transpiler) {
	t.Helper()

	if err := tr.RegisterOperatorFunc("betweenInclusive", func(_ string, args []interface{}) (string, error) {
		if len(args) != 3 {
			return "", fmt.Errorf("betweenInclusive expects 3 args")
		}
		return fmt.Sprintf("(%v BETWEEN %v AND %v)", args[0], args[1], args[2]), nil
	}); err != nil {
		t.Fatalf("RegisterOperatorFunc(betweenInclusive) error: %v", err)
	}

	if err := tr.RegisterOperatorFunc("isNonZero", func(_ string, args []interface{}) (string, error) {
		if len(args) != 1 {
			return "", fmt.Errorf("isNonZero expects 1 arg")
		}
		return fmt.Sprintf("(%v != 0)", args[0]), nil
	}); err != nil {
		t.Fatalf("RegisterOperatorFunc(isNonZero) error: %v", err)
	}

	if err := tr.RegisterDialectAwareOperatorFunc("dialectMetricPresent", func(_ string, args []interface{}, d Dialect) (string, error) {
		if len(args) != 1 {
			return "", fmt.Errorf("dialectMetricPresent expects 1 arg")
		}
		switch d {
		case DialectPostgreSQL, DialectDuckDB:
			return fmt.Sprintf("COALESCE(%v, 0) > 0", args[0]), nil
		case DialectClickHouse:
			return fmt.Sprintf("ifNull(%v, 0) > 0", args[0]), nil
		default:
			return fmt.Sprintf("IFNULL(%v, 0) > 0", args[0]), nil
		}
	}); err != nil {
		t.Fatalf("RegisterDialectAwareOperatorFunc(dialectMetricPresent) error: %v", err)
	}
}

func TestIdentifierQuotingRegression_UnsafeNoSchemaIdentifiersRejected_AllDialects(t *testing.T) {
	t.Parallel()

	logic := `{"==": [{"var": "metrics.24h;DROP.count"}, 1]}`
	dialects := []Dialect{
		DialectBigQuery,
		DialectSpanner,
		DialectPostgreSQL,
		DialectDuckDB,
		DialectClickHouse,
	}

	for _, d := range dialects {
		t.Run(d.String(), func(t *testing.T) {
			t.Parallel()

			tr, err := NewTranspiler(d)
			if err != nil {
				t.Fatalf("NewTranspiler() error: %v", err)
			}

			if _, err := tr.Transpile(logic); err == nil {
				t.Fatal("Transpile() expected invalid identifier error, got nil")
			}
			if _, err := tr.TranspileCondition(logic); err == nil {
				t.Fatal("TranspileCondition() expected invalid identifier error, got nil")
			}
			if _, _, err := tr.TranspileParameterized(logic); err == nil {
				t.Fatal("TranspileParameterized() expected invalid identifier error, got nil")
			}
			if _, _, err := tr.TranspileConditionParameterized(logic); err == nil {
				t.Fatal("TranspileConditionParameterized() expected invalid identifier error, got nil")
			}
		})
	}
}

func assertIdentifierQuotingSQL(
	t *testing.T,
	tr *Transpiler,
	d Dialect,
	logic string,
	expectedInline string,
	expectedParamSQL string,
	expectedParams []QueryParam,
) {
	t.Helper()

	sql, err := tr.Transpile(logic)
	if err != nil {
		t.Fatalf("Transpile() error: %v", err)
	}
	if sql != expectedInline {
		t.Fatalf("Transpile() for %s = %q, want %q", d, sql, expectedInline)
	}

	cond, err := tr.TranspileCondition(logic)
	if err != nil {
		t.Fatalf("TranspileCondition() error: %v", err)
	}
	if cond != strings.TrimPrefix(expectedInline, "WHERE ") {
		t.Fatalf("TranspileCondition() for %s = %q, want %q", d, cond, strings.TrimPrefix(expectedInline, "WHERE "))
	}

	paramSQL, gotParams, err := tr.TranspileParameterized(logic)
	if err != nil {
		t.Fatalf("TranspileParameterized() error: %v", err)
	}
	if paramSQL != expectedParamSQL {
		t.Fatalf("TranspileParameterized() for %s = %q, want %q", d, paramSQL, expectedParamSQL)
	}
	if !reflect.DeepEqual(gotParams, expectedParams) {
		t.Fatalf("TranspileParameterized() params for %s = %#v, want %#v", d, gotParams, expectedParams)
	}

	paramCond, condParams, err := tr.TranspileConditionParameterized(logic)
	if err != nil {
		t.Fatalf("TranspileConditionParameterized() error: %v", err)
	}
	if paramCond != strings.TrimPrefix(expectedParamSQL, "WHERE ") {
		t.Fatalf("TranspileConditionParameterized() for %s = %q, want %q", d, paramCond, strings.TrimPrefix(expectedParamSQL, "WHERE "))
	}
	if !reflect.DeepEqual(condParams, expectedParams) {
		t.Fatalf("TranspileConditionParameterized() params for %s = %#v, want %#v", d, condParams, expectedParams)
	}

	var logicMap map[string]interface{}
	if unmarshalErr := json.Unmarshal([]byte(logic), &logicMap); unmarshalErr != nil {
		t.Fatalf("json.Unmarshal() error: %v", unmarshalErr)
	}

	fromMapSQL, fromMapParams, err := tr.TranspileParameterizedFromMap(logicMap)
	if err != nil {
		t.Fatalf("TranspileParameterizedFromMap() error: %v", err)
	}
	if fromMapSQL != expectedParamSQL {
		t.Fatalf("TranspileParameterizedFromMap() for %s = %q, want %q", d, fromMapSQL, expectedParamSQL)
	}
	if !reflect.DeepEqual(fromMapParams, expectedParams) {
		t.Fatalf("TranspileParameterizedFromMap() params for %s = %#v, want %#v", d, fromMapParams, expectedParams)
	}
}

func sameSQLAllDialects(sql string) map[Dialect]string {
	return map[Dialect]string{
		DialectBigQuery:   sql,
		DialectSpanner:    sql,
		DialectPostgreSQL: sql,
		DialectDuckDB:     sql,
		DialectClickHouse: sql,
	}
}
