package jsonlogic2sql

import "testing"

// BenchmarkSimpleComparison benchmarks a simple equality comparison.
func BenchmarkSimpleComparison(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := `{"==": [{"var": "status"}, "active"]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.Transpile(input)
	}
}

// BenchmarkChainedComparison benchmarks a between-style chained comparison.
func BenchmarkChainedComparison(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := `{"<=": [18, {"var": "age"}, 65]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.Transpile(input)
	}
}

// BenchmarkNestedLogical benchmarks nested AND/OR with multiple conditions.
func BenchmarkNestedLogical(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := `{"and": [{">=": [{"var": "age"}, 18]}, {"or": [{"==": [{"var": "role"}, "admin"]}, {"==": [{"var": "role"}, "moderator"]}]}, {"!=": [{"var": "status"}, "banned"]}]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.Transpile(input)
	}
}

// BenchmarkArithmeticExpression benchmarks nested arithmetic operations.
func BenchmarkArithmeticExpression(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := `{">": [{"+": [{"var": "price"}, {"*": [{"var": "tax_rate"}, {"var": "price"}]}]}, 100]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.Transpile(input)
	}
}

// BenchmarkArrayAll benchmarks the all array operator.
func BenchmarkArrayAll(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := `{"all": [{"var": "scores"}, {">=": [{"var": "item"}, 70]}]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.Transpile(input)
	}
}

// BenchmarkArrayReduce benchmarks the reduce operator with SUM pattern.
func BenchmarkArrayReduce(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := `{"reduce": [{"var": "amounts"}, {"+": [{"var": "accumulator"}, {"var": "current"}]}, 0]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.Transpile(input)
	}
}

// BenchmarkStringConcat benchmarks string concatenation.
func BenchmarkStringConcat(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := `{"cat": [{"var": "first_name"}, " ", {"var": "last_name"}]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.Transpile(input)
	}
}

// BenchmarkIfCondition benchmarks the if/ternary operator.
func BenchmarkIfCondition(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := `{"if": [{">": [{"var": "score"}, 90]}, "A", {">": [{"var": "score"}, 80]}, "B", {">": [{"var": "score"}, 70]}, "C", "F"]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.Transpile(input)
	}
}

// BenchmarkDeeplyNested benchmarks a deeply nested expression combining multiple operator types.
func BenchmarkDeeplyNested(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := `{"and": [{"some": [{"filter": [{"var": "data"}, {">": [{"var": "value"}, 0]}]}, {">": [{"var": "elem.score"}, 50]}]}, {">": [{"reduce": [{"var": "totals"}, {"+": [{"var": "accumulator"}, {"var": "current"}]}, 0]}, 1000]}]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.Transpile(input)
	}
}

// BenchmarkWithSchema benchmarks transpilation with schema validation enabled.
func BenchmarkWithSchema(b *testing.B) {
	schema := NewSchema([]FieldSchema{
		{Name: "age", Type: FieldTypeInteger},
		{Name: "name", Type: FieldTypeString},
		{Name: "status", Type: FieldTypeEnum, AllowedValues: []string{"active", "inactive", "banned"}},
		{Name: "scores", Type: FieldTypeArray},
	})
	tr, _ := NewTranspilerWithConfig(&TranspilerConfig{
		Dialect: DialectBigQuery,
		Schema:  schema,
	})
	input := `{"and": [{">=": [{"var": "age"}, 18]}, {"==": [{"var": "status"}, "active"]}, {"some": [{"var": "scores"}, {">": [{"var": "item"}, 70]}]}]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.Transpile(input)
	}
}

// BenchmarkDialects benchmarks the same expression across all dialects.
func BenchmarkDialects(b *testing.B) {
	input := `{"and": [{">=": [{"var": "age"}, 18]}, {"in": [{"var": "status"}, ["active", "pending"]]}, {"some": [{"var": "tags"}, {"==": [{"var": "item"}, "vip"]}]}]}`

	dialects := []struct {
		name    string
		dialect Dialect
	}{
		{"BigQuery", DialectBigQuery},
		{"Spanner", DialectSpanner},
		{"PostgreSQL", DialectPostgreSQL},
		{"DuckDB", DialectDuckDB},
		{"ClickHouse", DialectClickHouse},
	}

	for _, d := range dialects {
		b.Run(d.name, func(b *testing.B) {
			tr, _ := NewTranspiler(d.dialect)
			b.ResetTimer()
			for b.Loop() {
				_, _ = tr.Transpile(input)
			}
		})
	}
}

// BenchmarkInOperator benchmarks the in operator with a large array.
func BenchmarkInOperator(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := `{"in": [{"var": "code"}, ["A001", "A002", "A003", "A004", "A005", "A006", "A007", "A008", "A009", "A010", "A011", "A012", "A013", "A014", "A015", "A016", "A017", "A018", "A019", "A020"]]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.Transpile(input)
	}
}

// BenchmarkTranspileCondition benchmarks TranspileCondition (without WHERE prefix).
func BenchmarkTranspileCondition(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := `{"and": [{">=": [{"var": "age"}, 18]}, {"==": [{"var": "active"}, true]}]}`
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.TranspileCondition(input)
	}
}

// BenchmarkTranspileFromMap benchmarks TranspileFromMap with pre-parsed input.
func BenchmarkTranspileFromMap(b *testing.B) {
	tr, _ := NewTranspiler(DialectBigQuery)
	input := map[string]interface{}{
		"and": []interface{}{
			map[string]interface{}{">=": []interface{}{map[string]interface{}{"var": "age"}, 18}},
			map[string]interface{}{"==": []interface{}{map[string]interface{}{"var": "status"}, "active"}},
		},
	}
	b.ResetTimer()
	for b.Loop() {
		_, _ = tr.TranspileFromMap(input)
	}
}
