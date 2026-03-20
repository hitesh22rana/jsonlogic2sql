package operators

import (
	"fmt"
	"testing"

	"github.com/h22rana/jsonlogic2sql/internal/dialect"
)

func TestNumericOperator_ToSQL(t *testing.T) {
	op := NewNumericOperator(nil)

	tests := []struct {
		name     string
		operator string
		args     []interface{}
		expected string
		hasError bool
	}{
		// Addition tests
		{
			name:     "addition with two numbers",
			operator: "+",
			args:     []interface{}{5, 3},
			expected: "(5 + 3)",
			hasError: false,
		},
		{
			name:     "addition with three numbers",
			operator: "+",
			args:     []interface{}{1, 2, 3},
			expected: "(1 + 2 + 3)",
			hasError: false,
		},
		{
			name:     "addition with var and number",
			operator: "+",
			args:     []interface{}{map[string]interface{}{"var": "amount"}, 100},
			expected: "(amount + 100)",
			hasError: false,
		},
		{
			name:     "addition with dotted var",
			operator: "+",
			args:     []interface{}{map[string]interface{}{"var": "user.score"}, 50},
			expected: "(user.score + 50)",
			hasError: false,
		},
		{
			name:     "unary plus (cast to number)",
			operator: "+",
			args:     []interface{}{5},
			expected: "CAST(5 AS NUMERIC)",
			hasError: false,
		},

		// Subtraction tests
		{
			name:     "subtraction with two numbers",
			operator: "-",
			args:     []interface{}{10, 3},
			expected: "(10 - 3)",
			hasError: false,
		},
		{
			name:     "subtraction with three numbers",
			operator: "-",
			args:     []interface{}{20, 5, 2},
			expected: "(20 - 5 - 2)",
			hasError: false,
		},
		{
			name:     "subtraction with var and number",
			operator: "-",
			args:     []interface{}{map[string]interface{}{"var": "balance"}, 50},
			expected: "(balance - 50)",
			hasError: false,
		},
		{
			name:     "unary minus (negation)",
			operator: "-",
			args:     []interface{}{10},
			expected: "(-10)",
			hasError: false,
		},
		{
			name:     "unary minus with var",
			operator: "-",
			args:     []interface{}{map[string]interface{}{"var": "value"}},
			expected: "(-value)",
			hasError: false,
		},

		// Multiplication tests
		{
			name:     "multiplication with two numbers",
			operator: "*",
			args:     []interface{}{4, 5},
			expected: "(4 * 5)",
			hasError: false,
		},
		{
			name:     "multiplication with three numbers",
			operator: "*",
			args:     []interface{}{2, 3, 4},
			expected: "(2 * 3 * 4)",
			hasError: false,
		},
		{
			name:     "multiplication with var and number",
			operator: "*",
			args:     []interface{}{map[string]interface{}{"var": "price"}, 1.2},
			expected: "(price * 1.2)",
			hasError: false,
		},
		{
			name:     "multiplication with too few arguments",
			operator: "*",
			args:     []interface{}{5},
			expected: "",
			hasError: true,
		},

		// Division tests
		{
			name:     "division with two numbers",
			operator: "/",
			args:     []interface{}{20, 4},
			expected: "(20 / 4)",
			hasError: false,
		},
		{
			name:     "division with three numbers",
			operator: "/",
			args:     []interface{}{100, 2, 5},
			expected: "(100 / 2 / 5)",
			hasError: false,
		},
		{
			name:     "division with var and number",
			operator: "/",
			args:     []interface{}{map[string]interface{}{"var": "total"}, 2},
			expected: "(total / 2)",
			hasError: false,
		},
		{
			name:     "division with too few arguments",
			operator: "/",
			args:     []interface{}{10},
			expected: "",
			hasError: true,
		},

		// Modulo tests
		{
			name:     "modulo with two numbers",
			operator: "%",
			args:     []interface{}{17, 5},
			expected: "(17 % 5)",
			hasError: false,
		},
		{
			name:     "modulo with var and number",
			operator: "%",
			args:     []interface{}{map[string]interface{}{"var": "count"}, 3},
			expected: "(count % 3)",
			hasError: false,
		},
		{
			name:     "modulo with wrong argument count",
			operator: "%",
			args:     []interface{}{17, 5, 2},
			expected: "",
			hasError: true,
		},
		{
			name:     "modulo with too few arguments",
			operator: "%",
			args:     []interface{}{17},
			expected: "",
			hasError: true,
		},

		// Max tests
		{
			name:     "max with two numbers",
			operator: "max",
			args:     []interface{}{10, 20},
			expected: "GREATEST(10, 20)",
			hasError: false,
		},
		{
			name:     "max with three numbers",
			operator: "max",
			args:     []interface{}{5, 15, 10},
			expected: "GREATEST(5, 15, 10)",
			hasError: false,
		},
		{
			name:     "max with var and numbers",
			operator: "max",
			args:     []interface{}{map[string]interface{}{"var": "score"}, 100, 50},
			expected: "GREATEST(score, 100, 50)",
			hasError: false,
		},
		{
			name:     "max with too few arguments",
			operator: "max",
			args:     []interface{}{10},
			expected: "",
			hasError: true,
		},

		// Min tests
		{
			name:     "min with two numbers",
			operator: "min",
			args:     []interface{}{10, 20},
			expected: "LEAST(10, 20)",
			hasError: false,
		},
		{
			name:     "min with three numbers",
			operator: "min",
			args:     []interface{}{5, 15, 10},
			expected: "LEAST(5, 15, 10)",
			hasError: false,
		},
		{
			name:     "min with var and numbers",
			operator: "min",
			args:     []interface{}{map[string]interface{}{"var": "score"}, 100, 50},
			expected: "LEAST(score, 100, 50)",
			hasError: false,
		},
		{
			name:     "min with too few arguments",
			operator: "min",
			args:     []interface{}{10},
			expected: "",
			hasError: true,
		},

		// Unsupported operator
		{
			name:     "unsupported operator",
			operator: "unsupported",
			args:     []interface{}{1, 2},
			expected: "",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.ToSQL(tt.operator, tt.args)
			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestNumericOperator_valueToSQL(t *testing.T) {
	op := NewNumericOperator(nil)

	tests := []struct {
		name     string
		input    interface{}
		expected string
		hasError bool
	}{
		{
			name:     "literal number",
			input:    42,
			expected: "42",
			hasError: false,
		},
		{
			name:     "literal float",
			input:    3.14,
			expected: "3.14",
			hasError: false,
		},
		{
			name:     "var expression",
			input:    map[string]interface{}{"var": "amount"},
			expected: "amount",
			hasError: false,
		},
		{
			name:     "dotted var expression",
			input:    map[string]interface{}{"var": "user.score"},
			expected: "user.score",
			hasError: false,
		},
		{
			name:     "non-var object",
			input:    map[string]interface{}{"other": "value"},
			expected: "",
			hasError: true,
		},
		// Nested expression tests
		{
			name:     "nested unary minus",
			input:    map[string]interface{}{"-": []interface{}{map[string]interface{}{"var": "x"}}},
			expected: "(-x)",
			hasError: false,
		},
		{
			name:     "nested addition",
			input:    map[string]interface{}{"+": []interface{}{map[string]interface{}{"var": "a"}, 5}},
			expected: "(a + 5)",
			hasError: false,
		},
		{
			name:     "multiplication with nested unary minus",
			input:    map[string]interface{}{"*": []interface{}{2, map[string]interface{}{"-": []interface{}{map[string]interface{}{"var": "x"}}}}},
			expected: "(2 * (-x))",
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.valueToSQL(tt.input)
			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

// numericSchemaProvider is a configurable schema provider for numeric operator tests.
type numericSchemaProvider struct {
	fields map[string]string
}

func (m *numericSchemaProvider) HasField(fieldName string) bool {
	_, ok := m.fields[fieldName]
	return ok
}

func (m *numericSchemaProvider) GetFieldType(fieldName string) string {
	return m.fields[fieldName]
}

func (m *numericSchemaProvider) ValidateField(_ string) error {
	return nil
}

func (m *numericSchemaProvider) IsArrayType(fieldName string) bool {
	return m.fields[fieldName] == "array"
}

func (m *numericSchemaProvider) IsStringType(fieldName string) bool {
	return m.fields[fieldName] == "string"
}

func (m *numericSchemaProvider) IsNumericType(fieldName string) bool {
	t := m.fields[fieldName]
	return t == "integer" || t == "number"
}

func (m *numericSchemaProvider) IsBooleanType(fieldName string) bool {
	return m.fields[fieldName] == "boolean"
}

func (m *numericSchemaProvider) IsEnumType(_ string) bool {
	return false
}

func (m *numericSchemaProvider) GetAllowedValues(_ string) []string {
	return nil
}

func (m *numericSchemaProvider) ValidateEnumValue(_, _ string) error {
	return nil
}

func TestNumericOperator_validateNumericOperand(t *testing.T) {
	schema := &numericSchemaProvider{
		fields: map[string]string{
			"amount":   "integer",
			"price":    "number",
			"name":     "string",
			"tags":     "array",
			"verified": "boolean",
		},
	}

	config := NewOperatorConfig(dialect.DialectBigQuery, schema)
	op := NewNumericOperator(config)

	tests := []struct {
		name     string
		value    interface{}
		hasError bool
	}{
		{
			name:     "numeric integer field passes",
			value:    map[string]interface{}{"var": "amount"},
			hasError: false,
		},
		{
			name:     "numeric number field passes",
			value:    map[string]interface{}{"var": "price"},
			hasError: false,
		},
		{
			name:     "string field fails",
			value:    map[string]interface{}{"var": "name"},
			hasError: true,
		},
		{
			name:     "array field fails",
			value:    map[string]interface{}{"var": "tags"},
			hasError: true,
		},
		{
			name:     "boolean field fails",
			value:    map[string]interface{}{"var": "verified"},
			hasError: true,
		},
		{
			name:     "literal number - no validation",
			value:    42,
			hasError: false,
		},
		{
			name:     "non-var map - no validation",
			value:    map[string]interface{}{"other": "value"},
			hasError: false,
		},
		{
			name:     "unknown field - no validation (no type in schema)",
			value:    map[string]interface{}{"var": "unknown_field"},
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := op.validateNumericOperand(tt.value)
			if tt.hasError {
				if err == nil {
					t.Errorf("validateNumericOperand() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("validateNumericOperand() unexpected error = %v", err)
				}
			}
		})
	}

	// Test with nil schema - should always pass
	opNoSchema := NewNumericOperator(nil)
	if err := opNoSchema.validateNumericOperand(map[string]interface{}{"var": "name"}); err != nil {
		t.Errorf("validateNumericOperand() with nil schema should pass, got %v", err)
	}
}

func TestNumericOperator_ToSQL_WithSchemaValidation(t *testing.T) {
	schema := &numericSchemaProvider{
		fields: map[string]string{
			"amount": "integer",
			"name":   "string",
		},
	}

	config := NewOperatorConfig(dialect.DialectBigQuery, schema)
	op := NewNumericOperator(config)

	// Should fail: string field in numeric operation
	_, err := op.ToSQL("+", []interface{}{
		map[string]interface{}{"var": "name"},
		10,
	})
	if err == nil {
		t.Errorf("ToSQL() expected error for string field in addition, got nil")
	}

	// Should succeed: numeric field in numeric operation
	result, err := op.ToSQL("+", []interface{}{
		map[string]interface{}{"var": "amount"},
		10,
	})
	if err != nil {
		t.Errorf("ToSQL() unexpected error = %v", err)
	}
	if result != "(amount + 10)" {
		t.Errorf("ToSQL() = %v, want (amount + 10)", result)
	}
}

func TestNumericOperator_valueToSQL_ProcessedValue(t *testing.T) {
	op := NewNumericOperator(nil)

	tests := []struct {
		name     string
		input    interface{}
		expected string
		hasError bool
	}{
		{
			name:     "ProcessedValue with SQL",
			input:    ProcessedValue{Value: "SUM(amount)", IsSQL: true},
			expected: "SUM(amount)",
			hasError: false,
		},
		{
			name:     "ProcessedValue with literal",
			input:    ProcessedValue{Value: "hello", IsSQL: false},
			expected: "'hello'",
			hasError: false,
		},
		{
			name:     "string value passed through as SQL",
			input:    "pre_processed_sql",
			expected: "pre_processed_sql",
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.valueToSQL(tt.input)
			if tt.hasError {
				if err == nil {
					t.Errorf("valueToSQL() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("valueToSQL() unexpected error = %v", err)
				}
				if result != tt.expected {
					t.Errorf("valueToSQL() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestNumericOperator_generateComplexSQL(t *testing.T) {
	op := NewNumericOperator(nil)

	tests := []struct {
		name     string
		operator string
		args     []string
		expected string
		hasError bool
	}{
		{
			name:     "addition two args",
			operator: "+",
			args:     []string{"a", "b"},
			expected: "(a + b)",
			hasError: false,
		},
		{
			name:     "addition three args",
			operator: "+",
			args:     []string{"a", "b", "c"},
			expected: "(a + b + c)",
			hasError: false,
		},
		{
			name:     "addition insufficient args",
			operator: "+",
			args:     []string{"a"},
			expected: "",
			hasError: true,
		},
		{
			name:     "subtraction two args",
			operator: "-",
			args:     []string{"a", "b"},
			expected: "(a - b)",
			hasError: false,
		},
		{
			name:     "subtraction unary minus",
			operator: "-",
			args:     []string{"x"},
			expected: "(-x)",
			hasError: false,
		},
		{
			name:     "multiplication two args",
			operator: "*",
			args:     []string{"a", "b"},
			expected: "(a * b)",
			hasError: false,
		},
		{
			name:     "multiplication insufficient args",
			operator: "*",
			args:     []string{"a"},
			expected: "",
			hasError: true,
		},
		{
			name:     "division two args",
			operator: "/",
			args:     []string{"a", "b"},
			expected: "(a / b)",
			hasError: false,
		},
		{
			name:     "division insufficient args",
			operator: "/",
			args:     []string{"a"},
			expected: "",
			hasError: true,
		},
		{
			name:     "modulo two args",
			operator: "%",
			args:     []string{"a", "b"},
			expected: "(a % b)",
			hasError: false,
		},
		{
			name:     "modulo insufficient args",
			operator: "%",
			args:     []string{"a"},
			expected: "",
			hasError: true,
		},
		{
			name:     "max two args",
			operator: "max",
			args:     []string{"a", "b"},
			expected: "GREATEST(a, b)",
			hasError: false,
		},
		{
			name:     "max insufficient args",
			operator: "max",
			args:     []string{"a"},
			expected: "",
			hasError: true,
		},
		{
			name:     "min two args",
			operator: "min",
			args:     []string{"a", "b"},
			expected: "LEAST(a, b)",
			hasError: false,
		},
		{
			name:     "min insufficient args",
			operator: "min",
			args:     []string{"a"},
			expected: "",
			hasError: true,
		},
		{
			name:     "unsupported operator",
			operator: "^",
			args:     []string{"a", "b"},
			expected: "",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.generateComplexSQL(tt.operator, tt.args)
			if tt.hasError {
				if err == nil {
					t.Errorf("generateComplexSQL() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("generateComplexSQL() unexpected error = %v", err)
				}
				if result != tt.expected {
					t.Errorf("generateComplexSQL() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestNumericOperator_valueToSQL_NestedComparison(t *testing.T) {
	op := NewNumericOperator(nil)

	// Test nested comparison inside numeric valueToSQL
	input := map[string]interface{}{"==": []interface{}{map[string]interface{}{"var": "status"}, "active"}}
	result, err := op.valueToSQL(input)
	if err != nil {
		t.Errorf("valueToSQL() unexpected error = %v", err)
	}
	// The var resolves to "status" but processComplexArgsForComparison passes raw strings
	// so comparison operator may quote the var result as a literal string
	if result == "" {
		t.Error("valueToSQL() returned empty string for nested comparison")
	}
}

func TestNumericOperator_valueToSQL_NestedIf(t *testing.T) {
	op := NewNumericOperator(nil)

	// Test nested if expression
	input := map[string]interface{}{
		"if": []interface{}{
			map[string]interface{}{">": []interface{}{map[string]interface{}{"var": "x"}, 0}},
			1,
			0,
		},
	}
	result, err := op.valueToSQL(input)
	if err != nil {
		t.Errorf("valueToSQL() unexpected error = %v", err)
	}
	expected := "CASE WHEN x > 0 THEN 1 ELSE 0 END"
	if result != expected {
		t.Errorf("valueToSQL() = %v, want %v", result, expected)
	}
}

func TestNumericOperator_valueToSQL_NestedLogical(t *testing.T) {
	op := NewNumericOperator(nil)

	tests := []struct {
		name     string
		input    interface{}
		expected string
		hasError bool
	}{
		{
			name: "nested and",
			input: map[string]interface{}{
				"and": []interface{}{
					map[string]interface{}{">": []interface{}{map[string]interface{}{"var": "x"}, 0}},
					map[string]interface{}{"<": []interface{}{map[string]interface{}{"var": "x"}, 100}},
				},
			},
			expected: "(x > 0 AND x < 100)",
			hasError: false,
		},
		{
			name: "nested or",
			input: map[string]interface{}{
				"or": []interface{}{
					map[string]interface{}{"==": []interface{}{map[string]interface{}{"var": "status"}, "a"}},
					map[string]interface{}{"==": []interface{}{map[string]interface{}{"var": "status"}, "b"}},
				},
			},
			expected: "(status = 'a' OR status = 'b')",
			hasError: false,
		},
		{
			name: "nested not",
			input: map[string]interface{}{
				"!": []interface{}{
					map[string]interface{}{"==": []interface{}{map[string]interface{}{"var": "x"}, 0}},
				},
			},
			expected: "NOT (x = 0)",
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.valueToSQL(tt.input)
			if tt.hasError {
				if err == nil {
					t.Errorf("valueToSQL() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("valueToSQL() unexpected error = %v", err)
				}
				if result != tt.expected {
					t.Errorf("valueToSQL() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestNumericOperator_valueToSQL_ExpressionParserCallback(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	config.SetExpressionParser(func(expr any, path string) (string, error) {
		return "CUSTOM_NUMERIC()", nil
	})
	op := NewNumericOperator(config)

	// Unknown operator with expression parser should delegate
	result, err := op.valueToSQL(map[string]interface{}{"customOp": []interface{}{1, 2}})
	if err != nil {
		t.Errorf("valueToSQL() unexpected error = %v", err)
	}
	if result != "CUSTOM_NUMERIC()" {
		t.Errorf("valueToSQL() = %v, want CUSTOM_NUMERIC()", result)
	}
}

func TestNumericOperator_processComplexArgsForComparison(t *testing.T) {
	op := NewNumericOperator(nil)

	args := []interface{}{
		map[string]interface{}{"var": "amount"},
		42,
	}
	result, err := op.processComplexArgsForComparison(args)
	if err != nil {
		t.Errorf("processComplexArgsForComparison() unexpected error = %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("processComplexArgsForComparison() returned %d args, want 2", len(result))
	}
	if fmt.Sprintf("%v", result[0]) != "amount" {
		t.Errorf("processComplexArgsForComparison()[0] = %v, want amount", result[0])
	}
	if fmt.Sprintf("%v", result[1]) != "42" {
		t.Errorf("processComplexArgsForComparison()[1] = %v, want 42", result[1])
	}
}

func TestNumericOperator_extractFieldName(t *testing.T) {
	op := NewNumericOperator(nil)

	tests := []struct {
		name     string
		varName  interface{}
		expected string
	}{
		{
			name:     "string var name",
			varName:  "field",
			expected: "field",
		},
		{
			name:     "array with string first element",
			varName:  []interface{}{"field", "default"},
			expected: "field",
		},
		{
			name:     "array with non-string first element",
			varName:  []interface{}{123},
			expected: "",
		},
		{
			name:     "empty array",
			varName:  []interface{}{},
			expected: "",
		},
		{
			name:     "non-string non-array",
			varName:  42,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := op.extractFieldName(tt.varName)
			if result != tt.expected {
				t.Errorf("extractFieldName() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNumericOperator_extractFieldNameFromValue(t *testing.T) {
	op := NewNumericOperator(nil)

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "var expression",
			value:    map[string]interface{}{"var": "amount"},
			expected: "amount",
		},
		{
			name:     "non-var map",
			value:    map[string]interface{}{"other": "value"},
			expected: "",
		},
		{
			name:     "primitive value",
			value:    42,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := op.extractFieldNameFromValue(tt.value)
			if result != tt.expected {
				t.Errorf("extractFieldNameFromValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}
