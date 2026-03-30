package operators

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/h22rana/jsonlogic2sql/internal/dialect"
	"github.com/h22rana/jsonlogic2sql/internal/params"
)

func TestComparisonOperator_ToSQL(t *testing.T) {
	op := NewComparisonOperator(nil)

	tests := []struct {
		name     string
		operator string
		args     []interface{}
		expected string
		hasError bool
	}{
		// Equality tests
		{
			name:     "equality with numbers",
			operator: "==",
			args:     []interface{}{1, 2},
			expected: "1 = 2",
			hasError: false,
		},
		{
			name:     "equality with strings",
			operator: "==",
			args:     []interface{}{"hello", "world"},
			expected: "'hello' = 'world'",
			hasError: false,
		},
		{
			name:     "equality with var and literal",
			operator: "==",
			args:     []interface{}{map[string]interface{}{"var": "status"}, "pending"},
			expected: "status = 'pending'",
			hasError: false,
		},
		{
			name:     "equality with dotted var",
			operator: "==",
			args:     []interface{}{map[string]interface{}{"var": "user.status"}, "active"},
			expected: "user.status = 'active'",
			hasError: false,
		},
		// Inequality tests
		{
			name:     "inequality with numbers",
			operator: "!=",
			args:     []interface{}{1, 2},
			expected: "1 != 2",
			hasError: false,
		},

		// Greater than tests
		{
			name:     "greater than with numbers",
			operator: ">",
			args:     []interface{}{5, 3},
			expected: "5 > 3",
			hasError: false,
		},
		{
			name:     "greater than with var",
			operator: ">",
			args:     []interface{}{map[string]interface{}{"var": "amount"}, 1000},
			expected: "amount > 1000",
			hasError: false,
		},
		{
			name:     "greater than with dotted var",
			operator: ">",
			args:     []interface{}{map[string]interface{}{"var": "transaction.amount"}, 5000},
			expected: "transaction.amount > 5000",
			hasError: false,
		},

		// Greater than or equal tests
		{
			name:     "greater than or equal",
			operator: ">=",
			args:     []interface{}{5, 5},
			expected: "5 >= 5",
			hasError: false,
		},
		{
			name:     "greater than or equal with var",
			operator: ">=",
			args:     []interface{}{map[string]interface{}{"var": "failedAttempts"}, 5},
			expected: "failedAttempts >= 5",
			hasError: false,
		},

		// Less than tests
		{
			name:     "less than with numbers",
			operator: "<",
			args:     []interface{}{3, 5},
			expected: "3 < 5",
			hasError: false,
		},
		{
			name:     "less than with var",
			operator: "<",
			args:     []interface{}{map[string]interface{}{"var": "age"}, 18},
			expected: "age < 18",
			hasError: false,
		},

		// Less than or equal tests
		{
			name:     "less than or equal",
			operator: "<=",
			args:     []interface{}{5, 5},
			expected: "5 <= 5",
			hasError: false,
		},
		{
			name:     "less than or equal with var",
			operator: "<=",
			args:     []interface{}{map[string]interface{}{"var": "user.accountAgeDays"}, 7},
			expected: "user.accountAgeDays <= 7",
			hasError: false,
		},

		// Boolean tests
		{
			name:     "equality with booleans",
			operator: "==",
			args:     []interface{}{true, false},
			expected: "TRUE = FALSE",
			hasError: false,
		},
		{
			name:     "equality with var and boolean",
			operator: "==",
			args:     []interface{}{map[string]interface{}{"var": "verified"}, false},
			expected: "verified = FALSE",
			hasError: false,
		},

		// Null tests
		{
			name:     "equality with null",
			operator: "==",
			args:     []interface{}{nil, nil},
			expected: "NULL IS NULL",
			hasError: false,
		},
		{
			name:     "inequality with null",
			operator: "!=",
			args:     []interface{}{map[string]interface{}{"var": "field"}, nil},
			expected: "field IS NOT NULL",
			hasError: false,
		},
		{
			name:     "equality with var and null",
			operator: "==",
			args:     []interface{}{map[string]interface{}{"var": "deleted_at"}, nil},
			expected: "deleted_at IS NULL",
			hasError: false,
		},
		{
			name:     "equality with null and var",
			operator: "==",
			args:     []interface{}{nil, map[string]interface{}{"var": "deleted_at"}},
			expected: "deleted_at IS NULL",
			hasError: false,
		},
		{
			name:     "strict equality with null",
			operator: "===",
			args:     []interface{}{map[string]interface{}{"var": "field"}, nil},
			expected: "field IS NULL",
			hasError: false,
		},
		{
			name:     "strict inequality with null",
			operator: "!==",
			args:     []interface{}{map[string]interface{}{"var": "field"}, nil},
			expected: "field IS NOT NULL",
			hasError: false,
		},

		// in operator tests
		{
			name:     "in with string array",
			operator: "in",
			args:     []interface{}{map[string]interface{}{"var": "country"}, []interface{}{"CN", "RU"}},
			expected: "country IN ('CN', 'RU')",
			hasError: false,
		},
		{
			name:     "in with number array",
			operator: "in",
			args:     []interface{}{map[string]interface{}{"var": "status"}, []interface{}{1, 2, 3}},
			expected: "status IN (1, 2, 3)",
			hasError: false,
		},
		{
			name:     "in with empty array",
			operator: "in",
			args:     []interface{}{map[string]interface{}{"var": "field"}, []interface{}{}},
			expected: "",
			hasError: true,
		},
		{
			name:     "in with string containment",
			operator: "in",
			args:     []interface{}{map[string]interface{}{"var": "field"}, "not-array"},
			expected: "STRPOS('not-array', field) > 0",
			hasError: false,
		},

		// Error cases
		{
			name:     "too few arguments",
			operator: "==",
			args:     []interface{}{1},
			expected: "",
			hasError: true,
		},
		{
			name:     "too many arguments",
			operator: "==",
			args:     []interface{}{1, 2, 3},
			expected: "",
			hasError: true,
		},
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
					t.Errorf("ToSQL() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("ToSQL() unexpected error = %v", err)
				}
				if result != tt.expected {
					t.Errorf("ToSQL() = %v, expected %v", result, tt.expected)
				}
			}
		})
	}
}

func TestComparisonOperator_valueToSQL(t *testing.T) {
	op := NewComparisonOperator(nil)

	tests := []struct {
		name     string
		input    interface{}
		expected string
		hasError bool
	}{
		{
			name:     "literal string",
			input:    "hello",
			expected: "'hello'",
			hasError: false,
		},
		{
			name:     "literal number",
			input:    42,
			expected: "42",
			hasError: false,
		},
		{
			name:     "literal boolean",
			input:    true,
			expected: "TRUE",
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
			input:    map[string]interface{}{"var": "user.name"},
			expected: "user.name",
			hasError: false,
		},
		{
			name:     "var with default",
			input:    map[string]interface{}{"var": []interface{}{"status", "pending"}},
			expected: "COALESCE(status, 'pending')",
			hasError: false,
		},
		{
			name:     "non-var object",
			input:    map[string]interface{}{"other": "value"},
			expected: "",
			hasError: true,
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
					t.Errorf("valueToSQL() = %v, expected %v", result, tt.expected)
				}
			}
		})
	}
}

func TestComparisonOperator_strposFunc(t *testing.T) {
	tests := []struct {
		name     string
		dialect  dialect.Dialect
		haystack string
		needle   string
		expected string
	}{
		{
			name:     "BigQuery dialect",
			dialect:  dialect.DialectBigQuery,
			haystack: "description",
			needle:   "'test'",
			expected: "STRPOS(description, 'test')",
		},
		{
			name:     "Spanner dialect",
			dialect:  dialect.DialectSpanner,
			haystack: "description",
			needle:   "'test'",
			expected: "STRPOS(description, 'test')",
		},
		{
			name:     "DuckDB dialect",
			dialect:  dialect.DialectDuckDB,
			haystack: "description",
			needle:   "'test'",
			expected: "STRPOS(description, 'test')",
		},
		{
			name:     "PostgreSQL dialect",
			dialect:  dialect.DialectPostgreSQL,
			haystack: "description",
			needle:   "'test'",
			expected: "POSITION('test' IN description)",
		},
		{
			name:     "ClickHouse dialect",
			dialect:  dialect.DialectClickHouse,
			haystack: "description",
			needle:   "'test'",
			expected: "position(description, 'test')",
		},
		{
			name:     "Unspecified dialect defaults to STRPOS",
			dialect:  dialect.DialectUnspecified,
			haystack: "col",
			needle:   "'val'",
			expected: "STRPOS(col, 'val')",
		},
		{
			name:     "nil config defaults to STRPOS",
			dialect:  dialect.DialectUnspecified,
			haystack: "field",
			needle:   "'search'",
			expected: "STRPOS(field, 'search')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config *OperatorConfig
			if tt.name != "nil config defaults to STRPOS" {
				config = NewOperatorConfig(tt.dialect, nil)
			}
			op := NewComparisonOperator(config)
			result := op.strposFunc(tt.haystack, tt.needle)
			if result != tt.expected {
				t.Errorf("strposFunc() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComparisonOperator_processArithmeticExpression(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	op := NewComparisonOperator(config)

	tests := []struct {
		name     string
		operator string
		args     interface{}
		expected string
		hasError bool
	}{
		{
			name:     "addition",
			operator: "+",
			args:     []interface{}{1, 2},
			expected: "(1 + 2)",
			hasError: false,
		},
		{
			name:     "subtraction",
			operator: "-",
			args:     []interface{}{5, 3},
			expected: "(5 - 3)",
			hasError: false,
		},
		{
			name:     "multiplication",
			operator: "*",
			args:     []interface{}{2, 4},
			expected: "(2 * 4)",
			hasError: false,
		},
		{
			name:     "division",
			operator: "/",
			args:     []interface{}{10, 2},
			expected: "(10 / 2)",
			hasError: false,
		},
		{
			name:     "modulo",
			operator: "%",
			args:     []interface{}{7, 3},
			expected: "(7 % 3)",
			hasError: false,
		},
		{
			name:     "unary minus (negation)",
			operator: "-",
			args:     []interface{}{42},
			expected: "(-42)",
			hasError: false,
		},
		{
			name:     "unary plus (cast to number)",
			operator: "+",
			args:     []interface{}{"42"},
			expected: "CAST('42' AS NUMERIC)",
			hasError: false,
		},
		{
			name:     "multiple operands addition",
			operator: "+",
			args:     []interface{}{1, 2, 3},
			expected: "(1 + 2 + 3)",
			hasError: false,
		},
		{
			name:     "unsupported operator",
			operator: "^",
			args:     []interface{}{2, 3},
			expected: "",
			hasError: true,
		},
		{
			name:     "non-array args",
			operator: "+",
			args:     "invalid",
			expected: "",
			hasError: true,
		},
		{
			name:     "insufficient args for binary op",
			operator: "*",
			args:     []interface{}{5},
			expected: "",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.processArithmeticExpression(tt.operator, tt.args)
			if tt.hasError {
				if err == nil {
					t.Errorf("processArithmeticExpression() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("processArithmeticExpression() unexpected error = %v", err)
				}
				if result != tt.expected {
					t.Errorf("processArithmeticExpression() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestComparisonOperator_processComparisonExpression(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	op := NewComparisonOperator(config)

	tests := []struct {
		name     string
		operator string
		args     interface{}
		expected string
		hasError bool
	}{
		{
			name:     "greater than",
			operator: ">",
			args:     []interface{}{5, 3},
			expected: "(5 > 3)",
			hasError: false,
		},
		{
			name:     "greater than or equal",
			operator: ">=",
			args:     []interface{}{5, 5},
			expected: "(5 >= 5)",
			hasError: false,
		},
		{
			name:     "less than",
			operator: "<",
			args:     []interface{}{3, 5},
			expected: "(3 < 5)",
			hasError: false,
		},
		{
			name:     "less than or equal",
			operator: "<=",
			args:     []interface{}{3, 3},
			expected: "(3 <= 3)",
			hasError: false,
		},
		{
			name:     "equality",
			operator: "==",
			args:     []interface{}{1, 1},
			expected: "(1 = 1)",
			hasError: false,
		},
		{
			name:     "strict equality",
			operator: "===",
			args:     []interface{}{1, 1},
			expected: "(1 = 1)",
			hasError: false,
		},
		{
			name:     "inequality",
			operator: "!=",
			args:     []interface{}{1, 2},
			expected: "(1 != 2)",
			hasError: false,
		},
		{
			name:     "strict inequality",
			operator: "!==",
			args:     []interface{}{1, 2},
			expected: "(1 <> 2)",
			hasError: false,
		},
		{
			name:     "unsupported comparison",
			operator: "<>",
			args:     []interface{}{1, 2},
			expected: "",
			hasError: true,
		},
		{
			name:     "non-array args",
			operator: ">",
			args:     "invalid",
			expected: "",
			hasError: true,
		},
		{
			name:     "wrong number of args",
			operator: ">",
			args:     []interface{}{1, 2, 3},
			expected: "",
			hasError: true,
		},
		{
			name:     "single arg (insufficient)",
			operator: ">",
			args:     []interface{}{1},
			expected: "",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.processComparisonExpression(tt.operator, tt.args)
			if tt.hasError {
				if err == nil {
					t.Errorf("processComparisonExpression() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("processComparisonExpression() unexpected error = %v", err)
				}
				if result != tt.expected {
					t.Errorf("processComparisonExpression() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestComparisonOperator_processMinMaxExpression(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	op := NewComparisonOperator(config)

	tests := []struct {
		name     string
		operator string
		args     interface{}
		expected string
		hasError bool
	}{
		{
			name:     "max with two args",
			operator: "max",
			args:     []interface{}{5, 10},
			expected: "GREATEST(5, 10)",
			hasError: false,
		},
		{
			name:     "max with three args",
			operator: "max",
			args:     []interface{}{5, 10, 15},
			expected: "GREATEST(5, 10, 15)",
			hasError: false,
		},
		{
			name:     "min with two args",
			operator: "min",
			args:     []interface{}{5, 10},
			expected: "LEAST(5, 10)",
			hasError: false,
		},
		{
			name:     "min with three args",
			operator: "min",
			args:     []interface{}{5, 10, 3},
			expected: "LEAST(5, 10, 3)",
			hasError: false,
		},
		{
			name:     "unsupported min/max operator",
			operator: "avg",
			args:     []interface{}{1, 2},
			expected: "",
			hasError: true,
		},
		{
			name:     "non-array args",
			operator: "max",
			args:     "invalid",
			expected: "",
			hasError: true,
		},
		{
			name:     "insufficient args",
			operator: "max",
			args:     []interface{}{5},
			expected: "",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.processMinMaxExpression(tt.operator, tt.args)
			if tt.hasError {
				if err == nil {
					t.Errorf("processMinMaxExpression() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("processMinMaxExpression() unexpected error = %v", err)
				}
				if result != tt.expected {
					t.Errorf("processMinMaxExpression() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

// comparisonSchemaProvider is a configurable schema provider for comparison tests.
type comparisonSchemaProvider struct {
	fields      map[string]string   // field name -> type
	enumValues  map[string][]string // field name -> allowed values
	knownFields map[string]bool     // fields that exist
}

func newComparisonSchemaProvider(fields map[string]string) *comparisonSchemaProvider {
	known := make(map[string]bool)
	for k := range fields {
		known[k] = true
	}
	return &comparisonSchemaProvider{
		fields:      fields,
		enumValues:  make(map[string][]string),
		knownFields: known,
	}
}

func (m *comparisonSchemaProvider) HasField(fieldName string) bool {
	return m.knownFields[fieldName]
}

func (m *comparisonSchemaProvider) GetFieldType(fieldName string) string {
	return m.fields[fieldName]
}

func (m *comparisonSchemaProvider) ValidateField(_ string) error {
	return nil
}

func (m *comparisonSchemaProvider) IsArrayType(fieldName string) bool {
	return m.fields[fieldName] == "array"
}

func (m *comparisonSchemaProvider) IsStringType(fieldName string) bool {
	return m.fields[fieldName] == "string"
}

func (m *comparisonSchemaProvider) IsNumericType(fieldName string) bool {
	t := m.fields[fieldName]
	return t == "integer" || t == "number"
}

func (m *comparisonSchemaProvider) IsBooleanType(fieldName string) bool {
	return m.fields[fieldName] == "boolean"
}

func (m *comparisonSchemaProvider) IsEnumType(fieldName string) bool {
	_, ok := m.enumValues[fieldName]
	return ok
}

func (m *comparisonSchemaProvider) GetAllowedValues(fieldName string) []string {
	return m.enumValues[fieldName]
}

func (m *comparisonSchemaProvider) ValidateEnumValue(fieldName, value string) error {
	allowed := m.enumValues[fieldName]
	for _, v := range allowed {
		if v == value {
			return nil
		}
	}
	return fmt.Errorf("invalid enum value '%s' for field '%s'", value, fieldName)
}

func TestComparisonOperator_arrayMembershipSQL(t *testing.T) {
	tests := []struct {
		name     string
		dialect  dialect.Dialect
		valueSQL string
		arraySQL string
		expected string
	}{
		{
			name:     "BigQuery - IN UNNEST",
			dialect:  dialect.DialectBigQuery,
			valueSQL: "'test'",
			arraySQL: "tags",
			expected: "'test' IN UNNEST(tags)",
		},
		{
			name:     "Spanner - IN UNNEST",
			dialect:  dialect.DialectSpanner,
			valueSQL: "'test'",
			arraySQL: "tags",
			expected: "'test' IN UNNEST(tags)",
		},
		{
			name:     "PostgreSQL - ANY",
			dialect:  dialect.DialectPostgreSQL,
			valueSQL: "'test'",
			arraySQL: "tags",
			expected: "'test' = ANY(tags)",
		},
		{
			name:     "DuckDB - list_contains",
			dialect:  dialect.DialectDuckDB,
			valueSQL: "'test'",
			arraySQL: "tags",
			expected: "list_contains(tags, 'test')",
		},
		{
			name:     "ClickHouse - has",
			dialect:  dialect.DialectClickHouse,
			valueSQL: "'test'",
			arraySQL: "tags",
			expected: "has(tags, 'test')",
		},
		{
			name:     "Unspecified dialect - fallback to IN UNNEST",
			dialect:  dialect.DialectUnspecified,
			valueSQL: "42",
			arraySQL: "numbers",
			expected: "42 IN UNNEST(numbers)",
		},
		{
			name:     "nil config - fallback to IN UNNEST",
			dialect:  dialect.Dialect(0), // placeholder, will use nil config
			valueSQL: "'val'",
			arraySQL: "arr",
			expected: "'val' IN UNNEST(arr)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var op *ComparisonOperator
			if tt.name == "nil config - fallback to IN UNNEST" {
				op = NewComparisonOperator(nil)
			} else {
				config := NewOperatorConfig(tt.dialect, nil)
				op = NewComparisonOperator(config)
			}
			result := op.arrayMembershipSQL(tt.valueSQL, tt.arraySQL)
			if result != tt.expected {
				t.Errorf("arrayMembershipSQL() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestComparisonOperator_coerceValueForComparison(t *testing.T) {
	schema := newComparisonSchemaProvider(map[string]string{
		"age":         "integer",
		"price":       "number",
		"name":        "string",
		"description": "string",
		"is_active":   "boolean",
		"tags":        "array",
	})

	config := NewOperatorConfig(dialect.DialectBigQuery, schema)
	op := NewComparisonOperator(config)

	tests := []struct {
		name      string
		value     interface{}
		fieldName string
		expected  interface{}
	}{
		// String-to-number coercion for numeric fields
		{
			name:      "string integer to numeric field",
			value:     "50000",
			fieldName: "age",
			expected:  int64(50000),
		},
		{
			name:      "string float to numeric field",
			value:     "3.14",
			fieldName: "price",
			expected:  float64(3.14),
		},
		{
			name:      "non-numeric string to numeric field (no coercion)",
			value:     "hello",
			fieldName: "age",
			expected:  "hello",
		},
		// Number-to-string coercion for string fields
		{
			name:      "float64 integer to string field",
			value:     float64(5960),
			fieldName: "name",
			expected:  "5960",
		},
		{
			name:      "float64 fractional to string field",
			value:     float64(3.14),
			fieldName: "name",
			expected:  "3.14",
		},
		{
			name:      "float32 to string field",
			value:     float32(1.5),
			fieldName: "name",
			expected:  "1.5",
		},
		{
			name:      "int to string field",
			value:     42,
			fieldName: "name",
			expected:  "42",
		},
		{
			name:      "int8 to string field",
			value:     int8(10),
			fieldName: "name",
			expected:  "10",
		},
		{
			name:      "int16 to string field",
			value:     int16(100),
			fieldName: "name",
			expected:  "100",
		},
		{
			name:      "int32 to string field",
			value:     int32(1000),
			fieldName: "name",
			expected:  "1000",
		},
		{
			name:      "int64 to string field",
			value:     int64(9999),
			fieldName: "name",
			expected:  "9999",
		},
		{
			name:      "uint to string field",
			value:     uint(7),
			fieldName: "name",
			expected:  "7",
		},
		{
			name:      "uint8 to string field",
			value:     uint8(8),
			fieldName: "name",
			expected:  "8",
		},
		{
			name:      "uint16 to string field",
			value:     uint16(16),
			fieldName: "name",
			expected:  "16",
		},
		{
			name:      "uint32 to string field",
			value:     uint32(32),
			fieldName: "name",
			expected:  "32",
		},
		{
			name:      "uint64 to string field",
			value:     uint64(64),
			fieldName: "name",
			expected:  "64",
		},
		// No coercion cases
		{
			name:      "nil schema returns value as-is",
			value:     "test",
			fieldName: "",
			expected:  "test",
		},
		{
			name:      "boolean field - no coercion",
			value:     "true",
			fieldName: "is_active",
			expected:  "true",
		},
		{
			name:      "array field - no coercion",
			value:     "something",
			fieldName: "tags",
			expected:  "something",
		},
		{
			name:      "number already a number for numeric field",
			value:     42,
			fieldName: "age",
			expected:  42,
		},
		{
			name:      "string already a string for string field",
			value:     "hello",
			fieldName: "name",
			expected:  "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := op.coerceValueForComparison(tt.value, tt.fieldName)
			if fmt.Sprintf("%v", result) != fmt.Sprintf("%v", tt.expected) {
				t.Errorf("coerceValueForComparison() = %v (%T), want %v (%T)", result, result, tt.expected, tt.expected)
			}
		})
	}

	// Test with nil schema
	opNoSchema := NewComparisonOperator(nil)
	result := opNoSchema.coerceValueForComparison("50000", "age")
	if result != "50000" {
		t.Errorf("coerceValueForComparison() with nil schema = %v, want '50000'", result)
	}
}

func TestComparisonOperator_validateEnumValue(t *testing.T) {
	schema := newComparisonSchemaProvider(map[string]string{
		"status":  "string",
		"country": "string",
		"age":     "integer",
	})
	schema.enumValues["status"] = []string{"active", "inactive", "pending"}
	schema.enumValues["country"] = []string{"US", "UK", "JP"}

	config := NewOperatorConfig(dialect.DialectBigQuery, schema)
	op := NewComparisonOperator(config)

	tests := []struct {
		name      string
		value     interface{}
		fieldName string
		hasError  bool
	}{
		{
			name:      "valid enum value",
			value:     "active",
			fieldName: "status",
			hasError:  false,
		},
		{
			name:      "another valid enum value",
			value:     "pending",
			fieldName: "status",
			hasError:  false,
		},
		{
			name:      "invalid enum value",
			value:     "deleted",
			fieldName: "status",
			hasError:  true,
		},
		{
			name:      "valid country enum value",
			value:     "US",
			fieldName: "country",
			hasError:  false,
		},
		{
			name:      "invalid country enum value",
			value:     "XX",
			fieldName: "country",
			hasError:  true,
		},
		{
			name:      "null value skips validation",
			value:     nil,
			fieldName: "status",
			hasError:  false,
		},
		{
			name:      "non-enum field skips validation",
			value:     "anything",
			fieldName: "age",
			hasError:  false,
		},
		{
			name:      "empty field name skips validation",
			value:     "something",
			fieldName: "",
			hasError:  false,
		},
		{
			name:      "non-string value converted to string for validation",
			value:     123,
			fieldName: "status",
			hasError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := op.validateEnumValue(tt.value, tt.fieldName)
			if tt.hasError {
				if err == nil {
					t.Errorf("validateEnumValue() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("validateEnumValue() unexpected error = %v", err)
				}
			}
		})
	}

	// Test with nil schema
	opNoSchema := NewComparisonOperator(nil)
	if err := opNoSchema.validateEnumValue("anything", "status"); err != nil {
		t.Errorf("validateEnumValue() with nil schema should return nil, got %v", err)
	}
}

func TestComparisonOperator_extractFieldName(t *testing.T) {
	op := NewComparisonOperator(nil)

	tests := []struct {
		name     string
		varName  interface{}
		expected string
	}{
		{
			name:     "string var name",
			varName:  "fieldName",
			expected: "fieldName",
		},
		{
			name:     "array with string first element",
			varName:  []interface{}{"fieldName", "default"},
			expected: "fieldName",
		},
		{
			name:     "array with non-string first element",
			varName:  []interface{}{123, "default"},
			expected: "",
		},
		{
			name:     "empty array",
			varName:  []interface{}{},
			expected: "",
		},
		{
			name:     "numeric var name",
			varName:  42,
			expected: "",
		},
		{
			name:     "nil var name",
			varName:  nil,
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

func TestComparisonOperator_valueToSQL_Extended(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	op := NewComparisonOperator(config)

	tests := []struct {
		name     string
		input    interface{}
		expected string
		hasError bool
	}{
		// ProcessedValue - SQL
		{
			name:     "ProcessedValue with SQL",
			input:    ProcessedValue{Value: "some_column > 5", IsSQL: true},
			expected: "some_column > 5",
			hasError: false,
		},
		// ProcessedValue - Literal
		{
			name:     "ProcessedValue with literal string",
			input:    ProcessedValue{Value: "hello", IsSQL: false},
			expected: "'hello'",
			hasError: false,
		},
		// nil value
		{
			name:     "nil value",
			input:    nil,
			expected: "NULL",
			hasError: false,
		},
		// boolean false
		{
			name:     "boolean false",
			input:    false,
			expected: "FALSE",
			hasError: false,
		},
		// empty var name (current element reference)
		{
			name:     "empty var name returns elem",
			input:    map[string]interface{}{"var": ""},
			expected: "elem",
			hasError: false,
		},
		// Arithmetic expression inside comparison
		{
			name:     "addition expression",
			input:    map[string]interface{}{"+": []interface{}{map[string]interface{}{"var": "x"}, 10}},
			expected: "(x + 10)",
			hasError: false,
		},
		{
			name:     "subtraction expression",
			input:    map[string]interface{}{"-": []interface{}{map[string]interface{}{"var": "x"}, 5}},
			expected: "(x - 5)",
			hasError: false,
		},
		{
			name:     "multiplication expression",
			input:    map[string]interface{}{"*": []interface{}{map[string]interface{}{"var": "x"}, 2}},
			expected: "(x * 2)",
			hasError: false,
		},
		{
			name:     "division expression",
			input:    map[string]interface{}{"/": []interface{}{map[string]interface{}{"var": "x"}, 2}},
			expected: "(x / 2)",
			hasError: false,
		},
		{
			name:     "modulo expression",
			input:    map[string]interface{}{"%": []interface{}{map[string]interface{}{"var": "x"}, 3}},
			expected: "(x % 3)",
			hasError: false,
		},
		// Comparison expression inside comparison
		{
			name:     "nested greater than expression",
			input:    map[string]interface{}{">": []interface{}{map[string]interface{}{"var": "a"}, 5}},
			expected: "(a > 5)",
			hasError: false,
		},
		// Min/Max expression
		{
			name:     "max expression",
			input:    map[string]interface{}{"max": []interface{}{map[string]interface{}{"var": "a"}, 100}},
			expected: "GREATEST(a, 100)",
			hasError: false,
		},
		{
			name:     "min expression",
			input:    map[string]interface{}{"min": []interface{}{map[string]interface{}{"var": "a"}, 0}},
			expected: "LEAST(a, 0)",
			hasError: false,
		},
		// If expression
		{
			name: "if expression",
			input: map[string]interface{}{"if": []interface{}{
				map[string]interface{}{">": []interface{}{map[string]interface{}{"var": "x"}, 0}},
				"positive",
				"negative",
			}},
			expected: "CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END",
			hasError: false,
		},
		{
			name:     "if expression non-array args error",
			input:    map[string]interface{}{"if": "invalid"},
			expected: "",
			hasError: true,
		},
		// Cat/Substr string operations
		{
			name:     "cat expression",
			input:    map[string]interface{}{"cat": []interface{}{"hello", " ", "world"}},
			expected: "CONCAT('hello', ' ', 'world')",
			hasError: false,
		},
		{
			name:     "cat expression non-array error",
			input:    map[string]interface{}{"cat": "invalid"},
			expected: "",
			hasError: true,
		},
		{
			name:     "substr expression",
			input:    map[string]interface{}{"substr": []interface{}{"hello", 1, 3}},
			expected: "SUBSTR('hello', 2, 3)",
			hasError: false,
		},
		{
			name:     "substr expression non-array error",
			input:    map[string]interface{}{"substr": "invalid"},
			expected: "",
			hasError: true,
		},
		// Array should error
		{
			name:     "array value should error",
			input:    []interface{}{1, 2, 3},
			expected: "",
			hasError: true,
		},
		// Array operators
		{
			name: "reduce expression non-array error",
			input: map[string]interface{}{
				"reduce": "invalid",
			},
			expected: "",
			hasError: true,
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

func TestComparisonOperator_handleIn_WithVarRightSide(t *testing.T) {
	// Test in with var on right side and schema indicating array type
	schema := newComparisonSchemaProvider(map[string]string{
		"tags":        "array",
		"description": "string",
		"name":        "string",
	})

	tests := []struct {
		name     string
		dialect  dialect.Dialect
		leftArg  interface{}
		rightArg interface{}
		expected string
		hasError bool
	}{
		// Array field on right side: use arrayMembershipSQL
		{
			name:     "BigQuery in with array var on right",
			dialect:  dialect.DialectBigQuery,
			leftArg:  "test",
			rightArg: map[string]interface{}{"var": "tags"},
			expected: "'test' IN UNNEST(tags)",
			hasError: false,
		},
		{
			name:     "PostgreSQL in with array var on right",
			dialect:  dialect.DialectPostgreSQL,
			leftArg:  "test",
			rightArg: map[string]interface{}{"var": "tags"},
			expected: "'test' = ANY(tags)",
			hasError: false,
		},
		{
			name:     "DuckDB in with array var on right",
			dialect:  dialect.DialectDuckDB,
			leftArg:  "test",
			rightArg: map[string]interface{}{"var": "tags"},
			expected: "list_contains(tags, 'test')",
			hasError: false,
		},
		{
			name:     "ClickHouse in with array var on right",
			dialect:  dialect.DialectClickHouse,
			leftArg:  "test",
			rightArg: map[string]interface{}{"var": "tags"},
			expected: "has(tags, 'test')",
			hasError: false,
		},
		// String field on right side: use STRPOS
		{
			name:     "BigQuery in with string var on right",
			dialect:  dialect.DialectBigQuery,
			leftArg:  "test",
			rightArg: map[string]interface{}{"var": "description"},
			expected: "STRPOS(description, 'test') > 0",
			hasError: false,
		},
		{
			name:     "PostgreSQL in with string var on right",
			dialect:  dialect.DialectPostgreSQL,
			leftArg:  "test",
			rightArg: map[string]interface{}{"var": "description"},
			expected: "POSITION('test' IN description) > 0",
			hasError: false,
		},
		{
			name:     "ClickHouse in with string var on right",
			dialect:  dialect.DialectClickHouse,
			leftArg:  "test",
			rightArg: map[string]interface{}{"var": "description"},
			expected: "position(description, 'test') > 0",
			hasError: false,
		},
		// Number containment (right side is a number literal)
		{
			name:     "in with number on right side",
			dialect:  dialect.DialectBigQuery,
			leftArg:  "3",
			rightArg: float64(12345),
			expected: "STRPOS(12345, '3') > 0",
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewOperatorConfig(tt.dialect, schema)
			op := NewComparisonOperator(config)
			result, err := op.ToSQL("in", []interface{}{tt.leftArg, tt.rightArg})
			if tt.hasError {
				if err == nil {
					t.Errorf("ToSQL() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("ToSQL() unexpected error = %v", err)
				}
				if result != tt.expected {
					t.Errorf("ToSQL() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestComparisonOperator_handleIn_NoSchema_VarRightSide(t *testing.T) {
	// Without schema, test heuristic based on left side being a literal
	op := NewComparisonOperator(nil)

	tests := []struct {
		name     string
		leftArg  interface{}
		rightArg interface{}
		expected string
		hasError bool
	}{
		{
			name:     "literal string left and var right - string containment",
			leftArg:  "search",
			rightArg: map[string]interface{}{"var": "field"},
			expected: "STRPOS(field, 'search') > 0",
			hasError: false,
		},
		{
			name:     "var left and var right - array membership fallback",
			leftArg:  map[string]interface{}{"var": "item"},
			rightArg: map[string]interface{}{"var": "collection"},
			expected: "item IN UNNEST(collection)",
			hasError: false,
		},
		{
			name:     "in with unsupported right side type",
			leftArg:  "test",
			rightArg: true,
			expected: "",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.ToSQL("in", []interface{}{tt.leftArg, tt.rightArg})
			if tt.hasError {
				if err == nil {
					t.Errorf("ToSQL() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("ToSQL() unexpected error = %v", err)
				}
				if result != tt.expected {
					t.Errorf("ToSQL() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestComparisonOperator_handleIn_WithEnumValidation(t *testing.T) {
	schema := newComparisonSchemaProvider(map[string]string{
		"status": "string",
	})
	schema.enumValues["status"] = []string{"active", "inactive", "pending"}

	config := NewOperatorConfig(dialect.DialectBigQuery, schema)
	op := NewComparisonOperator(config)

	// Valid enum values in array
	result, err := op.ToSQL("in", []interface{}{
		map[string]interface{}{"var": "status"},
		[]interface{}{"active", "pending"},
	})
	if err != nil {
		t.Errorf("ToSQL() unexpected error = %v", err)
	}
	expected := "status IN ('active', 'pending')"
	if result != expected {
		t.Errorf("ToSQL() = %v, want %v", result, expected)
	}

	// Invalid enum values in array
	_, err = op.ToSQL("in", []interface{}{
		map[string]interface{}{"var": "status"},
		[]interface{}{"active", "deleted"},
	})
	if err == nil {
		t.Errorf("ToSQL() expected error for invalid enum value, got nil")
	}
}

func TestComparisonOperator_ToSQL_WithSchemaCoercion(t *testing.T) {
	schema := newComparisonSchemaProvider(map[string]string{
		"age":  "integer",
		"name": "string",
	})

	config := NewOperatorConfig(dialect.DialectBigQuery, schema)
	op := NewComparisonOperator(config)

	tests := []struct {
		name     string
		operator string
		args     []interface{}
		expected string
		hasError bool
	}{
		{
			name:     "string to number coercion - left field numeric, right literal string",
			operator: ">=",
			args:     []interface{}{map[string]interface{}{"var": "age"}, "50000"},
			expected: "age >= 50000",
			hasError: false,
		},
		{
			name:     "number to string coercion - left field string, right literal number",
			operator: "==",
			args:     []interface{}{map[string]interface{}{"var": "name"}, float64(5960)},
			expected: "name = '5960'",
			hasError: false,
		},
		{
			name:     "coercion for right field and left literal",
			operator: "==",
			args:     []interface{}{"50000", map[string]interface{}{"var": "age"}},
			expected: "50000 = age",
			hasError: false,
		},
		{
			name:     "coercion for right field string and left literal number",
			operator: "==",
			args:     []interface{}{float64(42), map[string]interface{}{"var": "name"}},
			expected: "'42' = name",
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.ToSQL(tt.operator, tt.args)
			if tt.hasError {
				if err == nil {
					t.Errorf("ToSQL() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("ToSQL() unexpected error = %v", err)
				}
				if result != tt.expected {
					t.Errorf("ToSQL() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestComparisonOperator_handleIn_WithSchemaArrayVar(t *testing.T) {
	schema := newComparisonSchemaProvider(map[string]string{
		"tags": "array",
	})

	config := NewOperatorConfig(dialect.DialectBigQuery, schema)
	op := NewComparisonOperator(config)

	// Test in with array-type field on right side (array membership)
	result, err := op.ToSQL("in", []interface{}{
		"test",
		map[string]interface{}{"var": "tags"},
	})
	if err != nil {
		t.Errorf("ToSQL() unexpected error = %v", err)
	}
	expected := "'test' IN UNNEST(tags)"
	if result != expected {
		t.Errorf("ToSQL() = %v, want %v", result, expected)
	}
}

func TestComparisonOperator_valueToSQL_ExpressionParserCallback(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	config.SetExpressionParser(func(expr any, path string) (string, error) {
		return "CUSTOM_FUNC()", nil
	})
	op := NewComparisonOperator(config)

	// Unknown operator with expression parser set should delegate
	result, err := op.valueToSQL(map[string]interface{}{"customOp": []interface{}{1, 2}})
	if err != nil {
		t.Errorf("valueToSQL() unexpected error = %v", err)
	}
	if result != "CUSTOM_FUNC()" {
		t.Errorf("valueToSQL() = %v, want CUSTOM_FUNC()", result)
	}
}

func TestComparisonOperator_handleIn_StringCoercion(t *testing.T) {
	// Test that number left side gets coerced to string when right side is a string field
	schema := newComparisonSchemaProvider(map[string]string{
		"name": "string",
	})

	config := NewOperatorConfig(dialect.DialectBigQuery, schema)
	op := NewComparisonOperator(config)

	// Number left side with string var on right - should coerce number to string
	result, err := op.ToSQL("in", []interface{}{
		float64(123),
		map[string]interface{}{"var": "name"},
	})
	if err != nil {
		t.Errorf("ToSQL() unexpected error = %v", err)
	}
	expected := "STRPOS(name, '123') > 0"
	if result != expected {
		t.Errorf("ToSQL() = %v, want %v", result, expected)
	}
}

func assertQueryParams(t *testing.T, got, want []params.QueryParam) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("Params() len %d, want %d\ngot: %#v", len(got), len(want), got)
	}
	for i := range got {
		if got[i].Name != want[i].Name || !reflect.DeepEqual(got[i].Value, want[i].Value) {
			t.Fatalf("Params()[%d] = {Name:%q Value:%#v (%T)}, want {Name:%q Value:%#v (%T)}",
				i, got[i].Name, got[i].Value, got[i].Value,
				want[i].Name, want[i].Value, want[i].Value)
		}
	}
}

func TestComparisonOperator_ToSQLParam(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	op := NewComparisonOperator(config)

	tests := []struct {
		name       string
		operator   string
		args       []interface{}
		wantSQL    string
		wantParams []params.QueryParam
		wantErr    bool
	}{
		{
			name:     "equality with var and literal",
			operator: "==",
			args:     []interface{}{map[string]interface{}{"var": "status"}, "pending"},
			wantSQL:  "status = @p1",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: "pending"},
			},
		},
		{
			name:     "equality with numbers",
			operator: "==",
			args:     []interface{}{1, 2},
			wantSQL:  "@p1 = @p2",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: 1},
				{Name: "p2", Value: 2},
			},
		},
		{
			name:       "equality with null",
			operator:   "==",
			args:       []interface{}{nil, nil},
			wantSQL:    "NULL IS NULL",
			wantParams: nil,
		},
		{
			name:       "var and null",
			operator:   "==",
			args:       []interface{}{map[string]interface{}{"var": "field"}, nil},
			wantSQL:    "field IS NULL",
			wantParams: nil,
		},
		{
			name:       "inequality with var and null",
			operator:   "!=",
			args:       []interface{}{map[string]interface{}{"var": "field"}, nil},
			wantSQL:    "field IS NOT NULL",
			wantParams: nil,
		},
		{
			name:       "strict equality !==  with null",
			operator:   "!==",
			args:       []interface{}{map[string]interface{}{"var": "field"}, nil},
			wantSQL:    "field IS NOT NULL",
			wantParams: nil,
		},
		{
			name:     "greater than with var and number",
			operator: ">",
			args:     []interface{}{map[string]interface{}{"var": "amount"}, 1000},
			wantSQL:  "amount > @p1",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: 1000},
			},
		},
		{
			name:     "in with array",
			operator: "in",
			args:     []interface{}{map[string]interface{}{"var": "country"}, []interface{}{"CN", "RU"}},
			wantSQL:  "country IN (@p1, @p2)",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: "CN"},
				{Name: "p2", Value: "RU"},
			},
		},
		{
			name:     "chained comparison",
			operator: "<",
			args: []interface{}{
				10,
				map[string]interface{}{"var": "age"},
				30,
			},
			wantSQL: "(@p1 < age AND age < @p2)",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: 10},
				{Name: "p2", Value: 30},
			},
		},
		{
			name:     "too few arguments",
			operator: "==",
			args:     []interface{}{1},
			wantErr:  true,
		},
		{
			name:     "in with empty array",
			operator: "in",
			args:     []interface{}{map[string]interface{}{"var": "field"}, []interface{}{}},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pc := params.NewParamCollector(params.PlaceholderNamed)
			got, err := op.ToSQLParam(tt.operator, tt.args, pc)
			if tt.wantErr {
				if err == nil {
					t.Fatal("ToSQLParam() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("ToSQLParam() unexpected error = %v", err)
			}
			if got != tt.wantSQL {
				t.Errorf("ToSQLParam() SQL = %q, want %q", got, tt.wantSQL)
			}
			assertQueryParams(t, pc.Params(), tt.wantParams)
		})
	}
}

func TestComparisonOperator_valueToSQLParam(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	op := NewComparisonOperator(config)

	tests := []struct {
		name       string
		input      interface{}
		wantSQL    string
		wantParams []params.QueryParam
		wantErr    bool
	}{
		{
			name:    "literal string",
			input:   "hello",
			wantSQL: "@p1",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: "hello"},
			},
		},
		{
			name:    "literal number",
			input:   42,
			wantSQL: "@p1",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: 42},
			},
		},
		{
			name:       "literal boolean",
			input:      true,
			wantSQL:    "TRUE",
			wantParams: nil,
		},
		{
			name:       "var expression",
			input:      map[string]interface{}{"var": "amount"},
			wantSQL:    "amount",
			wantParams: nil,
		},
		{
			name:       "ProcessedValue SQL",
			input:      ProcessedValue{Value: "some_col + 1", IsSQL: true},
			wantSQL:    "some_col + 1",
			wantParams: nil,
		},
		{
			name:    "ProcessedValue literal",
			input:   ProcessedValue{Value: "inner", IsSQL: false},
			wantSQL: "@p1",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: "inner"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pc := params.NewParamCollector(params.PlaceholderNamed)
			got, err := op.valueToSQLParam(tt.input, pc)
			if tt.wantErr {
				if err == nil {
					t.Fatal("valueToSQLParam() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("valueToSQLParam() unexpected error = %v", err)
			}
			if got != tt.wantSQL {
				t.Errorf("valueToSQLParam() = %q, want %q", got, tt.wantSQL)
			}
			assertQueryParams(t, pc.Params(), tt.wantParams)
		})
	}
}

func TestComparisonOperator_handleInParam(t *testing.T) {
	schema := newComparisonSchemaProvider(map[string]string{
		"tags":        "array",
		"description": "string",
		"region":      "string",
	})
	config := NewOperatorConfig(dialect.DialectBigQuery, schema)
	op := NewComparisonOperator(config)

	t.Run("array type var on right uses arrayMembershipSQL", func(t *testing.T) {
		pc := params.NewParamCollector(params.PlaceholderNamed)
		got, err := op.handleInParam("needle", map[string]interface{}{"var": "tags"}, pc)
		if err != nil {
			t.Fatalf("handleInParam() error = %v", err)
		}
		want := "@p1 IN UNNEST(tags)"
		if got != want {
			t.Errorf("handleInParam() = %q, want %q", got, want)
		}
		assertQueryParams(t, pc.Params(), []params.QueryParam{{Name: "p1", Value: "needle"}})
	})

	t.Run("string type var on right uses strposFunc with parameterized left", func(t *testing.T) {
		pc := params.NewParamCollector(params.PlaceholderNamed)
		got, err := op.handleInParam("probe", map[string]interface{}{"var": "description"}, pc)
		if err != nil {
			t.Fatalf("handleInParam() error = %v", err)
		}
		want := "STRPOS(description, @p1) > 0"
		if got != want {
			t.Errorf("handleInParam() = %q, want %q", got, want)
		}
		assertQueryParams(t, pc.Params(), []params.QueryParam{{Name: "p1", Value: "probe"}})
	})

	t.Run("array of literals parameterized", func(t *testing.T) {
		pc := params.NewParamCollector(params.PlaceholderNamed)
		leftOriginal := map[string]interface{}{"var": "region"}
		got, err := op.handleInParam(leftOriginal, []interface{}{"EU", "APAC", "US"}, pc)
		if err != nil {
			t.Fatalf("handleInParam() error = %v", err)
		}
		want := "region IN (@p1, @p2, @p3)"
		if got != want {
			t.Errorf("handleInParam() = %q, want %q", got, want)
		}
		assertQueryParams(t, pc.Params(), []params.QueryParam{
			{Name: "p1", Value: "EU"},
			{Name: "p2", Value: "APAC"},
			{Name: "p3", Value: "US"},
		})
	})

	t.Run("string containment without schema", func(t *testing.T) {
		noSchemaConfig := NewOperatorConfig(dialect.DialectBigQuery, nil)
		noSchemaOp := NewComparisonOperator(noSchemaConfig)
		pc := params.NewParamCollector(params.PlaceholderNamed)
		got, err := noSchemaOp.handleInParam("foo", map[string]interface{}{"var": "bar"}, pc)
		if err != nil {
			t.Fatalf("handleInParam() error = %v", err)
		}
		want := "STRPOS(bar, @p1) > 0"
		if got != want {
			t.Errorf("handleInParam() = %q, want %q", got, want)
		}
		assertQueryParams(t, pc.Params(), []params.QueryParam{{Name: "p1", Value: "foo"}})
	})

	t.Run("schema coercion for string field", func(t *testing.T) {
		pc := params.NewParamCollector(params.PlaceholderNamed)
		got, err := op.handleInParam(float64(123), map[string]interface{}{"var": "description"}, pc)
		if err != nil {
			t.Fatalf("handleInParam() error = %v", err)
		}
		want := "STRPOS(description, @p1) > 0"
		if got != want {
			t.Errorf("handleInParam() = %q, want %q", got, want)
		}
		if len(pc.Params()) != 1 {
			t.Fatalf("expected 1 param, got %d: %v", len(pc.Params()), pc.Params())
		}
		if pc.Params()[0].Value != "123" {
			t.Errorf("param value = %v (%T), want string \"123\"", pc.Params()[0].Value, pc.Params()[0].Value)
		}
	})

	t.Run("ProcessedValue SQL literal treated as string containment", func(t *testing.T) {
		noSchemaConfig := NewOperatorConfig(dialect.DialectBigQuery, nil)
		noSchemaOp := NewComparisonOperator(noSchemaConfig)
		pc := params.NewParamCollector(params.PlaceholderNamed)
		got, err := noSchemaOp.handleInParam(
			ProcessedValue{IsSQL: true, Value: "'foo'"},
			map[string]interface{}{"var": "col"},
			pc,
		)
		if err != nil {
			t.Fatalf("handleInParam() error = %v", err)
		}
		want := "STRPOS(col, 'foo') > 0"
		if got != want {
			t.Errorf("handleInParam() = %q, want %q", got, want)
		}
	})

	t.Run("ProcessedValue placeholder for string param uses string containment", func(t *testing.T) {
		noSchemaConfig := NewOperatorConfig(dialect.DialectBigQuery, nil)
		noSchemaOp := NewComparisonOperator(noSchemaConfig)
		pc := params.NewParamCollector(params.PlaceholderNamed)
		pc.Add("hello") // @p1 = "hello" (string)
		got, err := noSchemaOp.handleInParam(
			ProcessedValue{IsSQL: true, Value: "@p1"},
			map[string]interface{}{"var": "col"},
			pc,
		)
		if err != nil {
			t.Fatalf("handleInParam() error = %v", err)
		}
		want := "STRPOS(col, @p1) > 0"
		if got != want {
			t.Errorf("handleInParam() = %q, want %q", got, want)
		}
	})

	t.Run("ProcessedValue placeholder for numeric param uses array membership", func(t *testing.T) {
		noSchemaConfig := NewOperatorConfig(dialect.DialectBigQuery, nil)
		noSchemaOp := NewComparisonOperator(noSchemaConfig)
		pc := params.NewParamCollector(params.PlaceholderNamed)
		pc.Add(float64(42)) // @p1 = 42 (numeric)
		got, err := noSchemaOp.handleInParam(
			ProcessedValue{IsSQL: true, Value: "@p1"},
			map[string]interface{}{"var": "col"},
			pc,
		)
		if err != nil {
			t.Fatalf("handleInParam() error = %v", err)
		}
		want := "@p1 IN UNNEST(col)"
		if got != want {
			t.Errorf("handleInParam() = %q, want %q", got, want)
		}
	})

	t.Run("ProcessedValue SQL expression uses array membership", func(t *testing.T) {
		noSchemaConfig := NewOperatorConfig(dialect.DialectBigQuery, nil)
		noSchemaOp := NewComparisonOperator(noSchemaConfig)
		pc := params.NewParamCollector(params.PlaceholderNamed)
		pc.Add("hello") // @p1 = "hello"
		got, err := noSchemaOp.handleInParam(
			ProcessedValue{IsSQL: true, Value: "LOWER(@p1)"},
			map[string]interface{}{"var": "col"},
			pc,
		)
		if err != nil {
			t.Fatalf("handleInParam() error = %v", err)
		}
		want := "LOWER(@p1) IN UNNEST(col)"
		if got != want {
			t.Errorf("handleInParam() = %q, want %q", got, want)
		}
	})
}

func TestComparisonOperator_InDoesNotMutateInputArray(t *testing.T) {
	schema := newComparisonSchemaProvider(map[string]string{
		"region": "string",
	})
	config := NewOperatorConfig(dialect.DialectBigQuery, schema)
	op := NewComparisonOperator(config)

	t.Run("non-parameterized path", func(t *testing.T) {
		arr := []interface{}{float64(1), float64(2)}
		original := append([]interface{}(nil), arr...)

		_, err := op.ToSQL("in", []interface{}{
			map[string]interface{}{"var": "region"},
			arr,
		})
		if err != nil {
			t.Fatalf("ToSQL(in) unexpected error: %v", err)
		}
		if !reflect.DeepEqual(arr, original) {
			t.Fatalf("input array mutated: got %#v, want %#v", arr, original)
		}
	})

	t.Run("parameterized path", func(t *testing.T) {
		arr := []interface{}{float64(1), float64(2)}
		original := append([]interface{}(nil), arr...)
		pc := params.NewParamCollector(params.PlaceholderNamed)

		_, err := op.handleInParam(
			map[string]interface{}{"var": "region"},
			arr,
			pc,
		)
		if err != nil {
			t.Fatalf("handleInParam() unexpected error: %v", err)
		}
		if !reflect.DeepEqual(arr, original) {
			t.Fatalf("input array mutated: got %#v, want %#v", arr, original)
		}
	})
}

func TestComparisonOperator_processArithmeticExpressionParam(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	op := NewComparisonOperator(config)

	tests := []struct {
		name       string
		operator   string
		args       interface{}
		wantSQL    string
		wantParams []params.QueryParam
		wantErr    bool
	}{
		{
			name:     "addition",
			operator: "+",
			args:     []interface{}{1, 2},
			wantSQL:  "(@p1 + @p2)",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: 1},
				{Name: "p2", Value: 2},
			},
		},
		{
			name:     "unary minus",
			operator: "-",
			args:     []interface{}{42},
			wantSQL:  "(-@p1)",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: 42},
			},
		},
		{
			name:     "unsupported operator",
			operator: "^",
			args:     []interface{}{2, 3},
			wantErr:  true,
		},
		{
			name:     "non-array args",
			operator: "+",
			args:     "invalid",
			wantErr:  true,
		},
		{
			name:     "insufficient args for binary op",
			operator: "*",
			args:     []interface{}{5},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pc := params.NewParamCollector(params.PlaceholderNamed)
			got, err := op.processArithmeticExpressionParam(tt.operator, tt.args, pc)
			if tt.wantErr {
				if err == nil {
					t.Fatal("processArithmeticExpressionParam() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("processArithmeticExpressionParam() unexpected error = %v", err)
			}
			if got != tt.wantSQL {
				t.Errorf("processArithmeticExpressionParam() = %q, want %q", got, tt.wantSQL)
			}
			assertQueryParams(t, pc.Params(), tt.wantParams)
		})
	}
}

func TestComparisonOperator_processComparisonExpressionParam(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	op := NewComparisonOperator(config)

	tests := []struct {
		name       string
		operator   string
		args       interface{}
		wantSQL    string
		wantParams []params.QueryParam
		wantErr    bool
	}{
		{
			name:     "greater than",
			operator: ">",
			args:     []interface{}{5, 3},
			wantSQL:  "(@p1 > @p2)",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: 5},
				{Name: "p2", Value: 3},
			},
		},
		{
			name:     "unsupported comparison",
			operator: "<>",
			args:     []interface{}{1, 2},
			wantErr:  true,
		},
		{
			name:     "non-array args",
			operator: ">",
			args:     "invalid",
			wantErr:  true,
		},
		{
			name:     "wrong number of args",
			operator: ">",
			args:     []interface{}{1, 2, 3},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pc := params.NewParamCollector(params.PlaceholderNamed)
			got, err := op.processComparisonExpressionParam(tt.operator, tt.args, pc)
			if tt.wantErr {
				if err == nil {
					t.Fatal("processComparisonExpressionParam() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("processComparisonExpressionParam() unexpected error = %v", err)
			}
			if got != tt.wantSQL {
				t.Errorf("processComparisonExpressionParam() = %q, want %q", got, tt.wantSQL)
			}
			assertQueryParams(t, pc.Params(), tt.wantParams)
		})
	}
}

func TestComparisonOperator_processMinMaxExpressionParam(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	op := NewComparisonOperator(config)

	tests := []struct {
		name       string
		operator   string
		args       interface{}
		wantSQL    string
		wantParams []params.QueryParam
		wantErr    bool
	}{
		{
			name:     "max",
			operator: "max",
			args:     []interface{}{5, 10},
			wantSQL:  "GREATEST(@p1, @p2)",
			wantParams: []params.QueryParam{
				{Name: "p1", Value: 5},
				{Name: "p2", Value: 10},
			},
		},
		{
			name:     "unsupported min/max operator",
			operator: "avg",
			args:     []interface{}{1, 2},
			wantErr:  true,
		},
		{
			name:     "non-array args",
			operator: "max",
			args:     "invalid",
			wantErr:  true,
		},
		{
			name:     "insufficient args",
			operator: "max",
			args:     []interface{}{5},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pc := params.NewParamCollector(params.PlaceholderNamed)
			got, err := op.processMinMaxExpressionParam(tt.operator, tt.args, pc)
			if tt.wantErr {
				if err == nil {
					t.Fatal("processMinMaxExpressionParam() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("processMinMaxExpressionParam() unexpected error = %v", err)
			}
			if got != tt.wantSQL {
				t.Errorf("processMinMaxExpressionParam() = %q, want %q", got, tt.wantSQL)
			}
			assertQueryParams(t, pc.Params(), tt.wantParams)
		})
	}
}

func TestComparisonOperator_ToSQLParam_WithSchemaCoercion(t *testing.T) {
	schema := newComparisonSchemaProvider(map[string]string{
		"age": "integer",
	})
	config := NewOperatorConfig(dialect.DialectBigQuery, schema)
	op := NewComparisonOperator(config)

	pc := params.NewParamCollector(params.PlaceholderNamed)
	got, err := op.ToSQLParam(">=", []interface{}{
		map[string]interface{}{"var": "age"},
		"50000",
	}, pc)
	if err != nil {
		t.Fatalf("ToSQLParam() unexpected error = %v", err)
	}
	if got != "age >= @p1" {
		t.Errorf("ToSQLParam() = %q, want %q", got, "age >= @p1")
	}
	assertQueryParams(t, pc.Params(), []params.QueryParam{
		{Name: "p1", Value: int64(50000)},
	})
}

func TestComparisonOperator_valueToSQLParam_ExpressionParserCallback(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	config.SetParamExpressionParser(func(expr any, path string, pc *params.ParamCollector) (string, error) {
		return "CUSTOM_PARAM()", nil
	})
	op := NewComparisonOperator(config)

	pc := params.NewParamCollector(params.PlaceholderNamed)
	result, err := op.valueToSQLParam(map[string]interface{}{"customOp": []interface{}{1, 2}}, pc)
	if err != nil {
		t.Fatalf("valueToSQLParam() unexpected error = %v", err)
	}
	if result != "CUSTOM_PARAM()" {
		t.Errorf("valueToSQLParam() = %q, want CUSTOM_PARAM()", result)
	}
	if len(pc.Params()) != 0 {
		t.Errorf("expected no params, got %#v", pc.Params())
	}
}
