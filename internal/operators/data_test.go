package operators

import (
	"testing"
)

func TestDataOperator_ToSQL(t *testing.T) {
	op := NewDataOperator(nil)

	tests := []struct {
		name     string
		operator string
		args     []interface{}
		expected string
		hasError bool
	}{
		// var operator tests
		{
			name:     "var with simple string",
			operator: "var",
			args:     []interface{}{"amount"},
			expected: "amount",
			hasError: false,
		},
		{
			name:     "var with dotted string",
			operator: "var",
			args:     []interface{}{"transaction.amount"},
			expected: "transaction.amount",
			hasError: false,
		},
		{
			name:     "var with nested dotted string",
			operator: "var",
			args:     []interface{}{"user.account.age"},
			expected: "user.account.age",
			hasError: false,
		},
		{
			name:     "var with array and default",
			operator: "var",
			args:     []interface{}{[]interface{}{"amount", 0}},
			expected: "COALESCE(amount, 0)",
			hasError: false,
		},
		{
			name:     "var with array and string default",
			operator: "var",
			args:     []interface{}{[]interface{}{"status", "pending"}},
			expected: "COALESCE(status, 'pending')",
			hasError: false,
		},
		{
			name:     "var with array and boolean default",
			operator: "var",
			args:     []interface{}{[]interface{}{"verified", true}},
			expected: "COALESCE(verified, TRUE)",
			hasError: false,
		},
		{
			name:     "var with array and null default",
			operator: "var",
			args:     []interface{}{[]interface{}{"field", nil}},
			expected: "COALESCE(field, NULL)",
			hasError: false,
		},
		{
			name:     "var with empty array",
			operator: "var",
			args:     []interface{}{[]interface{}{}},
			expected: "",
			hasError: true,
		},
		{
			name:     "var with non-string first arg",
			operator: "var",
			args:     []interface{}{[]interface{}{123, 0}},
			expected: "",
			hasError: true,
		},
		{
			name:     "var with no args",
			operator: "var",
			args:     []interface{}{},
			expected: "",
			hasError: true,
		},

		// missing operator tests
		{
			name:     "missing with simple string",
			operator: "missing",
			args:     []interface{}{"field"},
			expected: "field IS NULL",
			hasError: false,
		},
		{
			name:     "missing with dotted string",
			operator: "missing",
			args:     []interface{}{"user.name"},
			expected: "user.name IS NULL",
			hasError: false,
		},
		{
			name:     "missing with wrong arg count",
			operator: "missing",
			args:     []interface{}{"field", "extra"},
			expected: "",
			hasError: true,
		},
		{
			name:     "missing with non-string arg",
			operator: "missing",
			args:     []interface{}{123},
			expected: "",
			hasError: true,
		},
		{
			name:     "missing with array of fields",
			operator: "missing",
			args:     []interface{}{[]interface{}{"email", "phone", "address"}},
			expected: "(email IS NULL OR phone IS NULL OR address IS NULL)",
			hasError: false,
		},
		{
			name:     "missing with array of single field",
			operator: "missing",
			args:     []interface{}{[]interface{}{"email"}},
			expected: "(email IS NULL)",
			hasError: false,
		},
		{
			name:     "missing with empty array",
			operator: "missing",
			args:     []interface{}{[]interface{}{}},
			expected: "",
			hasError: true,
		},

		// missing_some operator tests
		{
			name:     "missing_some with single field",
			operator: "missing_some",
			args:     []interface{}{1, []interface{}{"field"}},
			expected: "(field IS NULL)",
			hasError: false,
		},
		{
			name:     "missing_some with multiple fields",
			operator: "missing_some",
			args:     []interface{}{2, []interface{}{"field1", "field2", "field3"}},
			expected: "(CASE WHEN field1 IS NULL THEN 1 ELSE 0 END + CASE WHEN field2 IS NULL THEN 1 ELSE 0 END + CASE WHEN field3 IS NULL THEN 1 ELSE 0 END) >= 2",
			hasError: false,
		},
		{
			name:     "missing_some with dotted fields",
			operator: "missing_some",
			args:     []interface{}{1, []interface{}{"user.name", "user.email"}},
			expected: "(user.name IS NULL OR user.email IS NULL)",
			hasError: false,
		},
		{
			name:     "missing_some with wrong arg count",
			operator: "missing_some",
			args:     []interface{}{1},
			expected: "",
			hasError: true,
		},
		{
			name:     "missing_some with non-number first arg",
			operator: "missing_some",
			args:     []interface{}{"1", []interface{}{"field"}},
			expected: "",
			hasError: true,
		},
		{
			name:     "missing_some with non-array second arg",
			operator: "missing_some",
			args:     []interface{}{1, "field"},
			expected: "",
			hasError: true,
		},
		{
			name:     "missing_some with empty array",
			operator: "missing_some",
			args:     []interface{}{1, []interface{}{}},
			expected: "",
			hasError: true,
		},
		{
			name:     "missing_some with non-string in array",
			operator: "missing_some",
			args:     []interface{}{1, []interface{}{"field", 123}},
			expected: "",
			hasError: true,
		},

		// unsupported operator
		{
			name:     "unsupported operator",
			operator: "unsupported",
			args:     []interface{}{"test"},
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

func TestDataOperator_convertVarName(t *testing.T) {
	op := NewDataOperator(nil)

	t.Run("valid identifiers", func(t *testing.T) {
		valid := []struct {
			input    string
			expected string
		}{
			{"amount", "amount"},
			{"transaction.amount", "transaction.amount"},
			{"user.account.age", "user.account.age"},
			{"a.b.c.d", "a.b.c.d"},
			{"simple", "simple"},
			{"_private", "_private"},
			{"field_name", "field_name"},
			{"Field123", "Field123"},
		}

		for _, tt := range valid {
			t.Run(tt.input, func(t *testing.T) {
				result, err := op.convertVarName(tt.input)
				if err != nil {
					t.Errorf("convertVarName(%q) unexpected error: %v", tt.input, err)
				}
				if result != tt.expected {
					t.Errorf("convertVarName(%q) = %q, expected %q", tt.input, result, tt.expected)
				}
			})
		}
	})

	t.Run("invalid identifiers rejected without schema", func(t *testing.T) {
		invalid := []string{
			"",
			"1; DROP TABLE users; --",
			"' OR 1=1 --",
			"field name",
			"field\ttab",
			"123starts_with_number",
			"field;name",
			"(expression)",
		}

		for _, input := range invalid {
			t.Run(input, func(t *testing.T) {
				_, err := op.convertVarName(input)
				if err == nil {
					t.Errorf("convertVarName(%q) expected error, got none", input)
				}
			})
		}
	})

	t.Run("schema bypasses identifier validation", func(t *testing.T) {
		schema := &dataSchemaProvider{}
		opWithSchema := NewDataOperator(NewOperatorConfig(0, schema))
		// With schema, even unusual names pass through (schema validates separately)
		result, err := opWithSchema.convertVarName("my field")
		if err != nil {
			t.Errorf("convertVarName with schema unexpected error: %v", err)
		}
		if result != "my field" {
			t.Errorf("convertVarName with schema = %q, expected %q", result, "my field")
		}
	})
}

// dataSchemaProvider is a minimal schema provider for data operator tests.
type dataSchemaProvider struct{}

func (m *dataSchemaProvider) HasField(_ string) bool              { return true }
func (m *dataSchemaProvider) GetFieldType(_ string) string        { return "string" }
func (m *dataSchemaProvider) ValidateField(_ string) error        { return nil }
func (m *dataSchemaProvider) IsArrayType(_ string) bool           { return false }
func (m *dataSchemaProvider) IsStringType(_ string) bool          { return true }
func (m *dataSchemaProvider) IsNumericType(_ string) bool         { return false }
func (m *dataSchemaProvider) IsBooleanType(_ string) bool         { return false }
func (m *dataSchemaProvider) IsEnumType(_ string) bool            { return false }
func (m *dataSchemaProvider) GetAllowedValues(_ string) []string  { return nil }
func (m *dataSchemaProvider) ValidateEnumValue(_, _ string) error { return nil }

func TestDataOperator_valueToSQL(t *testing.T) {
	op := NewDataOperator(nil)

	tests := []struct {
		name     string
		input    interface{}
		expected string
		hasError bool
	}{
		{"string", "hello", "'hello'", false},
		{"string with quotes", "he'llo", "'he''llo'", false},
		{"integer", 42, "42", false},
		{"float", 3.14, "3.14", false},
		{"boolean true", true, "TRUE", false},
		{"boolean false", false, "FALSE", false},
		{"null", nil, "NULL", false},
		{"int64", int64(123), "123", false},
		{"float32", float32(1.5), "1.5", false},
		{"unsupported type", []string{"a"}, "", true},
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
					t.Errorf("valueToSQL() = %s, expected %s", result, tt.expected)
				}
			}
		})
	}
}

func TestDataOperator_getNumber(t *testing.T) {
	op := NewDataOperator(nil)

	tests := []struct {
		name     string
		input    interface{}
		expected float64
		hasError bool
	}{
		{
			name:     "float64",
			input:    float64(3.14),
			expected: 3.14,
			hasError: false,
		},
		{
			name:     "float32",
			input:    float32(2.5),
			expected: float64(float32(2.5)),
			hasError: false,
		},
		{
			name:     "int",
			input:    42,
			expected: 42.0,
			hasError: false,
		},
		{
			name:     "int8",
			input:    int8(10),
			expected: 10.0,
			hasError: false,
		},
		{
			name:     "int16",
			input:    int16(100),
			expected: 100.0,
			hasError: false,
		},
		{
			name:     "int32",
			input:    int32(1000),
			expected: 1000.0,
			hasError: false,
		},
		{
			name:     "int64",
			input:    int64(9999),
			expected: 9999.0,
			hasError: false,
		},
		{
			name:     "uint",
			input:    uint(7),
			expected: 7.0,
			hasError: false,
		},
		{
			name:     "uint8",
			input:    uint8(8),
			expected: 8.0,
			hasError: false,
		},
		{
			name:     "uint16",
			input:    uint16(16),
			expected: 16.0,
			hasError: false,
		},
		{
			name:     "uint32",
			input:    uint32(32),
			expected: 32.0,
			hasError: false,
		},
		{
			name:     "uint64",
			input:    uint64(64),
			expected: 64.0,
			hasError: false,
		},
		{
			name:     "string - not a number",
			input:    "not a number",
			expected: 0,
			hasError: true,
		},
		{
			name:     "bool - not a number",
			input:    true,
			expected: 0,
			hasError: true,
		},
		{
			name:     "nil - not a number",
			input:    nil,
			expected: 0,
			hasError: true,
		},
		{
			name:     "slice - not a number",
			input:    []int{1, 2},
			expected: 0,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.getNumber(tt.input)
			if tt.hasError {
				if err == nil {
					t.Errorf("getNumber() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("getNumber() unexpected error = %v", err)
				}
				if result != tt.expected {
					t.Errorf("getNumber() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestDataOperator_valueToSQL_ProcessedValue(t *testing.T) {
	op := NewDataOperator(nil)

	tests := []struct {
		name     string
		input    interface{}
		expected string
		hasError bool
	}{
		{
			name:     "ProcessedValue with SQL",
			input:    ProcessedValue{Value: "COUNT(*)", IsSQL: true},
			expected: "COUNT(*)",
			hasError: false,
		},
		{
			name:     "ProcessedValue with literal string",
			input:    ProcessedValue{Value: "hello", IsSQL: false},
			expected: "'hello'",
			hasError: false,
		},
		{
			name:     "ProcessedValue with literal number string",
			input:    ProcessedValue{Value: "42", IsSQL: false},
			expected: "'42'",
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

func TestDataOperator_handleVar_EmptyVarName(t *testing.T) {
	op := NewDataOperator(nil)

	// Empty var name represents the current element in array operations
	result, err := op.ToSQL("var", []interface{}{""})
	if err != nil {
		t.Errorf("ToSQL() unexpected error = %v", err)
	}
	if result != ElemVar {
		t.Errorf("ToSQL() = %v, want %v", result, ElemVar)
	}
}

func TestDataOperator_handleVar_NonStringNonArrayArg(t *testing.T) {
	op := NewDataOperator(nil)

	// Non-string, non-array argument
	_, err := op.ToSQL("var", []interface{}{42})
	if err == nil {
		t.Errorf("ToSQL() expected error for non-string/non-array arg, got nil")
	}
}
