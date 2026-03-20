package operators

import (
	"strings"
	"testing"

	"github.com/h22rana/jsonlogic2sql/internal/dialect"
)

// TestArrayOperator_ElementRefNoCorruption verifies that field names containing
// "item" or "current" as substrings are NOT corrupted by the element variable rewrite.
// This is a regression test for the replaceElementReference bug where
// strings.ReplaceAll corrupted "current_balance" → "elem_balance".
func TestArrayOperator_ElementRefNoCorruption(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	op := NewArrayOperator(config)

	tests := []struct {
		name     string
		operator string
		args     []any
		contains string
		absent   string
	}{
		{
			name:     "current_balance not corrupted in reduce",
			operator: "reduce",
			args: []any{
				map[string]any{"var": "orders"},
				map[string]any{"+": []any{map[string]any{"var": "accumulator"}, map[string]any{"var": "current_balance"}}},
				0,
			},
			contains: "current_balance",
			absent:   "elem_balance",
		},
		{
			name:     "item_count not corrupted in all",
			operator: "all",
			args: []any{
				map[string]any{"var": "scores"},
				map[string]any{">": []any{map[string]any{"var": "item_count"}, 0}},
			},
			contains: "item_count",
			absent:   "elem_count",
		},
		{
			name:     "items field not corrupted in filter",
			operator: "filter",
			args: []any{
				map[string]any{"var": "data"},
				map[string]any{">": []any{map[string]any{"var": "items"}, 0}},
			},
			contains: "items",
		},
		{
			name:     "currently field not corrupted in some",
			operator: "some",
			args: []any{
				map[string]any{"var": "records"},
				map[string]any{"==": []any{map[string]any{"var": "currently"}, true}},
			},
			contains: "currently",
		},
		{
			name:     "current.amount correctly mapped in reduce",
			operator: "reduce",
			args: []any{
				map[string]any{"var": "orders"},
				map[string]any{"+": []any{map[string]any{"var": "accumulator"}, map[string]any{"var": "current.amount"}}},
				0,
			},
			contains: "elem.amount",
			absent:   "current.amount",
		},
		{
			name:     "item.score correctly mapped in all",
			operator: "all",
			args: []any{
				map[string]any{"var": "results"},
				map[string]any{">=": []any{map[string]any{"var": "item.score"}, 50}},
			},
			contains: "elem.score",
			absent:   "item.score",
		},
		{
			name:     "item exact match is replaced in none",
			operator: "none",
			args: []any{
				map[string]any{"var": "values"},
				map[string]any{"==": []any{map[string]any{"var": "item"}, 0}},
			},
			contains: "elem = 0",
			absent:   "item = 0",
		},
		{
			name:     "current exact match is replaced in map",
			operator: "map",
			args: []any{
				map[string]any{"var": "values"},
				map[string]any{"*": []any{map[string]any{"var": "current"}, 2}},
			},
			contains: "(elem * 2)",
			absent:   "(current * 2)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.ToSQL(tt.operator, tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.contains != "" && !strings.Contains(result, tt.contains) {
				t.Errorf("expected SQL to contain %q, got: %s", tt.contains, result)
			}
			if tt.absent != "" && strings.Contains(result, tt.absent) {
				t.Errorf("expected SQL NOT to contain %q, got: %s", tt.absent, result)
			}
		})
	}
}

// TestArrayOperator_ArrayFormVarRewrite verifies that array-form var expressions
// like {"var": ["current", 0]} are correctly rewritten to {"var": ["elem", 0]}.
func TestArrayOperator_ArrayFormVarRewrite(t *testing.T) {
	config := NewOperatorConfig(dialect.DialectBigQuery, nil)
	op := NewArrayOperator(config)

	tests := []struct {
		name     string
		operator string
		args     []any
		contains string
		absent   string
	}{
		{
			name:     "array-form current with default in reduce",
			operator: "reduce",
			args: []any{
				map[string]any{"var": "items"},
				map[string]any{"+": []any{map[string]any{"var": "accumulator"}, map[string]any{"var": []any{"current", 0}}}},
				0,
			},
			contains: "COALESCE(elem, 0)",
			absent:   "COALESCE(current, 0)",
		},
		{
			name:     "array-form item with default in all",
			operator: "all",
			args: []any{
				map[string]any{"var": "scores"},
				map[string]any{">": []any{map[string]any{"var": []any{"item", 0}}, 50}},
			},
			contains: "COALESCE(elem, 0)",
			absent:   "COALESCE(item, 0)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := op.ToSQL(tt.operator, tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.contains != "" && !strings.Contains(result, tt.contains) {
				t.Errorf("expected SQL to contain %q, got: %s", tt.contains, result)
			}
			if tt.absent != "" && strings.Contains(result, tt.absent) {
				t.Errorf("expected SQL NOT to contain %q, got: %s", tt.absent, result)
			}
		})
	}
}
