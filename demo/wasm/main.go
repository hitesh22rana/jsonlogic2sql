//go:build js && wasm

package main

import (
	"encoding/json"
	"syscall/js"

	jsonlogic2sql "github.com/h22rana/jsonlogic2sql"
)

// transpilers holds active transpiler instances keyed by an ID.
var transpilers = map[int]*jsonlogic2sql.Transpiler{}
var nextID = 1

func dialetFromString(s string) (jsonlogic2sql.Dialect, bool) {
	switch s {
	case "bigquery":
		return jsonlogic2sql.DialectBigQuery, true
	case "spanner":
		return jsonlogic2sql.DialectSpanner, true
	case "postgresql":
		return jsonlogic2sql.DialectPostgreSQL, true
	case "duckdb":
		return jsonlogic2sql.DialectDuckDB, true
	case "clickhouse":
		return jsonlogic2sql.DialectClickHouse, true
	default:
		return 0, false
	}
}

// newTranspiler(dialect: string) => {id: number} | {error: string}
func newTranspiler(_ js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return map[string]interface{}{"error": "dialect argument required"}
	}
	dialectStr := args[0].String()
	dialect, ok := dialetFromString(dialectStr)
	if !ok {
		return map[string]interface{}{"error": "unsupported dialect: " + dialectStr}
	}
	t, err := jsonlogic2sql.NewTranspiler(dialect)
	if err != nil {
		return map[string]interface{}{"error": err.Error()}
	}
	id := nextID
	nextID++
	transpilers[id] = t
	return map[string]interface{}{"id": id}
}

// setSchema(id: number, schemaJSON: string) => {ok: true} | {error: string}
func setSchema(_ js.Value, args []js.Value) interface{} {
	if len(args) < 2 {
		return map[string]interface{}{"error": "id and schemaJSON arguments required"}
	}
	id := args[0].Int()
	schemaJSON := args[1].String()

	t, ok := transpilers[id]
	if !ok {
		return map[string]interface{}{"error": "transpiler not found"}
	}

	schema, err := jsonlogic2sql.NewSchemaFromJSON([]byte(schemaJSON))
	if err != nil {
		return map[string]interface{}{"error": "invalid schema: " + err.Error()}
	}
	t.SetSchema(schema)
	return map[string]interface{}{"ok": true}
}

// transpile(id: number, jsonLogic: string) => {sql: string} | {error: string}
func transpile(_ js.Value, args []js.Value) interface{} {
	if len(args) < 2 {
		return map[string]interface{}{"error": "id and jsonLogic arguments required"}
	}
	id := args[0].Int()
	jsonLogic := args[1].String()

	t, ok := transpilers[id]
	if !ok {
		return map[string]interface{}{"error": "transpiler not found"}
	}

	sql, err := t.Transpile(jsonLogic)
	if err != nil {
		errResult := map[string]interface{}{"error": err.Error()}
		if tErr, ok := jsonlogic2sql.AsTranspileError(err); ok {
			errResult["code"] = string(tErr.Code)
			errResult["operator"] = tErr.Operator
			errResult["path"] = tErr.Path
		}
		return errResult
	}
	return map[string]interface{}{"sql": sql}
}

// transpileCondition(id: number, jsonLogic: string) => {sql: string} | {error: string}
func transpileCondition(_ js.Value, args []js.Value) interface{} {
	if len(args) < 2 {
		return map[string]interface{}{"error": "id and jsonLogic arguments required"}
	}
	id := args[0].Int()
	jsonLogic := args[1].String()

	t, ok := transpilers[id]
	if !ok {
		return map[string]interface{}{"error": "transpiler not found"}
	}

	sql, err := t.TranspileCondition(jsonLogic)
	if err != nil {
		return map[string]interface{}{"error": err.Error()}
	}
	return map[string]interface{}{"sql": sql}
}

// quickTranspile(dialect: string, jsonLogic: string) => {sql: string} | {error: string}
// Convenience function that doesn't require creating a transpiler instance.
func quickTranspile(_ js.Value, args []js.Value) interface{} {
	if len(args) < 2 {
		return map[string]interface{}{"error": "dialect and jsonLogic arguments required"}
	}
	dialectStr := args[0].String()
	jsonLogic := args[1].String()

	dialect, ok := dialetFromString(dialectStr)
	if !ok {
		return map[string]interface{}{"error": "unsupported dialect: " + dialectStr}
	}

	sql, err := jsonlogic2sql.Transpile(dialect, jsonLogic)
	if err != nil {
		return map[string]interface{}{"error": err.Error()}
	}
	return map[string]interface{}{"sql": sql}
}

// getDialects() => string[] - returns list of supported dialects.
func getDialects(_ js.Value, _ []js.Value) interface{} {
	dialects := []interface{}{"bigquery", "spanner", "postgresql", "duckdb", "clickhouse"}
	return dialects
}

// destroyTranspiler(id: number) => {ok: true}
func destroyTranspiler(_ js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return map[string]interface{}{"error": "id argument required"}
	}
	id := args[0].Int()
	delete(transpilers, id)
	return map[string]interface{}{"ok": true}
}

// getSamples() => JSON string of sample expressions.
func getSamples(_ js.Value, _ []js.Value) interface{} {
	samples := []map[string]string{
		{"name": "Simple equality", "jsonLogic": `{"==": [{"var": "status"}, "active"]}`},
		{"name": "Greater than", "jsonLogic": `{">": [{"var": "amount"}, 1000]}`},
		{"name": "AND condition", "jsonLogic": `{"and": [{">": [{"var": "amount"}, 5000]}, {"==": [{"var": "status"}, "pending"]}]}`},
		{"name": "OR condition", "jsonLogic": `{"or": [{">=": [{"var": "failedAttempts"}, 5]}, {"in": [{"var": "country"}, ["CN", "RU"]]}]}`},
		{"name": "IN array", "jsonLogic": `{"in": [{"var": "country"}, ["US", "CA", "MX"]]}`},
		{"name": "NOT IN", "jsonLogic": `{"!": {"in": [{"var": "status"}, ["blocked", "suspended"]]}}`},
		{"name": "NULL check", "jsonLogic": `{"==": [{"var": "deleted_at"}, null]}`},
		{"name": "Chained comparison", "jsonLogic": `{"<": [18, {"var": "age"}, 65]}`},
		{"name": "Nested arithmetic", "jsonLogic": `{">": [{"+": [{"var": "base"}, {"*": [{"var": "bonus"}, 0.1]}]}, 1000]}`},
		{"name": "Conditional (if)", "jsonLogic": `{"if": [{">": [{"var": "age"}, 18]}, "adult", "minor"]}`},
	}
	data, _ := json.Marshal(samples)
	return string(data)
}

func main() {
	c := make(chan struct{})

	// Register all functions on the global jsonlogic2sql object
	jsObj := map[string]interface{}{
		"newTranspiler":      js.FuncOf(newTranspiler),
		"setSchema":          js.FuncOf(setSchema),
		"transpile":          js.FuncOf(transpile),
		"transpileCondition": js.FuncOf(transpileCondition),
		"quickTranspile":     js.FuncOf(quickTranspile),
		"destroyTranspiler":  js.FuncOf(destroyTranspiler),
		"getDialects":        js.FuncOf(getDialects),
		"getSamples":         js.FuncOf(getSamples),
	}

	js.Global().Set("jsonlogic2sql", js.ValueOf(jsObj))

	// Signal that WASM is ready
	js.Global().Call("eval", "if (typeof onJsonlogic2sqlReady === 'function') onJsonlogic2sqlReady();")

	<-c // Block forever
}
