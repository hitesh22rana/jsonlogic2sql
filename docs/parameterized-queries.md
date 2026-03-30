# Parameterized Queries

jsonlogic2sql can generate SQL with bind parameter placeholders instead of inlined literals, returning the SQL string and a separate list of parameter values. This is the recommended approach for executing generated SQL against a database, as it prevents SQL injection and allows query plan caching.

## Quick Example

```go
transpiler, _ := jsonlogic2sql.NewTranspiler(jsonlogic2sql.DialectBigQuery)

sql, params, err := transpiler.TranspileParameterized(
    `{"and": [{"==": [{"var": "status"}, "active"]}, {">": [{"var": "amount"}, 1000]}]}`,
)
// sql    = "WHERE (status = @p1 AND amount > @p2)"
// params = [{Name: "p1", Value: "active"}, {Name: "p2", Value: 1000}]
```

## Placeholder Styles by Dialect

The placeholder style is determined by the dialect:

| Dialect | Style | Example |
|---------|-------|---------|
| BigQuery | Named | `@p1`, `@p2` |
| Spanner | Named | `@p1`, `@p2` |
| ClickHouse | Named | `@p1`, `@p2` |
| PostgreSQL | Positional | `$1`, `$2` |
| DuckDB | Positional | `$1`, `$2` |

### BigQuery

```go
sql, params, _ := jsonlogic2sql.TranspileParameterized(
    jsonlogic2sql.DialectBigQuery,
    `{"==": [{"var": "email"}, "alice@example.com"]}`,
)
// sql    = "WHERE email = @p1"
// params = [{Name: "p1", Value: "alice@example.com"}]
```

### PostgreSQL

```go
sql, params, _ := jsonlogic2sql.TranspileParameterized(
    jsonlogic2sql.DialectPostgreSQL,
    `{"==": [{"var": "email"}, "alice@example.com"]}`,
)
// sql    = "WHERE email = $1"
// params = [{Name: "p1", Value: "alice@example.com"}]
```

## What Gets Parameterized

| Value Type | Parameterized? | Output |
|------------|:--------------:|--------|
| Strings | Yes | `@p1` with bound value |
| Numbers (int, float) | Yes | `@p1` with bound value |
| `NULL` | No | `NULL` (structural SQL token) |
| `TRUE` / `FALSE` | No | `TRUE` / `FALSE` (structural SQL tokens) |
| Column names (`var`) | No | Column name as-is |

NULL and boolean values remain inline because they are structural SQL tokens used in `IS NULL`, `IS TRUE` patterns, not user-supplied data.

## LIKE Patterns with Parameters

When building `LIKE` patterns in parameterized mode, placeholders must stay as SQL expressions, not string literals.

Correct pattern construction:

```sql
WHERE name LIKE CONCAT(@p1, '%')
```

Incorrect pattern construction:

```sql
WHERE name LIKE '@p1%'
```

Why this matters:
- `CONCAT(@p1, '%')` keeps `@p1` as a bind parameter and appends `%` safely in SQL.
- `'@p1%'` turns the placeholder into a plain string literal, so no binding occurs for that pattern value.

The same rule applies to positional styles:
- PostgreSQL/DuckDB: `LIKE CONCAT($1, '%')`
- Prefix/suffix patterns: `LIKE CONCAT('%', @p1, '%')`, `LIKE CONCAT('%', $1)`

## API Reference

### Transpiler Methods

| Method | Returns WHERE? | Description |
|--------|:--------------:|-------------|
| `TranspileParameterized(jsonLogic string)` | Yes | From JSON string |
| `TranspileParameterizedFromMap(logic map[string]interface{})` | Yes | From pre-parsed map |
| `TranspileParameterizedFromInterface(logic interface{})` | Yes | From any interface |
| `TranspileConditionParameterized(jsonLogic string)` | No | Condition only (no WHERE) |
| `TranspileConditionParameterizedFromMap(logic map[string]interface{})` | No | Condition only from map |
| `TranspileConditionParameterizedFromInterface(logic interface{})` | No | Condition only from interface |

All methods return `(string, []QueryParam, error)`.

### Package-Level Convenience Functions

Each Transpiler method has a corresponding package-level function that takes a `Dialect` as the first argument:

```go
sql, params, err := jsonlogic2sql.TranspileParameterized(dialect, jsonLogic)
sql, params, err := jsonlogic2sql.TranspileConditionParameterized(dialect, jsonLogic)
// ... and 4 more variants (FromMap, FromInterface)
```

### QueryParam Type

```go
type QueryParam struct {
    Name  string      // "p1", "p2", etc.
    Value interface{} // Go native type (string, float64, int64, bool)
}
```

**Value types by input:**

| JSONLogic Input | `Value` Go Type | Notes |
|-----------------|-----------------|-------|
| `"hello"` | `string` | |
| `42` | `float64` | JSON numbers are float64 in Go |
| Coerced integer (schema) | `int64` | Schema coerces `"50000"` → `int64(50000)` for integer fields |
| `true` / `false` | — | Not parameterized (inline `TRUE`/`FALSE`) |
| `null` | — | Not parameterized (inline `NULL`) |
| Integer string `> int64` range | `string` | e.g., `"9223372036854775808"` is stored as string for `database/sql` driver compatibility; callers may need to convert to their driver's numeric type |

## Schema Coercion

When a schema is configured, values are coerced **before** being bound as parameters. For example, if a field is declared as `integer` and the JSONLogic contains a string `"50000"`, the bound parameter value will be `int64(50000)`, not the string `"50000"`.

```go
schema := jsonlogic2sql.NewSchema([]jsonlogic2sql.FieldSchema{
    {Name: "amount", Type: jsonlogic2sql.FieldTypeInteger},
})
transpiler.SetSchema(schema)

sql, params, _ := transpiler.TranspileParameterized(
    `{">=": [{"var": "amount"}, "50000"]}`,
)
// sql    = "WHERE amount >= @p1"
// params = [{Name: "p1", Value: int64(50000)}]  // coerced from string
```

## Using Parameters with Database Drivers

### BigQuery (Go)

```go
sql, params, _ := transpiler.TranspileParameterized(jsonLogic)

query := client.Query(sql)
for _, p := range params {
    query.Parameters = append(query.Parameters, bigquery.QueryParameter{
        Name:  p.Name,
        Value: p.Value,
    })
}
```

### PostgreSQL (Go - pgx)

```go
sql, params, _ := transpiler.TranspileParameterized(jsonLogic)

args := make([]interface{}, len(params))
for i, p := range params {
    args[i] = p.Value
}
rows, err := conn.Query(ctx, sql, args...)
```

### ClickHouse (Go - clickhouse-go v2)

The ClickHouse driver uses `{name:Type}` natively, not `@p1`. You can adapt the named parameters:

```go
sql, params, _ := transpiler.TranspileParameterized(jsonLogic)

// Convert @p1 → {p1:String}, @p2 → {p2:Int64}, etc.
chSQL := sql
chParams := make(clickhouse.Named, len(params))
for _, p := range params {
    placeholder := "@" + p.Name
    switch p.Value.(type) {
    case string:
        chSQL = strings.Replace(chSQL, placeholder, fmt.Sprintf("{%s:String}", p.Name), 1)
    case float64:
        chSQL = strings.Replace(chSQL, placeholder, fmt.Sprintf("{%s:Float64}", p.Name), 1)
    case int64:
        chSQL = strings.Replace(chSQL, placeholder, fmt.Sprintf("{%s:Int64}", p.Name), 1)
    }
    chParams = append(chParams, clickhouse.Named(p.Name, p.Value))
}
```

## Custom Operators

Custom operators receive SQL fragment arguments that may contain placeholders. The contract is the same as with inline mode: custom operators **must** include all provided arguments in their output SQL. Dropping an argument is a semantic bug that, in parameterized mode, additionally triggers an `E350 ErrUnreferencedPlaceholder` error.

```go
// Good: all args used
transpiler.RegisterOperatorFunc("double", func(op string, args []interface{}) (string, error) {
    return fmt.Sprintf("(%s * 2)", args[0]), nil // @p1 flows through
})

// Bad: dropping args causes E350
transpiler.RegisterOperatorFunc("broken", func(op string, args []interface{}) (string, error) {
    return "42", nil // discards args containing @p1 → E350 error
})
```

## Error Handling

The parameterized pipeline returns identical error types and codes as the inline pipeline. One additional error code is specific to parameterized mode:

| Code | Constant | Description |
|------|----------|-------------|
| E350 | `ErrUnreferencedPlaceholder` | A collected bind parameter has no matching placeholder in the generated SQL |

This typically occurs when a custom operator drops an argument. See [Error Handling](error-handling.md) for all error codes.

## Comparison: Inline vs Parameterized

| Input | Inline (`Transpile`) | Parameterized (`TranspileParameterized`) |
|-------|---------------------|------------------------------------------|
| `{"==": [{"var": "email"}, "alice"]}` | `WHERE email = 'alice'` | `WHERE email = @p1` + `[{p1, "alice"}]` |
| `{"in": [{"var": "x"}, [1, 2]]}` | `WHERE x IN (1, 2)` | `WHERE x IN (@p1, @p2)` + `[{p1, 1}, {p2, 2}]` |
| `{"==": [{"var": "f"}, null]}` | `WHERE f IS NULL` | `WHERE f IS NULL` (no params) |
| `{"==": [{"var": "f"}, true]}` | `WHERE f = TRUE` | `WHERE f = TRUE` (no params) |

## See Also

- [Getting Started](getting-started.md) - Basic usage
- [API Reference](api-reference.md) - Full API documentation
- [Error Handling](error-handling.md) - Error codes and handling
- [Custom Operators](custom-operators.md) - Custom operator registration
