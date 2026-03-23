package operators

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/h22rana/jsonlogic2sql/internal/dialect"
)

// Patterns for replacing element references in pre-processed SQL strings.
// These match "item"/"current"/"accumulator" only when they appear as standalone
// identifiers (not as suffixes like "account.current" or substrings like "current_balance").
// The pattern requires either start-of-string or a non-word non-dot character before the
// keyword, and a word boundary after it. This prevents false positives on:
//   - "current_balance" (underscore is a word char, no \b before "current")
//   - "account.current" (dot before "current" is blocked by [^\w.])
//
// But correctly matches:
//   - standalone "current" → "elem"
//   - "current.field" → "elem.field" (dot AFTER is fine, matched by \b)
//   - "(current + 1)" → "(elem + 1)" (paren is non-word non-dot)
var (
	itemPattern        = regexp.MustCompile(`(^|[^\w.])` + regexp.QuoteMeta(ItemVar) + `\b`)
	currentPattern     = regexp.MustCompile(`(^|[^\w.])` + regexp.QuoteMeta(CurrentVar) + `\b`)
	accumulatorPattern = regexp.MustCompile(`(^|[^\w.])` + regexp.QuoteMeta(AccumulatorVar) + `\b`)
)

// ArrayOperator handles array operations like map, filter, reduce, all, some, none, merge.
type ArrayOperator struct {
	config       *OperatorConfig
	dataOp       *DataOperator
	comparisonOp *ComparisonOperator
	logicalOp    *LogicalOperator
	numericOp    *NumericOperator
}

// NewArrayOperator creates a new ArrayOperator instance with optional config.
func NewArrayOperator(config *OperatorConfig) *ArrayOperator {
	return &ArrayOperator{
		config:       config,
		dataOp:       NewDataOperator(config),
		comparisonOp: NewComparisonOperator(config),
		logicalOp:    nil, // Will be created lazily
		numericOp:    NewNumericOperator(config),
	}
}

// schema returns the schema from config, or nil if not configured.
func (a *ArrayOperator) schema() SchemaProvider {
	if a.config == nil {
		return nil
	}
	return a.config.Schema
}

// getDialect returns the configured dialect, or DialectUnspecified if not configured.
func (a *ArrayOperator) getDialect() dialect.Dialect {
	if a.config == nil {
		return dialect.DialectUnspecified
	}
	return a.config.GetDialect()
}

// getLogicalOperator returns the logical operator, creating it lazily if needed.
func (a *ArrayOperator) getLogicalOperator() *LogicalOperator {
	if a.logicalOp == nil {
		a.logicalOp = NewLogicalOperator(a.config) // Config already has schema
	}
	return a.logicalOp
}

// validateArrayOperand checks if a field used in an array operation is of array type.
func (a *ArrayOperator) validateArrayOperand(value interface{}) error {
	if a.schema() == nil {
		return nil // No schema, no validation
	}

	// If it's a literal array, it's valid
	if _, ok := value.([]interface{}); ok {
		return nil
	}

	fieldName := a.extractFieldNameFromValue(value)
	if fieldName == "" {
		return nil // Can't determine field name, skip validation
	}

	fieldType := a.schema().GetFieldType(fieldName)
	if fieldType == "" {
		return nil // Field not in schema, skip validation (existence checked by DataOperator)
	}

	if !a.schema().IsArrayType(fieldName) {
		return fmt.Errorf("array operation on non-array field '%s' (type: %s)", fieldName, fieldType)
	}

	return nil
}

// extractFieldNameFromValue extracts field name from a value that might be a var expression.
func (a *ArrayOperator) extractFieldNameFromValue(value interface{}) string {
	if varExpr, ok := value.(map[string]interface{}); ok {
		if varName, hasVar := varExpr[OpVar]; hasVar {
			return a.extractFieldName(varName)
		}
	}
	return ""
}

// extractFieldName extracts the field name from a var argument.
func (a *ArrayOperator) extractFieldName(varName interface{}) string {
	if nameStr, ok := varName.(string); ok {
		return nameStr
	}
	if nameArr, ok := varName.([]interface{}); ok && len(nameArr) > 0 {
		if nameStr, ok := nameArr[0].(string); ok {
			return nameStr
		}
	}
	return ""
}

// ToSQL converts an array operation to SQL.
func (a *ArrayOperator) ToSQL(operator string, args []interface{}) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("array operator %s requires at least one argument", operator)
	}

	switch operator {
	case "map":
		return a.handleMap(args)
	case "filter":
		return a.handleFilter(args)
	case "reduce":
		return a.handleReduce(args)
	case "all":
		return a.handleAll(args)
	case "some":
		return a.handleSome(args)
	case "none":
		return a.handleNone(args)
	case "merge":
		return a.handleMerge(args)
	default:
		return "", fmt.Errorf("unsupported array operator: %s", operator)
	}
}

// handleMap converts map operator to SQL.
// Generates: ARRAY(SELECT transformation FROM UNNEST(array) AS elem).
// For ClickHouse: Uses arrayMap or subquery with arrayJoin.
func (a *ArrayOperator) handleMap(args []interface{}) (string, error) {
	if len(args) != 2 {
		return "", fmt.Errorf("map requires exactly 2 arguments")
	}

	// Validate dialect support
	if a.config != nil {
		if err := a.config.ValidateDialect("map"); err != nil {
			return "", err
		}
	}

	// Validate that first argument is an array type
	if err := a.validateArrayOperand(args[0]); err != nil {
		return "", err
	}

	// First argument: array
	array, err := a.valueToSQL(args[0])
	if err != nil {
		return "", fmt.Errorf("invalid map array argument: %w", err)
	}

	// Second argument: transformation expression - rewrite element vars before SQL generation
	rewritten := a.rewriteElementVars(args[1])
	transformation, err := a.expressionToSQL(rewritten)
	if err != nil {
		return "", fmt.Errorf("invalid map transformation argument: %w", err)
	}
	transformation = a.replaceElementRefsInSQL(transformation)

	// Generate SQL based on dialect
	switch a.getDialect() {
	case dialect.DialectClickHouse:
		return fmt.Sprintf("arrayMap(elem -> %s, %s)", transformation, array), nil
	case dialect.DialectUnspecified, dialect.DialectBigQuery, dialect.DialectSpanner, dialect.DialectPostgreSQL, dialect.DialectDuckDB:
		return fmt.Sprintf("ARRAY(SELECT %s FROM UNNEST(%s) AS elem)", transformation, array), nil
	}
	return fmt.Sprintf("ARRAY(SELECT %s FROM UNNEST(%s) AS elem)", transformation, array), nil
}

// handleFilter converts filter operator to SQL.
// Generates: ARRAY(SELECT elem FROM UNNEST(array) AS elem WHERE condition).
// For ClickHouse: Uses arrayFilter function.
func (a *ArrayOperator) handleFilter(args []interface{}) (string, error) {
	if len(args) != 2 {
		return "", fmt.Errorf("filter requires exactly 2 arguments")
	}

	// Validate dialect support
	if a.config != nil {
		if err := a.config.ValidateDialect("filter"); err != nil {
			return "", err
		}
	}

	// Validate that first argument is an array type
	if err := a.validateArrayOperand(args[0]); err != nil {
		return "", err
	}

	// First argument: array
	array, err := a.valueToSQL(args[0])
	if err != nil {
		return "", fmt.Errorf("invalid filter array argument: %w", err)
	}

	// Second argument: condition expression - rewrite element vars before SQL generation
	rewritten := a.rewriteElementVars(args[1])
	condition, err := a.expressionToSQL(rewritten)
	if err != nil {
		return "", fmt.Errorf("invalid filter condition argument: %w", err)
	}
	condition = a.replaceElementRefsInSQL(condition)

	// Generate SQL based on dialect
	switch a.getDialect() {
	case dialect.DialectClickHouse:
		return fmt.Sprintf("arrayFilter(elem -> %s, %s)", condition, array), nil
	case dialect.DialectUnspecified, dialect.DialectBigQuery, dialect.DialectSpanner, dialect.DialectPostgreSQL, dialect.DialectDuckDB:
		return fmt.Sprintf("ARRAY(SELECT elem FROM UNNEST(%s) AS elem WHERE %s)", array, condition), nil
	}
	return fmt.Sprintf("ARRAY(SELECT elem FROM UNNEST(%s) AS elem WHERE %s)", array, condition), nil
}

// handleReduce converts reduce operator to SQL.
// JSONLogic reduce: {"reduce": [array, reducer_expr, initial]}.
// The reducer expression uses "accumulator" and "current" variables.
//
// For common patterns, this generates optimized SQL:
// - Addition: initial + COALESCE((SELECT SUM(elem) FROM UNNEST(array) AS elem), 0).
// - General: (SELECT reducer FROM UNNEST(array) AS elem).
// For ClickHouse: Uses arrayReduce function for aggregates.
func (a *ArrayOperator) handleReduce(args []interface{}) (string, error) {
	if len(args) != 3 {
		return "", fmt.Errorf("reduce requires exactly 3 arguments")
	}

	// Validate dialect support
	if a.config != nil {
		if err := a.config.ValidateDialect("reduce"); err != nil {
			return "", err
		}
	}

	// Validate that first argument is an array type
	if err := a.validateArrayOperand(args[0]); err != nil {
		return "", err
	}

	// First argument: array
	array, err := a.valueToSQL(args[0])
	if err != nil {
		return "", fmt.Errorf("invalid reduce array argument: %w", err)
	}

	// Second argument: reducer expression
	reducerExpr := args[1]

	// Third argument: initial value
	initial, err := a.valueToSQL(args[2])
	if err != nil {
		return "", fmt.Errorf("invalid reduce initial argument: %w", err)
	}

	// Check for common reduction patterns and optimize
	if pattern := a.detectAggregatePattern(reducerExpr); pattern != nil {
		// Build the element reference: "elem" or "elem.field" if field suffix exists
		elemRef := ElemVar
		if pattern.fieldSuffix != "" {
			elemRef = ElemVar + "." + pattern.fieldSuffix
		}

		// Generate optimized aggregate SQL based on dialect
		switch a.getDialect() {
		case dialect.DialectClickHouse:
			// ClickHouse: For field access, we need arrayMap first to extract the field
			if pattern.fieldSuffix != "" {
				// initial + coalesce(arrayReduce('sum', arrayMap(x -> x.field, array)), 0)
				return fmt.Sprintf("%s + coalesce(arrayReduce('%s', arrayMap(x -> x.%s, %s)), 0)",
					initial, strings.ToLower(pattern.function), pattern.fieldSuffix, array), nil
			}
			// initial + coalesce(arrayReduce('sum', array), 0)
			return fmt.Sprintf("%s + coalesce(arrayReduce('%s', %s), 0)",
				initial, strings.ToLower(pattern.function), array), nil
		case dialect.DialectUnspecified, dialect.DialectBigQuery, dialect.DialectSpanner, dialect.DialectPostgreSQL, dialect.DialectDuckDB:
			// Standard SQL: initial + COALESCE((SELECT AGG(elem.field) FROM UNNEST(array) AS elem), 0)
			return fmt.Sprintf("%s + COALESCE((SELECT %s(%s) FROM UNNEST(%s) AS elem), 0)",
				initial, pattern.function, elemRef, array), nil
		}
		// Fallback for any future dialects
		return fmt.Sprintf("%s + COALESCE((SELECT %s(%s) FROM UNNEST(%s) AS elem), 0)",
			initial, pattern.function, elemRef, array), nil
	}

	// General case: rewrite element vars in the AST (item/current → elem),
	// then generate SQL, apply safety net for custom ops, and finally
	// substitute accumulator with the initial value. The order matters:
	// accumulator substitution must happen LAST so that the safety net
	// doesn't corrupt initial values containing "current"/"item" field names.
	rewritten := a.rewriteElementVars(reducerExpr)
	reducerWithElem, err := a.expressionToSQL(rewritten)
	if err != nil {
		return "", fmt.Errorf("invalid reduce expression: %w", err)
	}
	reducerWithElem = a.replaceElementRefsInSQL(reducerWithElem)
	reducerWithElem = replaceWithLiteral(accumulatorPattern, reducerWithElem, initial)

	// Generate SQL based on dialect
	switch a.getDialect() {
	case dialect.DialectClickHouse:
		// ClickHouse uses arrayFold for general reduction (ClickHouse 22.8+)
		return fmt.Sprintf("arrayFold((acc, elem) -> %s, %s, %s)", reducerWithElem, array, initial), nil
	case dialect.DialectUnspecified, dialect.DialectBigQuery, dialect.DialectSpanner, dialect.DialectPostgreSQL, dialect.DialectDuckDB:
		// Standard SQL using a subquery
		return fmt.Sprintf("(SELECT %s FROM UNNEST(%s) AS elem)", reducerWithElem, array), nil
	}
	return fmt.Sprintf("(SELECT %s FROM UNNEST(%s) AS elem)", reducerWithElem, array), nil
}

// aggregatePattern represents a detected aggregate pattern with optional field suffix.
type aggregatePattern struct {
	function    string // SQL aggregate function name (SUM, MIN, MAX)
	fieldSuffix string // Optional field suffix (e.g., "price" for "current.price")
}

// detectAggregatePattern checks if the reducer expression matches a common aggregate pattern.
// Returns the aggregate pattern if detected, nil otherwise.
func (a *ArrayOperator) detectAggregatePattern(expr interface{}) *aggregatePattern {
	exprMap, ok := expr.(map[string]interface{})
	if !ok {
		return nil
	}

	// Check for addition pattern: {"+": [{"var": "accumulator"}, {"var": "current"}]}
	// or {"+": [{"var": "accumulator"}, {"var": "current.price"}]}
	if args, hasPlus := exprMap[OpAdd]; hasPlus {
		if fieldSuffix, ok := a.isAccumulatorCurrentPattern(args); ok {
			return &aggregatePattern{function: AggregateSUM, fieldSuffix: fieldSuffix}
		}
	}

	// Check for min pattern: {"min": [{"var": "accumulator"}, {"var": "current"}]}
	// or {"min": [{"var": "accumulator"}, {"var": "current.price"}]}
	if args, hasMin := exprMap[OpMin]; hasMin {
		if fieldSuffix, ok := a.isAccumulatorCurrentPattern(args); ok {
			return &aggregatePattern{function: AggregateMIN, fieldSuffix: fieldSuffix}
		}
	}

	// Check for max pattern: {"max": [{"var": "accumulator"}, {"var": "current"}]}
	// or {"max": [{"var": "accumulator"}, {"var": "current.price"}]}
	if args, hasMax := exprMap[OpMax]; hasMax {
		if fieldSuffix, ok := a.isAccumulatorCurrentPattern(args); ok {
			return &aggregatePattern{function: AggregateMAX, fieldSuffix: fieldSuffix}
		}
	}

	return nil
}

// isAccumulatorCurrentPattern checks if args match [{"var": "accumulator"}, {"var": "current"}]
// or [{"var": "accumulator"}, {"var": "current.field"}].
// Returns (fieldSuffix, true) if pattern matches, ("", false) otherwise.
// fieldSuffix is empty for plain "current", or contains the field path (e.g., "price" for "current.price").
func (a *ArrayOperator) isAccumulatorCurrentPattern(args interface{}) (string, bool) {
	argsArr, ok := args.([]interface{})
	if !ok || len(argsArr) != 2 {
		return "", false
	}

	// Check first arg is {"var": "accumulator"}
	arg0Map, ok := argsArr[0].(map[string]interface{})
	if !ok {
		return "", false
	}
	if varName, hasVar := arg0Map[OpVar]; !hasVar || varName != AccumulatorVar {
		return "", false
	}

	// Check second arg is {"var": "current"} or {"var": "current.field"}
	arg1Map, ok := argsArr[1].(map[string]interface{})
	if !ok {
		return "", false
	}
	varName, hasVar := arg1Map[OpVar]
	if !hasVar {
		return "", false
	}

	varNameStr, ok := varName.(string)
	if !ok {
		return "", false
	}

	// Check if it's exactly "current" or starts with "current."
	if varNameStr == CurrentVar {
		return "", true // Plain current, no field suffix
	}
	if strings.HasPrefix(varNameStr, CurrentVar+".") {
		// Extract field suffix (e.g., "price" from "current.price")
		fieldSuffix := strings.TrimPrefix(varNameStr, CurrentVar+".")
		return fieldSuffix, true
	}

	return "", false
}

// handleAll converts all operator to SQL.
// This checks if all elements in an array satisfy a condition.
// Generates: NOT EXISTS (SELECT 1 FROM UNNEST(array) AS elem WHERE NOT (condition)).
// For ClickHouse: Uses arrayAll function.
func (a *ArrayOperator) handleAll(args []interface{}) (string, error) {
	if len(args) != 2 {
		return "", fmt.Errorf("all requires exactly 2 arguments")
	}

	// Validate dialect support
	if a.config != nil {
		if err := a.config.ValidateDialect("all"); err != nil {
			return "", err
		}
	}

	// Validate that first argument is an array type
	if err := a.validateArrayOperand(args[0]); err != nil {
		return "", err
	}

	// First argument: array
	array, err := a.valueToSQL(args[0])
	if err != nil {
		return "", fmt.Errorf("invalid all array argument: %w", err)
	}

	// Second argument: condition expression - rewrite element vars before SQL generation
	rewritten := a.rewriteElementVars(args[1])
	condition, err := a.expressionToSQL(rewritten)
	if err != nil {
		return "", fmt.Errorf("invalid all condition argument: %w", err)
	}
	condition = a.replaceElementRefsInSQL(condition)

	// Generate SQL based on dialect
	// JSONLogic spec: {"all": [[], condition]} returns false (empty array = false).
	// Without a guard, SQL NOT EXISTS on an empty UNNEST returns true (no rows to violate).
	// We add an emptiness check: array must be non-null and non-empty.
	lengthCheck := a.config.ArrayLengthFunc(array)
	switch a.getDialect() {
	case dialect.DialectClickHouse:
		return fmt.Sprintf("(%s > 0 AND arrayAll(elem -> %s, %s))", lengthCheck, condition, array), nil
	case dialect.DialectUnspecified, dialect.DialectBigQuery, dialect.DialectSpanner, dialect.DialectPostgreSQL, dialect.DialectDuckDB:
		return fmt.Sprintf("(%s > 0 AND NOT EXISTS (SELECT 1 FROM UNNEST(%s) AS elem WHERE NOT (%s)))", lengthCheck, array, condition), nil
	}
	return fmt.Sprintf("(%s > 0 AND NOT EXISTS (SELECT 1 FROM UNNEST(%s) AS elem WHERE NOT (%s)))", lengthCheck, array, condition), nil
}

// handleSome converts some operator to SQL.
// This checks if some elements in an array satisfy a condition.
// Generates: EXISTS (SELECT 1 FROM UNNEST(array) AS elem WHERE condition).
// For ClickHouse: Uses arrayExists function.
func (a *ArrayOperator) handleSome(args []interface{}) (string, error) {
	if len(args) != 2 {
		return "", fmt.Errorf("some requires exactly 2 arguments")
	}

	// Validate dialect support
	if a.config != nil {
		if err := a.config.ValidateDialect("some"); err != nil {
			return "", err
		}
	}

	// Validate that first argument is an array type
	if err := a.validateArrayOperand(args[0]); err != nil {
		return "", err
	}

	// First argument: array
	array, err := a.valueToSQL(args[0])
	if err != nil {
		return "", fmt.Errorf("invalid some array argument: %w", err)
	}

	// Second argument: condition expression - rewrite element vars before SQL generation
	rewritten := a.rewriteElementVars(args[1])
	condition, err := a.expressionToSQL(rewritten)
	if err != nil {
		return "", fmt.Errorf("invalid some condition argument: %w", err)
	}
	condition = a.replaceElementRefsInSQL(condition)

	// Generate SQL based on dialect
	switch a.getDialect() {
	case dialect.DialectClickHouse:
		return fmt.Sprintf("arrayExists(elem -> %s, %s)", condition, array), nil
	case dialect.DialectUnspecified, dialect.DialectBigQuery, dialect.DialectSpanner, dialect.DialectPostgreSQL, dialect.DialectDuckDB:
		// Standard SQL: EXISTS (SELECT 1 FROM UNNEST(array) AS elem WHERE condition)
		return fmt.Sprintf("EXISTS (SELECT 1 FROM UNNEST(%s) AS elem WHERE %s)", array, condition), nil
	}
	return fmt.Sprintf("EXISTS (SELECT 1 FROM UNNEST(%s) AS elem WHERE %s)", array, condition), nil
}

// handleNone converts none operator to SQL.
// This checks if no elements in an array satisfy a condition.
// Generates: NOT EXISTS (SELECT 1 FROM UNNEST(array) AS elem WHERE condition).
// For ClickHouse: Uses NOT arrayExists function.
func (a *ArrayOperator) handleNone(args []interface{}) (string, error) {
	if len(args) != 2 {
		return "", fmt.Errorf("none requires exactly 2 arguments")
	}

	// Validate dialect support
	if a.config != nil {
		if err := a.config.ValidateDialect("none"); err != nil {
			return "", err
		}
	}

	// Validate that first argument is an array type
	if err := a.validateArrayOperand(args[0]); err != nil {
		return "", err
	}

	// First argument: array
	array, err := a.valueToSQL(args[0])
	if err != nil {
		return "", fmt.Errorf("invalid none array argument: %w", err)
	}

	// Second argument: condition expression - rewrite element vars before SQL generation
	rewritten := a.rewriteElementVars(args[1])
	condition, err := a.expressionToSQL(rewritten)
	if err != nil {
		return "", fmt.Errorf("invalid none condition argument: %w", err)
	}
	condition = a.replaceElementRefsInSQL(condition)

	// Generate SQL based on dialect
	switch a.getDialect() {
	case dialect.DialectClickHouse:
		return fmt.Sprintf("NOT arrayExists(elem -> %s, %s)", condition, array), nil
	case dialect.DialectUnspecified, dialect.DialectBigQuery, dialect.DialectSpanner, dialect.DialectPostgreSQL, dialect.DialectDuckDB:
		// Standard SQL: NOT EXISTS (SELECT 1 FROM UNNEST(array) AS elem WHERE condition)
		return fmt.Sprintf("NOT EXISTS (SELECT 1 FROM UNNEST(%s) AS elem WHERE %s)", array, condition), nil
	}
	return fmt.Sprintf("NOT EXISTS (SELECT 1 FROM UNNEST(%s) AS elem WHERE %s)", array, condition), nil
}

// handleMerge converts merge operator to SQL.
// This merges multiple arrays into one.
// BigQuery/Spanner: ARRAY_CONCAT(array1, array2, ...)
// PostgreSQL: array1 || array2 || ...
func (a *ArrayOperator) handleMerge(args []interface{}) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("merge requires at least 1 argument")
	}

	// Validate dialect support
	if a.config != nil {
		if err := a.config.ValidateDialect("merge"); err != nil {
			return "", err
		}
	}

	// Validate that all arguments are array types
	for _, arg := range args {
		if err := a.validateArrayOperand(arg); err != nil {
			return "", err
		}
	}

	// Convert all array arguments to SQL
	arrays := make([]string, len(args))
	for i, arg := range args {
		array, err := a.valueToSQL(arg)
		if err != nil {
			return "", fmt.Errorf("invalid merge array argument %d: %w", i, err)
		}
		arrays[i] = array
	}

	// Generate SQL based on dialect
	d := dialect.DialectUnspecified
	if a.config != nil {
		d = a.config.GetDialect()
	}

	switch d {
	case dialect.DialectPostgreSQL:
		// PostgreSQL: Use || operator for array concatenation
		if len(arrays) == 1 {
			return arrays[0], nil
		}
		return fmt.Sprintf("(%s)", strings.Join(arrays, " || ")), nil
	case dialect.DialectBigQuery, dialect.DialectSpanner, dialect.DialectDuckDB:
		// BigQuery/Spanner/DuckDB: Use ARRAY_CONCAT function
		return fmt.Sprintf("ARRAY_CONCAT(%s)", strings.Join(arrays, ", ")), nil
	case dialect.DialectClickHouse:
		// ClickHouse: Use arrayConcat function
		return fmt.Sprintf("arrayConcat(%s)", strings.Join(arrays, ", ")), nil
	case dialect.DialectUnspecified:
		return "", fmt.Errorf("merge: dialect not specified")
	default:
		return "", fmt.Errorf("merge: unsupported dialect %s", d)
	}
}

// valueToSQL converts a value to SQL, handling var expressions, arrays, and literals.
func (a *ArrayOperator) valueToSQL(value interface{}) (string, error) {
	// Handle ProcessedValue (pre-processed SQL from parser)
	if pv, ok := value.(ProcessedValue); ok {
		if pv.IsSQL {
			return pv.Value, nil
		}
		// It's a literal, convert it
		return a.dataOp.valueToSQL(pv.Value)
	}

	// Handle complex expressions (operators)
	if expr, ok := value.(map[string]interface{}); ok {
		// Check if it's a var expression
		if varExpr, hasVar := expr[OpVar]; hasVar {
			return a.dataOp.ToSQL(OpVar, []interface{}{varExpr})
		}
		// Otherwise, it's a complex expression - convert it using expressionToSQL
		return a.expressionToSQL(value)
	}

	// Handle arrays
	if arr, ok := value.([]interface{}); ok {
		// Convert array elements to SQL literals
		elements := make([]string, len(arr))
		for i, elem := range arr {
			elementSQL, err := a.dataOp.valueToSQL(elem)
			if err != nil {
				return "", fmt.Errorf("invalid array element %d: %w", i, err)
			}
			elements[i] = elementSQL
		}
		return fmt.Sprintf("[%s]", strings.Join(elements, " ")), nil
	}

	// Handle primitive values
	return a.dataOp.valueToSQL(value)
}

// expressionToSQL converts a JSON Logic expression to SQL.
func (a *ArrayOperator) expressionToSQL(expr interface{}) (string, error) {
	// Handle ProcessedValue (pre-processed SQL from parser)
	if pv, ok := expr.(ProcessedValue); ok {
		if pv.IsSQL {
			return pv.Value, nil
		}
		// It's a literal, recursively convert it
		return a.expressionToSQL(pv.Value)
	}

	// Handle primitive values
	if a.isPrimitive(expr) {
		return a.dataOp.valueToSQL(expr)
	}

	// Handle var expressions
	if varExpr, ok := expr.(map[string]interface{}); ok {
		if varName, hasVar := varExpr[OpVar]; hasVar {
			// Special case: empty var name represents the current element in array operations
			if varName == "" {
				return ElemVar, nil
			}
			return a.dataOp.ToSQL(OpVar, []interface{}{varName})
		}
	}

	// Handle complex expressions by delegating to other operators
	if exprMap, ok := expr.(map[string]interface{}); ok {
		for operator, args := range exprMap {
			switch operator {
			case "==", "===", "!=", "!==", ">", ">=", "<", "<=", "in":
				if arr, ok := args.([]interface{}); ok {
					return a.comparisonOp.ToSQL(operator, arr)
				}
			case "and", "or", "!", "!!", "if":
				if arr, ok := args.([]interface{}); ok {
					return a.getLogicalOperator().ToSQL(operator, arr)
				}
			case "+", "-", "*", "/", "%", "max", "min":
				if arr, ok := args.([]interface{}); ok {
					return a.numericOp.ToSQL(operator, arr)
				}
			case "map", "filter", "reduce", "all", "some", "none", "merge":
				// Handle nested array operators
				if arr, ok := args.([]interface{}); ok {
					return a.ToSQL(operator, arr)
				}
			default:
				// Try to use the expression parser callback for unknown operators
				// This enables support for custom operators in nested contexts
				if a.config != nil && a.config.HasExpressionParser() {
					return a.config.ParseExpression(exprMap, "$")
				}
				return "", fmt.Errorf("unsupported operator in array expression: %s", operator)
			}
		}
	}

	return "", fmt.Errorf("invalid expression type: %T", expr)
}

// replaceElementRefsInSQL applies word-boundary replacement of "item" and "current"
// with "elem" in a final SQL string. This is a safety net for SQL produced by custom
// operators or nested operator chains that may emit literal "item"/"current" tokens
// not reachable by the AST-level rewrite.
func (a *ArrayOperator) replaceElementRefsInSQL(sql string) string {
	sql = replaceWithLiteral(itemPattern, sql, ElemVar)
	sql = replaceWithLiteral(currentPattern, sql, ElemVar)
	return sql
}

// replaceWithLiteral replaces regex matches while preserving the captured prefix
// and treating the replacement as a literal string (no $-expansion).
func replaceWithLiteral(re *regexp.Regexp, s, replacement string) string {
	return re.ReplaceAllStringFunc(s, func(match string) string {
		// The match includes the captured prefix character (or empty at start-of-string).
		// Find where the keyword starts by checking the prefix.
		loc := re.FindStringSubmatchIndex(match)
		if loc == nil {
			return match
		}
		prefix := match[loc[2]:loc[3]]
		return prefix + replacement
	})
}

// mapElementVarName maps JSONLogic element variable names to the SQL UNNEST alias.
// Returns the mapped name, or the original if no mapping applies.
// Only exact matches ("item", "current", "") and dot-prefix matches ("item.", "current.")
// are mapped - this prevents corrupting field names like "current_balance" or "item_count".
func (a *ArrayOperator) mapElementVarName(varStr string) string {
	// Exact matches for element references
	// Note: empty string ("") is NOT rewritten here - it's handled by expressionToSQL's
	// special case which returns ElemVar directly without schema validation.
	if varStr == ItemVar || varStr == CurrentVar {
		return ElemVar
	}
	// Dot-notation: "item.field" → "elem.field", "current.field" → "elem.field"
	if strings.HasPrefix(varStr, ItemVar+".") {
		return ElemVar + varStr[len(ItemVar):]
	}
	if strings.HasPrefix(varStr, CurrentVar+".") {
		return ElemVar + varStr[len(CurrentVar):]
	}
	return varStr
}

// isArrayOperator returns true if the operator is an array operator that
// introduces its own element scope. Used to prevent rewriting into nested scopes.
func (a *ArrayOperator) isArrayOperator(op string) bool {
	switch op {
	case OpMap, OpFilter, OpReduce, OpAll, OpSome, OpNone:
		return true
	}
	return false
}

// rewriteElementVars walks the JSONLogic AST and rewrites element variable
// references ("item", "current", "") to the UNNEST alias ("elem") before SQL
// generation. This replaces the old post-hoc string replacement approach which
// corrupted field names containing "item" or "current" as substrings.
//
// When a nested array operator is encountered, only its array source argument
// (args[0]) is rewritten - the lambda/condition (args[1+]) is left for the
// nested operator to handle, preserving correct variable scoping.
func (a *ArrayOperator) rewriteElementVars(expr interface{}) interface{} {
	switch e := expr.(type) {
	case ProcessedValue:
		// Pre-processed SQL from custom operators - use word-boundary regex
		if e.IsSQL {
			replaced := replaceWithLiteral(itemPattern, e.Value, ElemVar)
			replaced = replaceWithLiteral(currentPattern, replaced, ElemVar)
			if replaced != e.Value {
				return ProcessedValue{Value: replaced, IsSQL: true}
			}
		}
		return e
	case map[string]interface{}:
		if len(e) == 1 {
			// Check for var expression
			if varName, hasVar := e[OpVar]; hasVar {
				if varStr, ok := varName.(string); ok {
					mapped := a.mapElementVarName(varStr)
					if mapped != varStr {
						return map[string]interface{}{OpVar: mapped}
					}
				}
				// Handle array-form var: {"var": ["current", defaultValue]}
				if varArr, ok := varName.([]interface{}); ok && len(varArr) > 0 {
					if varStr, ok := varArr[0].(string); ok {
						mapped := a.mapElementVarName(varStr)
						if mapped != varStr {
							newArr := make([]interface{}, len(varArr))
							copy(newArr, varArr)
							newArr[0] = mapped
							return map[string]interface{}{OpVar: newArr}
						}
					}
				}
				return e
			}
			// Check for nested array operator - don't rewrite its lambda body
			for opName, opArgs := range e {
				if a.isArrayOperator(opName) {
					if arr, ok := opArgs.([]interface{}); ok {
						newArgs := make([]interface{}, len(arr))
						copy(newArgs, arr)
						// Rewrite args[0] (array source - outer scope)
						if len(newArgs) > 0 {
							newArgs[0] = a.rewriteElementVars(arr[0])
						}
						// For reduce, also rewrite args[2] (initial value - outer scope)
						if opName == OpReduce && len(newArgs) > 2 {
							newArgs[2] = a.rewriteElementVars(arr[2])
						}
						return map[string]interface{}{opName: newArgs}
					}
				}
			}
			// Regular single-key operator - recursively rewrite values
			for opName, opArgs := range e {
				return map[string]interface{}{opName: a.rewriteElementVars(opArgs)}
			}
		}
		// Multi-key map - recursively rewrite all values
		result := make(map[string]interface{}, len(e))
		for k, v := range e {
			result[k] = a.rewriteElementVars(v)
		}
		return result

	case []interface{}:
		result := make([]interface{}, len(e))
		for i, v := range e {
			result[i] = a.rewriteElementVars(v)
		}
		return result

	default:
		return expr
	}
}

// isPrimitive checks if a value is a primitive type.
func (a *ArrayOperator) isPrimitive(value interface{}) bool {
	switch value.(type) {
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return true
	case nil:
		return true
	default:
		return false
	}
}
