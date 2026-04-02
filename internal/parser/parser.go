package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/h22rana/jsonlogic2sql/internal/dialect"
	tperrors "github.com/h22rana/jsonlogic2sql/internal/errors"
	"github.com/h22rana/jsonlogic2sql/internal/operators"
	"github.com/h22rana/jsonlogic2sql/internal/params"
	"github.com/h22rana/jsonlogic2sql/internal/validator"
)

const (
	maxSafeJSONInt = int64(1<<53 - 1)
	minSafeJSONInt = -maxSafeJSONInt
)

// CustomOperatorHandler is an interface for custom operator implementations.
// This mirrors the public OperatorHandler interface.
type CustomOperatorHandler interface {
	ToSQL(operator string, args []interface{}) (string, error)
}

// CustomOperatorLookup is a function type for looking up custom operators.
type CustomOperatorLookup func(operatorName string) (CustomOperatorHandler, bool)

// Parser parses JSON Logic expressions and converts them to SQL WHERE clauses.
type Parser struct {
	validator      *validator.Validator
	config         *operators.OperatorConfig
	dataOp         *operators.DataOperator
	comparisonOp   *operators.ComparisonOperator
	logicalOp      *operators.LogicalOperator
	numericOp      *operators.NumericOperator
	stringOp       *operators.StringOperator
	arrayOp        *operators.ArrayOperator
	customOpLookup CustomOperatorLookup
}

// NewParser creates a new parser instance with config.
// If config is nil, defaults to BigQuery dialect for backward compatibility.
func NewParser(config *operators.OperatorConfig) *Parser {
	if config == nil {
		// Default to BigQuery for backward compatibility in internal usage
		config = operators.NewOperatorConfig(dialect.DialectBigQuery, nil)
	}
	p := &Parser{
		validator:    validator.NewValidator(),
		config:       config,
		dataOp:       operators.NewDataOperator(config),
		comparisonOp: operators.NewComparisonOperator(config),
		logicalOp:    operators.NewLogicalOperator(config),
		numericOp:    operators.NewNumericOperator(config),
		stringOp:     operators.NewStringOperator(config),
		arrayOp:      operators.NewArrayOperator(config),
	}

	// Set the expression parser callbacks so operators can delegate
	// nested expression parsing back to the parser (enabling custom operators)
	config.SetExpressionParser(p.parseExpression)
	config.SetParamExpressionParser(p.parseExpressionParam)

	return p
}

// SetCustomOperatorLookup sets the function used to look up custom operators.
// This also sets up the validator to recognize custom operators.
func (p *Parser) SetCustomOperatorLookup(lookup CustomOperatorLookup) {
	p.customOpLookup = lookup
	// Also set up the validator to recognize custom operators
	p.validator.SetCustomOperatorChecker(func(operatorName string) bool {
		if lookup == nil {
			return false
		}
		_, ok := lookup(operatorName)
		return ok
	})
}

// SetSchema sets the schema provider for field validation and type checking.
func (p *Parser) SetSchema(schema operators.SchemaProvider) {
	p.config.Schema = schema
	// All operators share the same config, so they automatically see the new schema
}

// Parse converts a JSON Logic expression to SQL WHERE clause.
func (p *Parser) Parse(logic interface{}) (string, error) {
	// First validate the expression
	if err := p.validator.Validate(logic); err != nil {
		return "", tperrors.NewValidationError(err)
	}

	// Parse the expression with root path
	sql, err := p.parseExpression(logic, "$")
	if err != nil {
		return "", err // TranspileError already contains full context
	}

	// Wrap in WHERE clause
	return fmt.Sprintf("WHERE %s", sql), nil
}

// ParseCondition converts a JSON Logic expression to a SQL condition without the WHERE keyword.
// This is useful when you need to embed the condition in a larger query.
func (p *Parser) ParseCondition(logic interface{}) (string, error) {
	// First validate the expression
	if err := p.validator.Validate(logic); err != nil {
		return "", tperrors.NewValidationError(err)
	}

	// Parse the expression with root path
	sql, err := p.parseExpression(logic, "$")
	if err != nil {
		return "", err // TranspileError already contains full context
	}

	// Return condition without WHERE prefix
	return sql, nil
}

// parseExpression recursively parses JSON Logic expressions.
// path is the JSONPath to the current expression for error reporting.
func (p *Parser) parseExpression(expr interface{}, path string) (string, error) {
	// Handle primitive values (should not happen in normal JSON Logic, but handle gracefully)
	if p.isPrimitive(expr) {
		return "", tperrors.NewPrimitiveNotAllowed(path)
	}

	// Handle arrays (should not happen in normal JSON Logic, but handle gracefully)
	if _, ok := expr.([]interface{}); ok {
		return "", tperrors.NewArrayNotAllowed(path)
	}

	// Handle objects (operators)
	if obj, ok := expr.(map[string]interface{}); ok {
		if len(obj) != 1 {
			return "", tperrors.NewMultipleKeys(path)
		}

		for operator, args := range obj {
			operatorPath := tperrors.BuildPath(path, operator, -1)
			return p.parseOperator(operator, args, operatorPath)
		}
	}

	return "", tperrors.New(tperrors.ErrInvalidExpression, "", path,
		fmt.Sprintf("invalid expression type: %T", expr))
}

// wrapOperatorError wraps an operator error with TranspileError if it isn't already.
func (p *Parser) wrapOperatorError(operator, path string, err error) error {
	if err == nil {
		return nil
	}
	// Check if it's already a TranspileError
	var tpErr *tperrors.TranspileError
	if errors.As(err, &tpErr) {
		return err
	}
	// Wrap with appropriate error code based on error message
	return tperrors.Wrap(tperrors.ErrInvalidArgument, operator, path, "operator error", err)
}

// parseOperator parses a specific operator.
// path is the JSONPath to this operator for error reporting.
func (p *Parser) parseOperator(operator string, args interface{}, path string) (string, error) {
	// Check for custom operators first
	if p.customOpLookup != nil {
		if handler, ok := p.customOpLookup(operator); ok {
			// Process the arguments for the custom operator
			processedArgs, err := p.processCustomOperatorArgs(args, path)
			if err != nil {
				return "", tperrors.Wrap(tperrors.ErrCustomOperatorFailed, operator, path,
					"failed to process custom operator arguments", err)
			}
			sql, err := handler.ToSQL(operator, processedArgs)
			if err != nil {
				return "", tperrors.Wrap(tperrors.ErrCustomOperatorFailed, operator, path,
					"custom operator failed", err)
			}
			return sql, nil
		}
	}

	// Handle different operator types
	switch operator {
	// Data access operators
	case "var":
		sql, err := p.dataOp.ToSQL(operator, []interface{}{args})
		return sql, p.wrapOperatorError(operator, path, err)
	case "missing":
		// missing takes a single string argument, wrap it in an array
		sql, err := p.dataOp.ToSQL(operator, []interface{}{args})
		return sql, p.wrapOperatorError(operator, path, err)
	case "missing_some":
		if arr, ok := args.([]interface{}); ok {
			sql, err := p.dataOp.ToSQL(operator, arr)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)

	// Comparison operators
	case "==", "===", "!=", "!==", ">", ">=", "<", "<=", "in":
		if arr, ok := args.([]interface{}); ok {
			// Process arguments to handle complex expressions
			processedArgs, err := p.processArgs(arr, path)
			if err != nil {
				return "", err // processArgs already returns TranspileError
			}
			sql, err := p.comparisonOp.ToSQL(operator, processedArgs)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)

	// Logical operators
	case "and", "or", "if":
		if arr, ok := args.([]interface{}); ok {
			// Process arguments to handle custom operators in nested expressions
			processedArgs, err := p.processArgs(arr, path)
			if err != nil {
				return "", err
			}
			sql, err := p.logicalOp.ToSQL(operator, processedArgs)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)
	case "!", "!!":
		// These unary operators can accept both array and non-array arguments
		if arr, ok := args.([]interface{}); ok {
			// Process arguments to handle custom operators
			processedArgs, err := p.processArgs(arr, path)
			if err != nil {
				return "", err
			}
			sql, err := p.logicalOp.ToSQL(operator, processedArgs)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		// Process non-array argument to handle custom operators before wrapping
		processedArg, err := p.processArg(args, path, 0)
		if err != nil {
			return "", err
		}
		sql, err := p.logicalOp.ToSQL(operator, []interface{}{processedArg})
		return sql, p.wrapOperatorError(operator, path, err)

	// Numeric operators
	case "+", "-", "*", "/", "%", "max", "min":
		if arr, ok := args.([]interface{}); ok {
			// Process arguments to handle complex expressions
			processedArgs, err := p.processArgs(arr, path)
			if err != nil {
				return "", err
			}
			sql, err := p.numericOp.ToSQL(operator, processedArgs)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)

	// Array operators
	case operators.OpMap, operators.OpFilter, operators.OpReduce, operators.OpAll, operators.OpSome, operators.OpNone, operators.OpMerge:
		if arr, ok := args.([]interface{}); ok {
			sql, err := p.arrayOp.ToSQLAtPath(operator, arr, path)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)

	// String operators
	case "cat", "substr":
		if arr, ok := args.([]interface{}); ok {
			sql, err := p.stringOp.ToSQL(operator, arr)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)

	// All operators are now supported
	default:
		return "", tperrors.NewUnsupportedOperator(operator, path)
	}
}

// isBuiltInOperator checks if an operator is a built-in operator.
func (p *Parser) isBuiltInOperator(operator string) bool {
	builtInOps := map[string]bool{
		// Data access
		"var": true, "missing": true, "missing_some": true,
		// Comparison
		"==": true, "===": true, "!=": true, "!==": true,
		">": true, ">=": true, "<": true, "<=": true, "in": true,
		// Logical
		"and": true, "or": true, "!": true, "!!": true, "if": true,
		// Numeric
		"+": true, "-": true, "*": true, "/": true, "%": true,
		"max": true, "min": true,
		// String
		"cat": true, "substr": true,
		// Array
		"map": true, "filter": true, "reduce": true,
		"all": true, "some": true, "none": true, "merge": true,
	}
	return builtInOps[operator]
}

// isArrayOperator checks if an operator introduces/depends on array expression
// semantics that should be delegated directly to ArrayOperator without parser-level
// eager argument preprocessing.
func (p *Parser) isArrayOperator(operator string) bool {
	switch operator {
	case operators.OpMap, operators.OpFilter, operators.OpReduce, operators.OpAll, operators.OpSome, operators.OpNone, operators.OpMerge:
		return true
	}
	return false
}

// processArgs recursively processes arguments to handle custom operators at any nesting level.
// It converts custom operators to SQL while preserving the structure of built-in operators
// but with their nested custom operators already processed.
// path is the JSONPath to the parent operator.
func (p *Parser) processArgs(args []interface{}, path string) ([]interface{}, error) {
	processed := make([]interface{}, len(args))

	for i, arg := range args {
		processedArg, err := p.processArg(arg, path, i)
		if err != nil {
			return nil, err
		}
		processed[i] = processedArg
	}

	return processed, nil
}

// processArg processes a single argument, recursively handling custom operators.
// Returns ProcessedValue when SQL is generated, otherwise returns the original type.
// path is the JSONPath to the parent, index is the argument index.
func (p *Parser) processArg(arg interface{}, path string, index int) (interface{}, error) {
	argPath := tperrors.BuildArrayPath(path, index)

	// If it's a complex expression (map with single key)
	if exprMap, ok := arg.(map[string]interface{}); ok {
		if len(exprMap) == 1 {
			for operator, opArgs := range exprMap {
				operatorPath := tperrors.BuildPath(path, operator, index)

				// Check if it's a custom operator (not built-in)
				if !p.isBuiltInOperator(operator) {
					// It's a custom operator, parse it to SQL
					sql, err := p.parseOperator(operator, opArgs, operatorPath)
					if err != nil {
						return nil, err
					}
					// Wrap in ProcessedValue to mark as SQL
					return operators.SQLResult(sql), nil
				}

				// It's a built-in operator - recursively process its arguments
				// to handle any nested custom operators.
				// Array operators are handled specially: parse them immediately with
				// their full operatorPath so nested custom-operator failures preserve
				// complete JSONPath context under non-array parents (e.g. == / and).
				// ArrayOperator still performs scope-aware rewrites before nested
				// custom operators are parsed.
				if p.isArrayOperator(operator) {
					sql, err := p.parseOperator(operator, opArgs, operatorPath)
					if err != nil {
						return nil, err
					}
					return operators.SQLResult(sql), nil
				}
				processedOpArgs, err := p.processOpArgs(opArgs, operatorPath)
				if err != nil {
					return nil, err
				}
				// Return the expression with processed arguments
				return map[string]interface{}{operator: processedOpArgs}, nil
			}
		}
		// Multi-key maps - keep as is
		return arg, nil
	}

	// Arrays need recursive processing too
	if arr, ok := arg.([]interface{}); ok {
		return p.processArgs(arr, argPath)
	}

	// Primitives - keep as is
	return arg, nil
}

// processOpArgs processes operator arguments (can be array or single value).
// path is the JSONPath to the operator.
func (p *Parser) processOpArgs(opArgs interface{}, path string) (interface{}, error) {
	if arr, ok := opArgs.([]interface{}); ok {
		return p.processArgs(arr, path)
	}
	// Single argument
	return p.processArg(opArgs, path, 0)
}

// processCustomOperatorArgs processes arguments for custom operators.
// It converts all expressions (including var) to their SQL representation.
// path is the JSONPath to the custom operator.
func (p *Parser) processCustomOperatorArgs(args interface{}, path string) ([]interface{}, error) {
	// Handle array arguments
	if arr, ok := args.([]interface{}); ok {
		processed := make([]interface{}, len(arr))
		for i, arg := range arr {
			argPath := tperrors.BuildArrayPath(path, i)
			sql, err := p.processArgToSQL(arg, argPath)
			if err != nil {
				return nil, err
			}
			processed[i] = sql
		}
		return processed, nil
	}

	// Handle single argument (wrap in array)
	sql, err := p.processArgToSQL(args, path)
	if err != nil {
		return nil, err
	}
	return []interface{}{sql}, nil
}

// processArgToSQL converts a single argument to its SQL representation.
// path is the JSONPath to this argument.
func (p *Parser) processArgToSQL(arg interface{}, path string) (interface{}, error) {
	if pv, ok := arg.(operators.ProcessedValue); ok {
		if pv.IsSQL {
			return pv.Value, nil
		}
		return p.primitiveToSQL(pv.Value), nil
	}

	// Handle complex expressions (maps)
	if exprMap, ok := arg.(map[string]interface{}); ok {
		if len(exprMap) == 1 {
			for operator, opArgs := range exprMap {
				operatorPath := tperrors.BuildPath(path, operator, -1)
				// Parse any expression (including var)
				sql, err := p.parseOperator(operator, opArgs, operatorPath)
				if err != nil {
					return nil, err
				}
				return sql, nil
			}
		}
	}

	// Handle primitive values - convert to SQL representation
	return p.primitiveToSQL(arg), nil
}

// primitiveToSQL converts a primitive value to its SQL representation.
func (p *Parser) primitiveToSQL(value interface{}) interface{} {
	switch v := value.(type) {
	case string:
		escaped := strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case nil:
		return "NULL"
	default:
		// Numbers and other types
		return fmt.Sprintf("%v", v)
	}
}

// isPrimitive checks if a value is a primitive type.
func (p *Parser) isPrimitive(value interface{}) bool {
	switch value.(type) {
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number, bool:
		return true
	case nil:
		return true
	default:
		return false
	}
}

// ParseParameterized converts a JSON Logic expression to a SQL WHERE clause
// with bind parameter placeholders instead of inlined literals.
func (p *Parser) ParseParameterized(logic interface{}) (string, []params.QueryParam, error) {
	if err := p.validator.Validate(logic); err != nil {
		return "", nil, tperrors.NewValidationError(err)
	}

	style := params.StyleForDialect(p.config.GetDialect())
	pc := params.NewParamCollector(style)

	sql, err := p.parseExpressionParam(logic, "$", pc)
	if err != nil {
		return "", nil, err
	}

	whereSQL := fmt.Sprintf("WHERE %s", sql)

	if vErr := params.ValidatePlaceholderRefs(whereSQL, pc.Params(), style); vErr != nil {
		return "", nil, vErr
	}

	return whereSQL, pc.Params(), nil
}

// ParseConditionParameterized converts a JSON Logic expression to a SQL condition
// (without WHERE keyword) with bind parameter placeholders.
func (p *Parser) ParseConditionParameterized(logic interface{}) (string, []params.QueryParam, error) {
	if err := p.validator.Validate(logic); err != nil {
		return "", nil, tperrors.NewValidationError(err)
	}

	style := params.StyleForDialect(p.config.GetDialect())
	pc := params.NewParamCollector(style)

	sql, err := p.parseExpressionParam(logic, "$", pc)
	if err != nil {
		return "", nil, err
	}

	if vErr := params.ValidatePlaceholderRefs(sql, pc.Params(), style); vErr != nil {
		return "", nil, vErr
	}

	return sql, pc.Params(), nil
}

// parseExpressionParam is the parameterized variant of parseExpression. Keep in sync.
func (p *Parser) parseExpressionParam(expr interface{}, path string, pc *params.ParamCollector) (string, error) {
	if p.isPrimitive(expr) {
		return "", tperrors.NewPrimitiveNotAllowed(path)
	}

	if _, ok := expr.([]interface{}); ok {
		return "", tperrors.NewArrayNotAllowed(path)
	}

	if obj, ok := expr.(map[string]interface{}); ok {
		if len(obj) != 1 {
			return "", tperrors.NewMultipleKeys(path)
		}

		for operator, args := range obj {
			operatorPath := tperrors.BuildPath(path, operator, -1)
			return p.parseOperatorParam(operator, args, operatorPath, pc)
		}
	}

	return "", tperrors.New(tperrors.ErrInvalidExpression, "", path,
		fmt.Sprintf("invalid expression type: %T", expr))
}

// parseOperatorParam is the parameterized variant of parseOperator. Keep in sync.
func (p *Parser) parseOperatorParam(operator string, args interface{}, path string, pc *params.ParamCollector) (string, error) {
	if p.customOpLookup != nil {
		if handler, ok := p.customOpLookup(operator); ok {
			processedArgs, err := p.processCustomOperatorArgsParam(args, path, pc)
			if err != nil {
				return "", tperrors.Wrap(tperrors.ErrCustomOperatorFailed, operator, path,
					"failed to process custom operator arguments", err)
			}
			sql, err := handler.ToSQL(operator, processedArgs)
			if err != nil {
				return "", tperrors.Wrap(tperrors.ErrCustomOperatorFailed, operator, path,
					"custom operator failed", err)
			}
			if ph, bad := params.FindQuotedPlaceholderRef(sql, pc.Params(), pc.Style()); bad {
				return "", tperrors.New(tperrors.ErrCustomOperatorFailed, operator, path,
					fmt.Sprintf("custom operator produced invalid parameterized SQL: placeholder %s appears inside a quoted string literal", ph))
			}
			return sql, nil
		}
	}

	switch operator {
	case "var":
		sql, err := p.dataOp.ToSQLParam(operator, []interface{}{args}, pc)
		return sql, p.wrapOperatorError(operator, path, err)
	case "missing":
		sql, err := p.dataOp.ToSQLParam(operator, []interface{}{args}, pc)
		return sql, p.wrapOperatorError(operator, path, err)
	case "missing_some":
		if arr, ok := args.([]interface{}); ok {
			sql, err := p.dataOp.ToSQLParam(operator, arr, pc)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)

	case "==", "===", "!=", "!==", ">", ">=", "<", "<=", "in":
		if arr, ok := args.([]interface{}); ok {
			processedArgs, err := p.processArgsParam(arr, path, pc)
			if err != nil {
				return "", err
			}
			sql, err := p.comparisonOp.ToSQLParam(operator, processedArgs, pc)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)

	case "and", "or", "if":
		if arr, ok := args.([]interface{}); ok {
			processedArgs, err := p.processArgsParam(arr, path, pc)
			if err != nil {
				return "", err
			}
			sql, err := p.logicalOp.ToSQLParam(operator, processedArgs, pc)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)
	case "!", "!!":
		if arr, ok := args.([]interface{}); ok {
			processedArgs, err := p.processArgsParam(arr, path, pc)
			if err != nil {
				return "", err
			}
			sql, err := p.logicalOp.ToSQLParam(operator, processedArgs, pc)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		processedArg, err := p.processArgParam(args, path, 0, pc)
		if err != nil {
			return "", err
		}
		sql, err := p.logicalOp.ToSQLParam(operator, []interface{}{processedArg}, pc)
		return sql, p.wrapOperatorError(operator, path, err)

	case "+", "-", "*", "/", "%", "max", "min":
		if arr, ok := args.([]interface{}); ok {
			processedArgs, err := p.processArgsParam(arr, path, pc)
			if err != nil {
				return "", err
			}
			sql, err := p.numericOp.ToSQLParam(operator, processedArgs, pc)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)

	case operators.OpMap, operators.OpFilter, operators.OpReduce, operators.OpAll, operators.OpSome, operators.OpNone, operators.OpMerge:
		if arr, ok := args.([]interface{}); ok {
			sql, err := p.arrayOp.ToSQLParamAtPath(operator, arr, pc, path)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)

	case "cat", "substr":
		if arr, ok := args.([]interface{}); ok {
			sql, err := p.stringOp.ToSQLParam(operator, arr, pc)
			return sql, p.wrapOperatorError(operator, path, err)
		}
		return "", tperrors.NewOperatorRequiresArray(operator, path)

	default:
		return "", tperrors.NewUnsupportedOperator(operator, path)
	}
}

// processArgsParam is the parameterized variant of processArgs. Keep in sync.
func (p *Parser) processArgsParam(args []interface{}, path string, pc *params.ParamCollector) ([]interface{}, error) {
	processed := make([]interface{}, len(args))
	for i, arg := range args {
		processedArg, err := p.processArgParam(arg, path, i, pc)
		if err != nil {
			return nil, err
		}
		processed[i] = processedArg
	}
	return processed, nil
}

// processArgParam is the parameterized variant of processArg. Keep in sync.
func (p *Parser) processArgParam(arg interface{}, path string, index int, pc *params.ParamCollector) (interface{}, error) {
	if exprMap, ok := arg.(map[string]interface{}); ok {
		if len(exprMap) == 1 {
			for operator, opArgs := range exprMap {
				operatorPath := tperrors.BuildPath(path, operator, index)

				if !p.isBuiltInOperator(operator) {
					sql, err := p.parseOperatorParam(operator, opArgs, operatorPath, pc)
					if err != nil {
						return nil, err
					}
					return operators.SQLResult(sql), nil
				}

				if p.isArrayOperator(operator) {
					sql, err := p.parseOperatorParam(operator, opArgs, operatorPath, pc)
					if err != nil {
						return nil, err
					}
					return operators.SQLResult(sql), nil
				}
				processedOpArgs, err := p.processOpArgsParam(opArgs, operatorPath, pc)
				if err != nil {
					return nil, err
				}
				return map[string]interface{}{operator: processedOpArgs}, nil
			}
		}
		return arg, nil
	}

	if arr, ok := arg.([]interface{}); ok {
		argPath := tperrors.BuildArrayPath(path, index)
		return p.processArgsParam(arr, argPath, pc)
	}

	return arg, nil
}

// processOpArgsParam is the parameterized variant of processOpArgs. Keep in sync.
func (p *Parser) processOpArgsParam(opArgs interface{}, path string, pc *params.ParamCollector) (interface{}, error) {
	if arr, ok := opArgs.([]interface{}); ok {
		return p.processArgsParam(arr, path, pc)
	}
	return p.processArgParam(opArgs, path, 0, pc)
}

// processCustomOperatorArgsParam is the parameterized variant of processCustomOperatorArgs. Keep in sync.
func (p *Parser) processCustomOperatorArgsParam(args interface{}, path string, pc *params.ParamCollector) ([]interface{}, error) {
	if arr, ok := args.([]interface{}); ok {
		processed := make([]interface{}, len(arr))
		for i, arg := range arr {
			argPath := tperrors.BuildArrayPath(path, i)
			sql, err := p.processArgToSQLParam(arg, argPath, pc)
			if err != nil {
				return nil, err
			}
			processed[i] = sql
		}
		return processed, nil
	}

	sql, err := p.processArgToSQLParam(args, path, pc)
	if err != nil {
		return nil, err
	}
	return []interface{}{sql}, nil
}

// processArgToSQLParam is the parameterized variant of processArgToSQL. Keep in sync.
func (p *Parser) processArgToSQLParam(arg interface{}, path string, pc *params.ParamCollector) (interface{}, error) {
	if pv, ok := arg.(operators.ProcessedValue); ok {
		if pv.IsSQL {
			return pv.Value, nil
		}
		return p.primitiveToSQLParam(pv.Value, pc), nil
	}

	if exprMap, ok := arg.(map[string]interface{}); ok {
		if len(exprMap) == 1 {
			for operator, opArgs := range exprMap {
				operatorPath := tperrors.BuildPath(path, operator, -1)
				sql, err := p.parseOperatorParam(operator, opArgs, operatorPath, pc)
				if err != nil {
					return nil, err
				}
				return sql, nil
			}
		}
	}

	return p.primitiveToSQLParam(arg, pc), nil
}

// primitiveToSQLParam is the parameterized variant of primitiveToSQL. Keep in sync.
// Registers string and numeric values with the ParamCollector.
func (p *Parser) primitiveToSQLParam(value interface{}, pc *params.ParamCollector) interface{} {
	switch v := value.(type) {
	case string:
		return pc.Add(v)
	case json.Number:
		numStr := v.String()
		if i, err := strconv.ParseInt(numStr, 10, 64); err == nil {
			if i >= minSafeJSONInt && i <= maxSafeJSONInt {
				return pc.Add(float64(i))
			}
			return pc.Add(numStr)
		}
		// Preserve large integer precision in parameter values.
		if !strings.ContainsAny(numStr, ".eE") {
			return pc.Add(numStr)
		}
		if f, err := strconv.ParseFloat(numStr, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			// Preserve tiny non-zero numbers that underflow to 0 in float64
			// (e.g. 1e-400) to avoid silently changing semantics.
			if f == 0 && hasNonZeroSignificand(numStr) {
				return pc.Add(numStr)
			}
			return pc.Add(f)
		}
		return pc.Add(numStr)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case nil:
		return "NULL"
	case []interface{}:
		// Format arrays as strings matching the non-param primitiveToSQL path.
		// Custom operators (e.g. REPL contains) rely on the "[x]" string form
		// to detect and unwrap single-element array patterns.
		return fmt.Sprintf("%v", v)
	default:
		return pc.Add(v)
	}
}

// hasNonZeroSignificand reports whether a numeric string has a non-zero digit
// in its significand (the part before 'e'/'E'). Used to detect float64
// underflow where ParseFloat returns 0 but the original value is non-zero.
func hasNonZeroSignificand(s string) bool {
	for _, c := range s {
		if c == 'e' || c == 'E' {
			break
		}
		if c >= '1' && c <= '9' {
			return true
		}
	}
	return false
}
