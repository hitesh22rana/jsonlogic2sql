package operators

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// NumericOperator handles numeric operations like +, -, *, /, %, max, min.
type NumericOperator struct {
	config       *OperatorConfig
	dataOp       *DataOperator
	comparisonOp *ComparisonOperator
}

// NewNumericOperator creates a new NumericOperator instance with optional config.
func NewNumericOperator(config *OperatorConfig) *NumericOperator {
	return &NumericOperator{
		config:       config,
		dataOp:       NewDataOperator(config),
		comparisonOp: NewComparisonOperator(config),
	}
}

// schema returns the schema from config, or nil if not configured.
func (n *NumericOperator) schema() SchemaProvider {
	if n.config == nil {
		return nil
	}
	return n.config.Schema
}

// validateNumericOperand checks if a field used in a numeric operation is of numeric type.
func (n *NumericOperator) validateNumericOperand(value interface{}) error {
	if n.schema() == nil {
		return nil // No schema, no validation
	}

	fieldName := n.extractFieldNameFromValue(value)
	if fieldName == "" {
		return nil // Can't determine field name, skip validation
	}

	fieldType := n.schema().GetFieldType(fieldName)
	if fieldType == "" {
		return nil // Field not in schema, skip validation (existence checked by DataOperator)
	}

	if !n.schema().IsNumericType(fieldName) {
		return fmt.Errorf("numeric operation on non-numeric field '%s' (type: %s)", fieldName, fieldType)
	}

	return nil
}

// extractFieldNameFromValue extracts field name from a value that might be a var expression.
func (n *NumericOperator) extractFieldNameFromValue(value interface{}) string {
	if varExpr, ok := value.(map[string]interface{}); ok {
		if varName, hasVar := varExpr[OpVar]; hasVar {
			return n.extractFieldName(varName)
		}
	}
	return ""
}

// extractFieldName extracts the field name from a var argument.
func (n *NumericOperator) extractFieldName(varName interface{}) string {
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

// ToSQL converts a numeric operation to SQL.
func (n *NumericOperator) ToSQL(operator string, args []interface{}) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("numeric operator %s requires at least one argument", operator)
	}

	switch operator {
	case "+":
		return n.handleAddition(args)
	case "-":
		return n.handleSubtraction(args)
	case "*":
		return n.handleMultiplication(args)
	case "/":
		return n.handleDivision(args)
	case "%":
		return n.handleModulo(args)
	case "max":
		return n.handleMax(args)
	case "min":
		return n.handleMin(args)
	default:
		return "", fmt.Errorf("unsupported numeric operator: %s", operator)
	}
}

// handleAddition converts + operator to SQL.
func (n *NumericOperator) handleAddition(args []interface{}) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("addition requires at least 1 argument")
	}

	// Validate operand types
	for _, arg := range args {
		if err := n.validateNumericOperand(arg); err != nil {
			return "", err
		}
	}

	// Handle unary plus (cast to number)
	if len(args) == 1 {
		operand, err := n.valueToSQL(args[0])
		if err != nil {
			return "", fmt.Errorf("invalid unary plus argument: %w", err)
		}
		return fmt.Sprintf("CAST(%s AS NUMERIC)", operand), nil
	}

	operands := make([]string, len(args))
	for i, arg := range args {
		operand, err := n.valueToSQL(arg)
		if err != nil {
			return "", fmt.Errorf("invalid addition argument %d: %w", i, err)
		}
		operands[i] = operand
	}

	return fmt.Sprintf("(%s)", strings.Join(operands, " + ")), nil
}

// handleSubtraction converts - operator to SQL.
func (n *NumericOperator) handleSubtraction(args []interface{}) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("subtraction requires at least 1 argument")
	}

	// Validate operand types
	for _, arg := range args {
		if err := n.validateNumericOperand(arg); err != nil {
			return "", err
		}
	}

	// Handle unary minus (negation) - wrap in parentheses for safety in nested expressions
	if len(args) == 1 {
		operand, err := n.valueToSQL(args[0])
		if err != nil {
			return "", fmt.Errorf("invalid unary minus argument: %w", err)
		}
		return fmt.Sprintf("(-%s)", operand), nil
	}

	operands := make([]string, len(args))
	for i, arg := range args {
		operand, err := n.valueToSQL(arg)
		if err != nil {
			return "", fmt.Errorf("invalid subtraction argument %d: %w", i, err)
		}
		operands[i] = operand
	}

	return fmt.Sprintf("(%s)", strings.Join(operands, " - ")), nil
}

// handleMultiplication converts * operator to SQL.
func (n *NumericOperator) handleMultiplication(args []interface{}) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("multiplication requires at least 2 arguments")
	}

	// Validate operand types
	for _, arg := range args {
		if err := n.validateNumericOperand(arg); err != nil {
			return "", err
		}
	}

	operands := make([]string, len(args))
	for i, arg := range args {
		operand, err := n.valueToSQL(arg)
		if err != nil {
			return "", fmt.Errorf("invalid multiplication argument %d: %w", i, err)
		}
		operands[i] = operand
	}

	return fmt.Sprintf("(%s)", strings.Join(operands, " * ")), nil
}

// handleDivision converts / operator to SQL.
func (n *NumericOperator) handleDivision(args []interface{}) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("division requires at least 2 arguments")
	}

	// Validate operand types
	for _, arg := range args {
		if err := n.validateNumericOperand(arg); err != nil {
			return "", err
		}
	}

	operands := make([]string, len(args))
	for i, arg := range args {
		operand, err := n.valueToSQL(arg)
		if err != nil {
			return "", fmt.Errorf("invalid division argument %d: %w", i, err)
		}
		operands[i] = operand
	}

	return fmt.Sprintf("(%s)", strings.Join(operands, " / ")), nil
}

// handleModulo converts % operator to SQL.
func (n *NumericOperator) handleModulo(args []interface{}) (string, error) {
	if len(args) != 2 {
		return "", fmt.Errorf("modulo requires exactly 2 arguments")
	}

	// Validate operand types
	for _, arg := range args {
		if err := n.validateNumericOperand(arg); err != nil {
			return "", err
		}
	}

	left, err := n.valueToSQL(args[0])
	if err != nil {
		return "", fmt.Errorf("invalid modulo left argument: %w", err)
	}

	right, err := n.valueToSQL(args[1])
	if err != nil {
		return "", fmt.Errorf("invalid modulo right argument: %w", err)
	}

	return fmt.Sprintf("(%s %% %s)", left, right), nil
}

// handleMax converts max operator to SQL.
func (n *NumericOperator) handleMax(args []interface{}) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("max requires at least 2 arguments")
	}

	// Validate operand types
	for _, arg := range args {
		if err := n.validateNumericOperand(arg); err != nil {
			return "", err
		}
	}

	operands := make([]string, len(args))
	for i, arg := range args {
		operand, err := n.valueToSQL(arg)
		if err != nil {
			return "", fmt.Errorf("invalid max argument %d: %w", i, err)
		}
		operands[i] = operand
	}

	return fmt.Sprintf("GREATEST(%s)", strings.Join(operands, ", ")), nil
}

// handleMin converts min operator to SQL.
func (n *NumericOperator) handleMin(args []interface{}) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("min requires at least 2 arguments")
	}

	// Validate operand types
	for _, arg := range args {
		if err := n.validateNumericOperand(arg); err != nil {
			return "", err
		}
	}

	operands := make([]string, len(args))
	for i, arg := range args {
		operand, err := n.valueToSQL(arg)
		if err != nil {
			return "", fmt.Errorf("invalid min argument %d: %w", i, err)
		}
		operands[i] = operand
	}

	return fmt.Sprintf("LEAST(%s)", strings.Join(operands, ", ")), nil
}

// valueToSQL converts a value to SQL, handling var expressions and literals.
func (n *NumericOperator) valueToSQL(value interface{}) (string, error) {
	// Handle ProcessedValue (pre-processed SQL from parser)
	if pv, ok := value.(ProcessedValue); ok {
		if pv.IsSQL {
			return pv.Value, nil
		}
		// It's a literal, convert it
		return n.dataOp.valueToSQL(pv.Value)
	}

	// Handle plain strings: attempt numeric coercion per JSONLogic spec,
	// otherwise treat as a safely-quoted literal to prevent SQL injection.
	// Pre-processed SQL from the parser arrives as ProcessedValue (handled above),
	// so any plain string here is a raw JSON literal.
	// Trim whitespace before numeric checks to match JSONLogic's JS coercion
	// (e.g. " 3 " → 3), but quote the original if it's non-numeric.
	if str, ok := value.(string); ok {
		trimmed := strings.TrimSpace(str)
		if isIntegerLiteral(trimmed) {
			return trimmed, nil
		}
		if num, err := strconv.ParseFloat(trimmed, 64); err == nil && !math.IsNaN(num) && !math.IsInf(num, 0) {
			return strconv.FormatFloat(num, 'f', -1, 64), nil
		}
		return n.dataOp.valueToSQL(str)
	}

	// Handle var expressions and complex expressions
	if expr, ok := value.(map[string]interface{}); ok {
		if varExpr, hasVar := expr[OpVar]; hasVar {
			return n.dataOp.ToSQL(OpVar, []interface{}{varExpr})
		}
		// Handle complex expressions by recursively parsing them
		for operator, args := range expr {
			if arr, ok := args.([]interface{}); ok {
				// Handle different operator types
				switch operator {
				case "==", "===", "!=", "!==", ">", ">=", "<", "<=", "in":
					// Process arguments first to handle nested expressions
					processedArgs, err := n.processComplexArgsForComparison(arr)
					if err != nil {
						return "", err
					}
					// Delegate to comparison operator
					return n.comparisonOp.ToSQL(operator, processedArgs)
				case "+", "-", "*", "/", "%", "max", "min":
					// Recursively process the arguments
					processedArgs, err := n.processComplexArgs(arr)
					if err != nil {
						return "", err
					}
					// Generate SQL for the complex expression
					return n.generateComplexSQL(operator, processedArgs)
				case "if":
					// Handle if operator - delegate to logical operator
					logicalOp := NewLogicalOperator(n.config)
					return logicalOp.ToSQL("if", arr)
				case "and", "or", "!":
					// Handle logical operators - delegate to logical operator
					logicalOp := NewLogicalOperator(n.config)
					return logicalOp.ToSQL(operator, arr)
				case "reduce", "filter", "map", "some", "all", "none", "merge":
					// Handle array operators - delegate to array operator
					arrayOp := NewArrayOperator(n.config)
					return arrayOp.ToSQL(operator, arr)
				default:
					// Try to use the expression parser callback for unknown operators
					// This enables support for custom operators in nested contexts
					if n.config != nil && n.config.HasExpressionParser() {
						return n.config.ParseExpression(expr, "$")
					}
					return "", fmt.Errorf("unsupported operator in numeric expression: %s", operator)
				}
			}
		}
	}

	// Handle primitive values
	return n.dataOp.valueToSQL(value)
}

// processComplexArgs recursively processes arguments for complex expressions.
func (n *NumericOperator) processComplexArgs(args []interface{}) ([]string, error) {
	processed := make([]string, len(args))

	for i, arg := range args {
		sql, err := n.valueToSQL(arg)
		if err != nil {
			return nil, err
		}
		processed[i] = sql
	}

	return processed, nil
}

// processComplexArgsForComparison processes arguments for comparison operators.
// Var expressions and primitives are passed through so the comparison operator
// can perform schema-based type coercion (e.g. number→string for string fields).
// Only nested complex expressions (arithmetic, logical, etc.) are pre-evaluated
// to SQL and wrapped in SQLResult.
func (n *NumericOperator) processComplexArgsForComparison(args []interface{}) ([]interface{}, error) {
	processed := make([]interface{}, len(args))

	for i, arg := range args {
		if exprMap, ok := arg.(map[string]interface{}); ok && len(exprMap) == 1 {
			if _, isVar := exprMap[OpVar]; isVar {
				processed[i] = arg
				continue
			}
			sql, err := n.valueToSQL(arg)
			if err != nil {
				return nil, err
			}
			processed[i] = SQLResult(sql)
			continue
		}
		// Primitives (string, float64, bool, nil) and ProcessedValue pass through
		// so comparison can apply schema coercion and proper quoting.
		processed[i] = arg
	}

	return processed, nil
}

// generateComplexSQL generates SQL for complex expressions.
func (n *NumericOperator) generateComplexSQL(operator string, args []string) (string, error) {
	switch operator {
	case "+":
		if len(args) < 2 {
			return "", fmt.Errorf("addition requires at least 2 arguments")
		}
		return fmt.Sprintf("(%s)", strings.Join(args, " + ")), nil
	case "-":
		if len(args) == 1 {
			// Unary minus (negation) - wrap in parentheses for safety in nested expressions
			return fmt.Sprintf("(-%s)", args[0]), nil
		}
		if len(args) < 2 {
			return "", fmt.Errorf("subtraction requires at least 1 argument")
		}
		return fmt.Sprintf("(%s)", strings.Join(args, " - ")), nil
	case "*":
		if len(args) < 2 {
			return "", fmt.Errorf("multiplication requires at least 2 arguments")
		}
		return fmt.Sprintf("(%s)", strings.Join(args, " * ")), nil
	case "/":
		if len(args) < 2 {
			return "", fmt.Errorf("division requires at least 2 arguments")
		}
		return fmt.Sprintf("(%s)", strings.Join(args, " / ")), nil
	case "%":
		if len(args) < 2 {
			return "", fmt.Errorf("modulo requires at least 2 arguments")
		}
		return fmt.Sprintf("(%s)", strings.Join(args, " % ")), nil
	case "max":
		if len(args) < 2 {
			return "", fmt.Errorf("max requires at least 2 arguments")
		}
		return fmt.Sprintf("GREATEST(%s)", strings.Join(args, ", ")), nil
	case "min":
		if len(args) < 2 {
			return "", fmt.Errorf("min requires at least 2 arguments")
		}
		return fmt.Sprintf("LEAST(%s)", strings.Join(args, ", ")), nil
	default:
		// For other operators (array, logical, etc.), they should have been pre-processed
		// If we see them here, it means they weren't processed correctly
		return "", fmt.Errorf("unsupported operator in numeric expression: %s", operator)
	}
}

// isIntegerLiteral reports whether s matches ^[+-]?[0-9]+$ - a bare integer
// with an optional sign and no decimal point, exponent, or other characters.
// Validated strings are safe to emit directly as SQL numeric literals.
func isIntegerLiteral(s string) bool {
	if len(s) == 0 {
		return false
	}
	start := 0
	if s[0] == '+' || s[0] == '-' {
		start = 1
	}
	if start >= len(s) {
		return false
	}
	for i := start; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
