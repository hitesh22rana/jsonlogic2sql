package jsonlogic2sql

import (
	"encoding/json"

	tperrors "github.com/h22rana/jsonlogic2sql/internal/errors"
	"github.com/h22rana/jsonlogic2sql/internal/params"
)

// QueryParam represents a single bind parameter collected during parameterized transpilation.
// Name is the stable identifier (e.g., "p1", "p2"), and Value is the Go-native value to bind.
type QueryParam = params.QueryParam

// TranspileParameterized converts a JSON Logic string to a SQL WHERE clause
// with bind parameter placeholders instead of inlined literals.
func (t *Transpiler) TranspileParameterized(jsonLogic string) (string, []QueryParam, error) {
	var logic interface{}
	if err := json.Unmarshal([]byte(jsonLogic), &logic); err != nil {
		return "", nil, tperrors.NewInvalidJSON(err)
	}
	return t.parser.ParseParameterized(logic)
}

// TranspileParameterizedFromMap converts a pre-parsed JSON Logic map to a SQL
// WHERE clause with bind parameter placeholders.
func (t *Transpiler) TranspileParameterizedFromMap(logic map[string]interface{}) (string, []QueryParam, error) {
	return t.parser.ParseParameterized(logic)
}

// TranspileParameterizedFromInterface converts any JSON Logic interface{} to a
// SQL WHERE clause with bind parameter placeholders.
func (t *Transpiler) TranspileParameterizedFromInterface(logic interface{}) (string, []QueryParam, error) {
	return t.parser.ParseParameterized(logic)
}

// TranspileConditionParameterized converts a JSON Logic string to a SQL condition
// (without the WHERE keyword) with bind parameter placeholders.
func (t *Transpiler) TranspileConditionParameterized(jsonLogic string) (string, []QueryParam, error) {
	var logic interface{}
	if err := json.Unmarshal([]byte(jsonLogic), &logic); err != nil {
		return "", nil, tperrors.NewInvalidJSON(err)
	}
	return t.parser.ParseConditionParameterized(logic)
}

// TranspileConditionParameterizedFromMap converts a pre-parsed JSON Logic map to
// a SQL condition (without the WHERE keyword) with bind parameter placeholders.
func (t *Transpiler) TranspileConditionParameterizedFromMap(logic map[string]interface{}) (string, []QueryParam, error) {
	return t.parser.ParseConditionParameterized(logic)
}

// TranspileConditionParameterizedFromInterface converts any JSON Logic interface{}
// to a SQL condition (without the WHERE keyword) with bind parameter placeholders.
func (t *Transpiler) TranspileConditionParameterizedFromInterface(logic interface{}) (string, []QueryParam, error) {
	return t.parser.ParseConditionParameterized(logic)
}

// Package-level convenience functions

// TranspileParameterized converts a JSON Logic string to a SQL WHERE clause
// with bind parameter placeholders.
func TranspileParameterized(d Dialect, jsonLogic string) (string, []QueryParam, error) {
	t, err := NewTranspiler(d)
	if err != nil {
		return "", nil, err
	}
	return t.TranspileParameterized(jsonLogic)
}

// TranspileParameterizedFromMap converts a pre-parsed JSON Logic map to a SQL
// WHERE clause with bind parameter placeholders.
func TranspileParameterizedFromMap(d Dialect, logic map[string]interface{}) (string, []QueryParam, error) {
	t, err := NewTranspiler(d)
	if err != nil {
		return "", nil, err
	}
	return t.TranspileParameterizedFromMap(logic)
}

// TranspileParameterizedFromInterface converts any JSON Logic interface{} to a
// SQL WHERE clause with bind parameter placeholders.
func TranspileParameterizedFromInterface(d Dialect, logic interface{}) (string, []QueryParam, error) {
	t, err := NewTranspiler(d)
	if err != nil {
		return "", nil, err
	}
	return t.TranspileParameterizedFromInterface(logic)
}

// TranspileConditionParameterized converts a JSON Logic string to a SQL condition
// (without the WHERE keyword) with bind parameter placeholders.
func TranspileConditionParameterized(d Dialect, jsonLogic string) (string, []QueryParam, error) {
	t, err := NewTranspiler(d)
	if err != nil {
		return "", nil, err
	}
	return t.TranspileConditionParameterized(jsonLogic)
}

// TranspileConditionParameterizedFromMap converts a pre-parsed JSON Logic map to
// a SQL condition (without the WHERE keyword) with bind parameter placeholders.
func TranspileConditionParameterizedFromMap(d Dialect, logic map[string]interface{}) (string, []QueryParam, error) {
	t, err := NewTranspiler(d)
	if err != nil {
		return "", nil, err
	}
	return t.TranspileConditionParameterizedFromMap(logic)
}

// TranspileConditionParameterizedFromInterface converts any JSON Logic interface{}
// to a SQL condition (without the WHERE keyword) with bind parameter placeholders.
func TranspileConditionParameterizedFromInterface(d Dialect, logic interface{}) (string, []QueryParam, error) {
	t, err := NewTranspiler(d)
	if err != nil {
		return "", nil, err
	}
	return t.TranspileConditionParameterizedFromInterface(logic)
}
