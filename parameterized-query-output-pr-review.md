# PR #34 Code Review (Local Notes)

PR: `https://github.com/h22rana/jsonlogic2sql/pull/34`  
Branch: `feat/parameterized-query-output`  
Scope reviewed: CLAUDE guidance, shallow bug scan, git history/blame, prior PR context, in-code comment guidance.

## Outcome

Initial static review had no issues above the configured threshold (**>= 80**).  
After runtime validation using the PR branch REPL (`/tmp/jsonlogic2sql-pr34`), two issues are now confirmed with high confidence.

## REPL Runtime Validation (PR branch)

Test setup:
- Built REPL from PR branch worktree: `/tmp/jsonlogic2sql-pr34`
- Command pattern used: start REPL -> enable `:params` -> run expression -> inspect SQL + Params output

### Confirmed Issue A: Custom `startsWith` breaks in parameterized mode
- **Confidence:** 100
- **File:** `cmd/repl/main.go` (custom operator registration path) + parameterized custom-arg flow in parser
- **Repro input (REPL):**
  - `:params`
  - `{"startsWith": [{"var": "name"}, "Al"]}`
- **Observed output:**
  - `SQL: WHERE name LIKE '@p1%'`
  - `Params: [{p1: "Al"}]`
- **Why this is a bug:** Placeholder is quoted into the LIKE literal, so DB treats `'@p1%'` as a string constant, not a bind placeholder with suffixing semantics.
- **Expected shape:** Placeholder should remain a bind token in SQL expression assembly (for example string concatenation around placeholder, not quote-wrapping the token itself).

### Confirmed Issue B: Large integer precision loss in parameterized numeric path
- **Confidence:** 100
- **File:** `internal/operators/numeric.go` (parameterized numeric string conversion path)
- **Repro input (REPL):**
  - `:params`
  - `{"*": ["9223372036854775808", 2]}`
- **Observed output:**
  - `SQL: WHERE (@p1 * @p2)`
  - `Params: [{p1: 9.223372036854776e+18}, {p2: 2}]`
- **Why this is a bug:** The integer string is converted to floating-point scientific notation in params, losing exact integer fidelity.
- **Expected shape:** Preserve exact integer value semantics for large integer strings (as string/integer-safe representation), not rounded `float64`.

### Additional behavior checks (non-bug confirmations)
- BigQuery parameterized equality:
  - Input: `{"==": [{"var": "email"}, "alice@example.com"]}`
  - Output: `WHERE email = @p1` with params list as expected.
- ClickHouse placeholder style in REPL:
  - After `:dialect` -> `5` and `:params`, equality output used named placeholders (`@p1`) as documented.

## Candidate Findings (Below Threshold)

### 1) `PlaceholderQuestion` validation is not per-parameter
- **Confidence:** 75
- **File:** `internal/params/params.go`
- **Why flagged:** In `ValidatePlaceholderRefs`, `PlaceholderQuestion` (`?`) uses the same placeholder pattern for every param, so each loop iteration can match the same `?` token in SQL. This can miss under-referenced placeholders if `?` style is used.
- **Impact:** Latent correctness gap in the safety guard for `?` style (currently not default).

### 2) Potential precision loss for very large integer strings in parameterized numeric path
- **Confidence:** 75
- **File:** `internal/operators/numeric.go`
- **Why flagged:** If integer parsing falls back to float parsing for out-of-range integer strings, very large integers may round (precision loss).
- **Impact:** Possible semantic drift for large integer literals in numeric expressions.

### 3) Comment wording about “all user-originated literals” appears overstated
- **Confidence:** 50
- **File:** `internal/params/params.go`
- **Why flagged:** Some values (for example structural SQL tokens like boolean/null in specific paths) may remain inline by design, while comment wording implies all user-originated literals become placeholders.
- **Impact:** Documentation clarity issue; low runtime risk.

### 4) `NewParser` comment and behavior mismatch (`nil` config)
- **Confidence:** 50
- **File:** `internal/parser/parser.go`
- **Why flagged:** Comment states config must not be nil, but implementation handles nil by creating a default config.
- **Impact:** Contract/documentation mismatch; low runtime risk.

## Notes

- This document intentionally captures review output locally per request (no `gh` comment posting).
- Two previously borderline findings were re-validated via REPL and promoted to confirmed issues above.

## Fix Revalidation (REPL + Tests)

Revalidation performed on current branch `feat/parameterized-query-output` after the LIKE/operator fix.

### Verdict on CONCAT approach

The updated approach is valid for the generated SQL in this transpiler context:
- Parameterized LIKE operators now emit SQL like:
  - `WHERE name LIKE CONCAT(@p1, '%')`
  - `WHERE name LIKE CONCAT('%', @p1, '%')`
  - PostgreSQL/DuckDB variants: `CONCAT($1, '%')`, `CONCAT('%', $1, '%')`
- This keeps placeholders outside quoted string literals and preserves bind semantics.
- It directly fixes the earlier invalid output form (`LIKE '@p1%'`).

### REPL runtime checks (rerun)

1. **BigQuery**
   - Input: `{"startsWith": [{"var":"name"}, "Al"]}` with `:params`
   - Output: `WHERE name LIKE CONCAT(@p1, '%')`
2. **PostgreSQL**
   - Output: `WHERE name LIKE CONCAT($1, '%')`
3. **DuckDB**
   - Output: `WHERE name LIKE CONCAT($1, '%')`
4. **ClickHouse**
   - Output: `WHERE name LIKE CONCAT(@p1, '%')`
5. **contains + apostrophe**
   - Output: `WHERE name LIKE CONCAT('%', @p1, '%')`
   - Params include raw value, e.g. `p1: "O'Brien"` (safe binding preserved)

### Large integer precision re-check

Previous issue was also re-tested:
- Input: `{"*": ["9223372036854775808", 2]}` with `:params`
- Output params now preserve exact integer string:
  - `Params: [{p1: "9223372036854775808"}, {p2: 2}]`
- The earlier float/scientific-notation precision-loss behavior is no longer observed.

### Test suite results

- `go test ./cmd/repl` → pass
- `go test ./cmd/repl -run TestTranspileParameterized_LikeOperators -count=1 -v` → pass
- `go test ./...` → pass

### Updated conclusion

The fix appears correct based on runtime REPL output and passing tests.  
The specific concern that CONCAT-based SQL is invalid is **not reproduced** in the generated statements from this branch.
