# Mistakes Log

Track mistakes Claude makes so they don't repeat. Add entries here when something goes wrong.
Reference this file in CLAUDE.md so Claude checks it.

## Format

### [Date] - Brief description
- **What happened**: describe the mistake
- **Root cause**: why it happened
- **Fix**: what was done to correct it
- **Prevention**: rule to add to CLAUDE.md or settings

## Entries

### 2026-03-11 — Adding new columns to OUTPUT_HEADERS without migrating existing data rows
- **What happened**: When new columns are added to OUTPUT_HEADERS and the header row is updated in-place via `ws.update("A1", [OUTPUT_HEADERS])`, existing data rows stay at their old column positions — headers shift but data does not, causing misalignment
- **Root cause**: In-place header update only rewrites row 1; it does not touch existing data rows
- **Rule**: Before updating headers, write a small temporary migration script that reads all existing rows, inserts empty cells at the new column positions, and rewrites the data. Then update the headers. Delete the script after running. Never rely on "append to end only" as a workaround — that just defers the problem.
- **Scope**: Global

### 2026-03-11 — gspread sheet writes must always use header-name mapping, never column position
- **What happened**: When new columns (ai_score, ai_flag) were added to OUTPUT_HEADERS, the concern was that positional writes would put data in wrong columns for existing rows
- **Root cause**: Writing rows as positional lists breaks when column order changes or new columns are added anywhere other than the end
- **Fix**: `append_batch()` already uses the correct pattern — reads `sheet_headers = ws.row_values(1)` then maps by name
- **Prevention**: Never construct gspread row data as `[val1, val2, val3]` in order. Always use `[row_dict.get(h, "") for h in sheet_headers]`
