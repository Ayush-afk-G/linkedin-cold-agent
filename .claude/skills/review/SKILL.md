---
name: review
description: Review code for bugs, edge cases, and improvements
---

When asked to review code:

1. Read the target files thoroughly
2. Check for:
   - Logic bugs and off-by-one errors
   - Unhandled edge cases (empty inputs, None values, API failures)
   - Security issues (exposed secrets, injection, unvalidated input)
   - Performance concerns (N+1 queries, unbounded loops, memory leaks)
   - Code style inconsistencies
3. For each issue found, report:
   - File and line number
   - What the issue is
   - Suggested fix
4. Do NOT auto-fix anything — only report findings
5. End with a severity summary: critical / warning / suggestion counts
