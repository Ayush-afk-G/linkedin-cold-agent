# LinkedIn Cold Agent

## Project overview
Python CLI pipeline: read leads from Google Sheets → generate personalized LinkedIn messages with Gemini AI → write results back → push to HeyReach.

## Tech stack
- Python 3.11+, google-generativeai, gspread, oauth2client, python-dotenv, requests

## Code style
- Type hints everywhere
- Dataclasses for structured data
- logging over print
- One function per workflow step
- Handle API errors with try/except and fallback values

## Running
- pip install -r requirements.txt
- cp .env.example .env (fill in real keys)
- Test first: BATCH_SIZE=1 python linkedin_cold_agent.py
- Full run: python linkedin_cold_agent.py

## Exclusion list
The exclusion list (Core Email DB) filters out companies already in our sales pipeline. EXCLUSION_SPREADSHEET_ID is optional — if not set, the step is skipped with a warning.

## Common mistakes
- Check docs/mistakes.md before making changes
- gspread rate limit: 60 req/min. Use batch+delay pattern.
- Gemini rate limit: respect RATE_LIMIT_DELAY. Increase on 429 errors.
- LinkedIn URL dedup: always lowercase + trailing slash.
- NEVER read .env or credential files directly.

## Skills
- /commit-push-pr — commit, push, open PR
- /learn — explore code without changing anything
- /review — find bugs without auto-fixing

## Agents
- linkedin-message-reviewer — QA generated messages against outreach rules
