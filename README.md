# LinkedIn Cold Agent

Python CLI pipeline that reads leads from Google Sheets, generates personalised LinkedIn outreach messages with Gemini AI, writes results back to the sheet, and pushes leads to HeyReach.

## How it works

```
Google Sheets (leads) → Gemini AI (message generation) → Google Sheets (results) → HeyReach
```

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Google Sheets service account

1. Create a service account in Google Cloud Console and download the credentials JSON file.
2. Share **both** the leads spreadsheet and the exclusion spreadsheet (Core Email DB) with the service account email.

### 3. Environment variables

Copy `.env.example` to `.env` and fill in the values:

| Variable | Required | Default | Description |
|---|---|---|---|
| `GOOGLE_SHEETS_CREDENTIALS_FILE` | Yes | `credentials.json` | Path to service account JSON |
| `SPREADSHEET_ID` | Yes | — | Google Sheet ID containing raw leads |
| `SOURCE_SHEET_NAME` | No | `ClayData.csv` | Tab name with raw leads |
| `OUTPUT_SHEET_NAME` | No | `Leads_Personalized` | Tab name to write results |
| `GEMINI_API_KEY` | Yes | — | Google Gemini API key |
| `GEMINI_MODEL` | No | `gemini-2.0-flash` | Gemini model name |
| `HEYREACH_API_KEY` | No | — | HeyReach API key (omit to skip push) |
| `HEYREACH_LIST_ID` | No | — | HeyReach list ID |
| `EXCLUSION_SPREADSHEET_ID` | No | — | Sheet ID of Core Email DB (existing clients) |
| `EXCLUSION_SHEET_NAME` | No | `Reality` | Tab name in the exclusion sheet |
| `BATCH_SIZE` | No | `5` | Leads processed per batch |
| `RATE_LIMIT_DELAY` | No | `2.0` | Seconds between Gemini API calls |

## Running

```bash
# Test with a single lead
BATCH_SIZE=1 python linkedin_cold_agent.py

# Full run
python linkedin_cold_agent.py
```

## Pipeline steps

1. Validate required config — exits early if anything is missing
2. Read leads from `SOURCE_SHEET_NAME`
3. Ensure `OUTPUT_SHEET_NAME` tab exists (creates it with headers if not)
4. Deduplicate leads by LinkedIn URL against already-processed rows
5. Exclude existing clients matched against the Core Email DB
6. Generate personalised messages via Gemini in batches
7. Write results to `OUTPUT_SHEET_NAME` after each batch
8. Push successful leads to HeyReach (skipped if `HEYREACH_API_KEY` is not set)

## Testing messages without HeyReach

Leave `HEYREACH_API_KEY` unset in `.env`. The pipeline will still generate messages and write them to the Google Sheet — you can review the `Leads_Personalized` tab directly without triggering any HeyReach campaign.
