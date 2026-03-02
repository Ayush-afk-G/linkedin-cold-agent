import logging
import time

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import GOOGLE_SHEETS_CREDENTIALS_FILE, OUTPUT_HEADERS, OUTPUT_SHEET_NAME, RATE_LIMIT_DELAY
from models import Lead, OutputRow

logger = logging.getLogger(__name__)

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


def get_sheet_client() -> gspread.Client:
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_CREDENTIALS_FILE, SCOPES)
    return gspread.authorize(creds)


def read_leads(client: gspread.Client, spreadsheet_id: str) -> list[Lead]:
    sh = client.open_by_key(spreadsheet_id)
    from config import SOURCE_SHEET_NAME
    ws = sh.worksheet(SOURCE_SHEET_NAME)
    records = ws.get_all_records()
    leads: list[Lead] = []
    for row in records:
        url = str(row.get("linkedin_url") or row.get("LinkedIn URL") or "").strip()
        if not url:
            continue
        leads.append(
            Lead(
                linkedin_url=url,
                first_name=str(row.get("first_name") or row.get("First Name") or ""),
                last_name=str(row.get("last_name") or row.get("Last Name") or ""),
                company=str(row.get("company") or row.get("Company") or ""),
                title=str(row.get("title") or row.get("Title") or row.get("Job Title") or ""),
                industry=str(row.get("industry") or row.get("Industry") or ""),
                location=str(row.get("location") or row.get("Location") or ""),
                notes=str(row.get("notes") or row.get("Notes") or ""),
            )
        )
    logger.info("Read %d leads from sheet", len(leads))
    return leads


def ensure_output_tab(client: gspread.Client, spreadsheet_id: str) -> gspread.Worksheet:
    sh = client.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(OUTPUT_SHEET_NAME)
        logger.info("Found existing tab '%s'", OUTPUT_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=OUTPUT_SHEET_NAME, rows=1000, cols=len(OUTPUT_HEADERS))
        ws.append_row(OUTPUT_HEADERS)
        logger.info("Created new tab '%s' with headers", OUTPUT_SHEET_NAME)
    return ws


def get_existing_urls(ws: gspread.Worksheet) -> set[str]:
    col_a = ws.col_values(1)
    normalized = {_normalize_url(u) for u in col_a[1:] if u}  # skip header
    logger.info("Found %d existing URLs in output tab", len(normalized))
    return normalized


def append_batch(ws: gspread.Worksheet, rows: list[OutputRow]) -> None:
    if not rows:
        return
    data = [[r.linkedin_url, r.message, r.status] for r in rows]
    try:
        ws.append_rows(data, value_input_option="RAW")
        logger.info("Appended %d rows to sheet", len(rows))
    except gspread.exceptions.APIError as exc:
        logger.error("Sheet append failed: %s", exc)
        raise
    finally:
        time.sleep(RATE_LIMIT_DELAY)


def _normalize_url(url: str) -> str:
    return url.strip().lower().rstrip("/") + "/"
