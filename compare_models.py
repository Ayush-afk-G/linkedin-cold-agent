"""
compare_models.py — Build a side-by-side model comparison tab in Google Sheets.

Reads 3 Leads_Personalized tabs (Gemini 3, Sonnet 4.6, Opus 4.6), joins them by
LinkedIn URL, and writes a 'Model Comparison' tab with message/score/ai_score/ai_flag
for each model side by side.

Usage:
    cd linkedin-cold-agent && source venv/bin/activate
    python compare_models.py
"""

from __future__ import annotations

import logging
import os

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CREDENTIALS_FILE: str = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "credentials.json")
SPREADSHEET_ID: str = os.getenv("SPREADSHEET_ID", "")

SOURCE_TABS: list[tuple[str, str]] = [
    ("gemini3",  "gemini-3 Leads_Personalized"),
    ("sonnet",   "sonnet-4.6 Leads_Personalized"),
    ("opus",     "Leads_Personalized"),
]

OUTPUT_TAB: str = "Model Comparison"

OUTPUT_HEADERS: list[str] = [
    "full_name",
    "linkedin_profile",
    "gemini3_message",
    "gemini3_score",
    "gemini3_ai_score",
    "gemini3_ai_flag",
    "sonnet_message",
    "sonnet_score",
    "sonnet_ai_score",
    "sonnet_ai_flag",
    "opus_message",
    "opus_score",
    "opus_ai_score",
    "opus_ai_flag",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_sheets_client() -> gspread.Client:
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scopes)
    return gspread.authorize(creds)


def _normalize_url(url: str) -> str:
    """Lowercase, strip whitespace, ensure single trailing slash."""
    u = url.strip().lower()
    if u:
        u = u.rstrip("/") + "/"
    return u


def _read_tab(sh: gspread.Spreadsheet, tab_name: str) -> dict[str, dict[str, str]]:
    """Read a tab and return {normalised_linkedin_url: {col: value}} dict."""
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        log.warning("Tab '%s' not found — skipping", tab_name)
        return {}

    all_values = ws.get_all_values()
    if len(all_values) < 2:
        log.info("Tab '%s' has no data rows", tab_name)
        return {}

    headers = all_values[0]
    result: dict[str, dict[str, str]] = {}
    for row in all_values[1:]:
        row_dict = dict(zip(headers, row))
        raw_url = row_dict.get("linkedin_profile", "").strip()
        if not raw_url:
            continue
        norm = _normalize_url(raw_url)
        result[norm] = row_dict

    log.info("Read %d row(s) from '%s'", len(result), tab_name)
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    if not SPREADSHEET_ID:
        raise SystemExit("SPREADSHEET_ID not set in .env")

    client = _get_sheets_client()
    sh = client.open_by_key(SPREADSHEET_ID)

    # Read all 3 source tabs
    tab_data: dict[str, dict[str, dict[str, str]]] = {}
    for key, tab_name in SOURCE_TABS:
        tab_data[key] = _read_tab(sh, tab_name)

    # Union of all LinkedIn URLs
    all_urls: set[str] = set()
    for data in tab_data.values():
        all_urls.update(data.keys())

    log.info("Total unique LinkedIn URLs across all tabs: %d", len(all_urls))

    # Build output rows
    rows: list[list[str]] = []
    for norm_url in all_urls:
        # Resolve display name + raw URL (prefer opus > sonnet > gemini)
        row_ref = (
            tab_data["opus"].get(norm_url)
            or tab_data["sonnet"].get(norm_url)
            or tab_data["gemini3"].get(norm_url)
            or {}
        )
        full_name = row_ref.get("full_name", "")
        raw_url = row_ref.get("linkedin_profile", norm_url)

        def _fields(key: str) -> list[str]:
            d = tab_data[key].get(norm_url, {})
            return [
                d.get("personalised_message", ""),
                d.get("score", ""),
                d.get("ai_score", ""),
                d.get("ai_flag", ""),
            ]

        row = [full_name, raw_url] + _fields("gemini3") + _fields("sonnet") + _fields("opus")
        rows.append(row)

    # Sort A→Z by full_name
    rows.sort(key=lambda r: r[0].lower())

    # Ensure output tab exists (clear if present, create if not)
    try:
        out_ws = sh.worksheet(OUTPUT_TAB)
        out_ws.clear()
        log.info("Cleared existing '%s' tab", OUTPUT_TAB)
    except gspread.WorksheetNotFound:
        out_ws = sh.add_worksheet(title=OUTPUT_TAB, rows=len(rows) + 10, cols=len(OUTPUT_HEADERS))
        log.info("Created '%s' tab", OUTPUT_TAB)

    # Write headers + data in one call
    all_rows = [OUTPUT_HEADERS] + rows
    out_ws.update(range_name="A1", values=all_rows, value_input_option="RAW")
    log.info("Written %d row(s) to '%s'", len(rows), OUTPUT_TAB)


if __name__ == "__main__":
    run()
