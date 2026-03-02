#!/usr/bin/env python3
"""LinkedIn Cold Agent — orchestrates the full pipeline."""

import logging
import os
import sys
import time
from dataclasses import dataclass, field
from itertools import islice
from typing import Any, Iterator

import google.generativeai as genai
import gspread
import requests
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

GOOGLE_SHEETS_CREDENTIALS_FILE: str = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "credentials.json")
SPREADSHEET_ID: str = os.getenv("SPREADSHEET_ID", "")
SOURCE_SHEET_NAME: str = os.getenv("SOURCE_SHEET_NAME", "ClayData.csv")
OUTPUT_SHEET_NAME: str = os.getenv("OUTPUT_SHEET_NAME", "Leads_Personalized")
GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
HEYREACH_API_KEY: str = os.getenv("HEYREACH_API_KEY", "")
HEYREACH_LIST_ID: str = os.getenv("HEYREACH_LIST_ID", "")
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "5"))
RATE_LIMIT_DELAY: float = float(os.getenv("RATE_LIMIT_DELAY", "2.0"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Spectatr context
# ---------------------------------------------------------------------------

SPECTATR_CONTEXT = """
Spectatr.ai is a sports analytics platform with four core products:
- PULSE: real-time fan sentiment analytics — tracks how fans feel about teams, players, and events across social media and beyond.
- AXIS: multi-platform data aggregation — unifies data streams from ticketing, social, broadcast, and sponsorship into a single view.
- JORDY AI: AI-powered insights assistant — answers natural-language questions about fan behaviour and performance metrics instantly.
- BRAND GAUGE: sponsorship effectiveness measurement — quantifies the ROI of brand partnerships inside sports properties.
""".strip()

# ---------------------------------------------------------------------------
# Gemini system prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """
You are a sales development representative at Spectatr.ai writing personalised LinkedIn outreach messages.

{spectatr_context}

Rules:
- Write a single short message (max 300 characters including spaces).
- Address the lead by first name.
- Reference their specific role and/or company to show you did your research.
- Naturally mention exactly one Spectatr.ai product that is most relevant to their context.
- Do NOT use generic openers like "I came across your profile" or "Hope this finds you well".
- Sound like a human, not a marketing email. No bullet points, no subject line, no sign-off.
- Output only the message body — nothing else.
""".strip().format(spectatr_context=SPECTATR_CONTEXT)

# ---------------------------------------------------------------------------
# Output sheet headers
# ---------------------------------------------------------------------------

OUTPUT_HEADERS: list[str] = [
    "company_name",
    "company_domain",
    "full_name",
    "first_name",
    "last_name",
    "job_title",
    "location",
    "linkedin_profile",
    "personalised_message",
]

# ---------------------------------------------------------------------------
# Lead dataclass
# ---------------------------------------------------------------------------

@dataclass
class Lead:
    company_name: str
    company_domain: str
    full_name: str
    first_name: str
    last_name: str
    job_title: str
    location: str
    linkedin_profile: str
    personalised_message: str = ""

    @classmethod
    def from_sheet_row(cls, row: dict[str, Any]) -> "Lead":
        """Construct a Lead from a gspread get_all_records() row dict.

        Tries multiple common column-name variants so the sheet header casing
        doesn't need to be exact.
        """
        def get(*keys: str) -> str:
            for k in keys:
                v = row.get(k) or row.get(k.lower()) or row.get(k.replace("_", " ").title()) or ""
                if v:
                    return str(v).strip()
            return ""

        return cls(
            company_name=get("company_name", "Company Name", "Company"),
            company_domain=get("company_domain", "Company Domain", "Domain"),
            full_name=get("full_name", "Full Name", "Name"),
            first_name=get("first_name", "First Name"),
            last_name=get("last_name", "Last Name"),
            job_title=get("job_title", "Job Title", "Title"),
            location=get("location", "Location"),
            linkedin_profile=get("linkedin_profile", "LinkedIn Profile", "LinkedIn URL", "linkedin_url"),
        )

    def to_sheet_row(self) -> list[str]:
        """Return values in the same order as OUTPUT_HEADERS."""
        return [
            self.company_name,
            self.company_domain,
            self.full_name,
            self.first_name,
            self.last_name,
            self.job_title,
            self.location,
            self.linkedin_profile,
            self.personalised_message,
        ]


# ---------------------------------------------------------------------------
# Google Sheets helpers
# ---------------------------------------------------------------------------

_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


def get_sheets_client() -> gspread.Client:
    """Authenticate with Google Sheets using a service account JSON file."""
    logger.info("Authenticating with Google Sheets using '%s'", GOOGLE_SHEETS_CREDENTIALS_FILE)
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_CREDENTIALS_FILE, _SCOPES)
    client = gspread.authorize(creds)
    logger.info("Google Sheets client ready")
    return client


def read_source_leads(client: gspread.Client) -> list[Lead]:
    """Read all rows from the source tab and return them as Lead objects."""
    logger.info("Opening spreadsheet '%s', tab '%s'", SPREADSHEET_ID, SOURCE_SHEET_NAME)
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SOURCE_SHEET_NAME)
    records = ws.get_all_records()
    leads = [Lead.from_sheet_row(r) for r in records if r]
    logger.info("Read %d leads from '%s'", len(leads), SOURCE_SHEET_NAME)
    return leads


def ensure_output_tab(client: gspread.Client) -> gspread.Worksheet:
    """Return the output worksheet, creating it with headers if it does not exist."""
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(OUTPUT_SHEET_NAME)
        logger.info("Found existing output tab '%s'", OUTPUT_SHEET_NAME)
    except gspread.WorksheetNotFound:
        logger.info("Output tab '%s' not found — creating it", OUTPUT_SHEET_NAME)
        ws = sh.add_worksheet(title=OUTPUT_SHEET_NAME, rows=1000, cols=len(OUTPUT_HEADERS))
        ws.append_row(OUTPUT_HEADERS)
        logger.info("Created output tab '%s' with %d headers", OUTPUT_SHEET_NAME, len(OUTPUT_HEADERS))
    return ws


def normalize_linkedin_url(url: str) -> str:
    """Return a canonical form of a LinkedIn URL for reliable deduplication.

    Strips whitespace, lowercases, and ensures a single trailing slash.
    """
    return url.strip().lower().rstrip("/") + "/"


def get_existing_linkedin_urls(ws: gspread.Worksheet) -> set[str]:
    """Return the set of normalised LinkedIn URLs already present in the output tab.

    Reads column 8 (linkedin_profile) and skips the header row.
    """
    raw_urls = ws.col_values(8)  # column 8 = linkedin_profile
    existing = {normalize_linkedin_url(u) for u in raw_urls[1:] if u}
    logger.info(
        "Found %d already-processed LinkedIn URL(s) in '%s'",
        len(existing),
        OUTPUT_SHEET_NAME,
    )
    return existing


def deduplicate_leads(leads: list[Lead], existing_urls: set[str]) -> list[Lead]:
    """Filter out leads whose LinkedIn URL has already been processed.

    Logs the number of skipped and remaining leads.
    """
    new_leads = [l for l in leads if normalize_linkedin_url(l.linkedin_profile) not in existing_urls]
    skipped = len(leads) - len(new_leads)
    if skipped:
        logger.info("Skipped %d already-processed lead(s)", skipped)
    logger.info("%d new lead(s) queued for processing", len(new_leads))
    return new_leads


def append_batch(ws: gspread.Worksheet, leads: list[Lead]) -> None:
    """Append a batch of processed leads to the output tab.

    Sleeps for RATE_LIMIT_DELAY after the write to respect the Sheets API quota.
    """
    if not leads:
        logger.debug("append_batch called with empty list — nothing to write")
        return
    rows = [lead.to_sheet_row() for lead in leads]
    try:
        ws.append_rows(rows, value_input_option="RAW")
        logger.info("Appended %d row(s) to '%s'", len(rows), OUTPUT_SHEET_NAME)
    except gspread.exceptions.APIError as exc:
        logger.error("Sheet append failed: %s", exc)
        raise
    finally:
        time.sleep(RATE_LIMIT_DELAY)


# ---------------------------------------------------------------------------
# Gemini message generation
# ---------------------------------------------------------------------------

_gemini_model: genai.GenerativeModel | None = None


def init_gemini() -> genai.GenerativeModel:
    """Configure the Gemini client and return a reusable GenerativeModel instance.

    Configures the API key once and caches the model so it is not re-created
    on every call to generate_linkedin_message().
    """
    global _gemini_model
    if _gemini_model is not None:
        return _gemini_model
    logger.info("Initialising Gemini client (model: %s)", GEMINI_MODEL)
    genai.configure(api_key=GEMINI_API_KEY)
    _gemini_model = genai.GenerativeModel(
        model_name=GEMINI_MODEL,
        system_instruction=SYSTEM_PROMPT,
    )
    logger.info("Gemini client ready")
    return _gemini_model


def _build_prompt(lead: Lead) -> str:
    """Compose the per-lead prompt that is sent to Gemini alongside the system prompt."""
    return (
        f"Write a LinkedIn outreach message for this lead:\n"
        f"Name: {lead.first_name} {lead.last_name}\n"
        f"Title: {lead.job_title}\n"
        f"Company: {lead.company_name}\n"
        f"Domain: {lead.company_domain}\n"
        f"Location: {lead.location}\n"
        f"\n"
        f"Spectatr context:\n{SPECTATR_CONTEXT}"
    )


def _strip_quotes(text: str) -> str:
    """Remove surrounding quotation marks that the model sometimes adds."""
    stripped = text.strip()
    if len(stripped) >= 2 and stripped[0] in ('"', "\u201c") and stripped[-1] in ('"', "\u201d"):
        return stripped[1:-1].strip()
    return stripped


def generate_linkedin_message(lead: Lead) -> str:
    """Generate a personalised LinkedIn outreach message for a single lead.

    Builds the prompt from lead fields and the Spectatr context, calls Gemini,
    and strips any surrounding quotation marks from the response.

    Returns '(generation_failed)' if the API call raises any exception,
    and sleeps for RATE_LIMIT_DELAY regardless of success or failure.
    """
    model = init_gemini()
    prompt = _build_prompt(lead)
    logger.debug(
        "Sending prompt to Gemini for %s %s (%s)",
        lead.first_name, lead.last_name, lead.linkedin_profile,
    )
    try:
        response = model.generate_content(prompt)
        message = _strip_quotes(response.text)
        logger.info(
            "Message generated for %s %s at %s (%d chars)",
            lead.first_name, lead.last_name, lead.company_name, len(message),
        )
        return message
    except Exception as exc:
        logger.error(
            "Gemini generation failed for %s %s (%s): %s",
            lead.first_name, lead.last_name, lead.linkedin_profile, exc,
        )
        return "(generation_failed)"
    finally:
        time.sleep(RATE_LIMIT_DELAY)


# ---------------------------------------------------------------------------
# HeyReach push
# ---------------------------------------------------------------------------

_HEYREACH_BASE = "https://api.heyreach.io/api/public"


def _lead_to_heyreach_payload(lead: Lead) -> dict:
    """Convert a Lead into the HeyReach lead object expected by AddLeadsToListV2."""
    return {
        "profileUrl": lead.linkedin_profile,
        "firstName": lead.first_name,
        "lastName": lead.last_name,
        "companyName": lead.company_name,
        "position": lead.job_title,
        "location": lead.location,
        "customUserFields": [
            {"name": "personalised_message", "value": lead.personalised_message}
        ],
    }


def push_to_heyreach(leads: list[Lead]) -> None:
    """POST all leads to the HeyReach list at /api/public/list/AddLeadsToListV2.

    Each lead's personalised_message is sent as a custom field so HeyReach
    campaign steps can reference it via a variable.

    Skips silently with a warning log if HEYREACH_API_KEY or HEYREACH_LIST_ID
    is not configured. Logs success/failure individually per lead and prints a
    final summary of how many were pushed successfully.
    """
    if not HEYREACH_API_KEY:
        logger.warning("HEYREACH_API_KEY is not set — skipping HeyReach push")
        return
    if not HEYREACH_LIST_ID:
        logger.warning("HEYREACH_LIST_ID is not set — skipping HeyReach push")
        return
    if not leads:
        logger.info("No leads to push to HeyReach")
        return

    url = f"{_HEYREACH_BASE}/list/AddLeadsToListV2"
    headers = {"X-API-KEY": HEYREACH_API_KEY, "Content-Type": "application/json"}

    logger.info("Pushing %d lead(s) to HeyReach list %s", len(leads), HEYREACH_LIST_ID)

    success_count = 0
    for lead in leads:
        payload = {"listId": int(HEYREACH_LIST_ID), "leads": [_lead_to_heyreach_payload(lead)]}
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=15)
            resp.raise_for_status()
            logger.info(
                "Pushed %s %s (%s) to HeyReach",
                lead.first_name, lead.last_name, lead.linkedin_profile,
            )
            success_count += 1
        except requests.RequestException as exc:
            logger.error(
                "HeyReach push failed for %s %s (%s): %s",
                lead.first_name, lead.last_name, lead.linkedin_profile, exc,
            )
        finally:
            time.sleep(0.5)

    logger.info("HeyReach push complete: %d/%d succeeded", success_count, len(leads))


# ---------------------------------------------------------------------------
# Orchestration helpers
# ---------------------------------------------------------------------------

def _chunked(iterable: list, size: int) -> Iterator[list]:
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk


def _validate_config() -> None:
    """Raise SystemExit if any required env vars are missing."""
    missing = []
    if not SPREADSHEET_ID:
        missing.append("SPREADSHEET_ID")
    if not GEMINI_API_KEY:
        missing.append("GEMINI_API_KEY")
    if missing:
        for var in missing:
            logger.error("%s is not set — check your .env file", var)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    """Orchestrate the full LinkedIn Cold Agent pipeline.

    Steps
    -----
    1. Validate required config — exit early if anything is missing.
    2. Authenticate and read leads from the source Google Sheet tab.
    3. Ensure the output tab exists (create with headers if not).
    4. Deduplicate leads against already-processed LinkedIn URLs.
    5. Process leads in batches of BATCH_SIZE:
       a. Generate a personalised message for each lead via Gemini,
          honouring RATE_LIMIT_DELAY between calls.
       b. Append the completed batch to the output sheet immediately
          so progress is saved even if a later batch fails.
       c. Push the successful leads in the batch to HeyReach.
    6. Log a final summary: total processed, messages generated,
       generation failures, and HeyReach pushes.
    """
    logger.info("=== LinkedIn Cold Agent starting ===")
    logger.info(
        "Config: BATCH_SIZE=%d  RATE_LIMIT_DELAY=%.1fs  model=%s",
        BATCH_SIZE, RATE_LIMIT_DELAY, GEMINI_MODEL,
    )

    # 1. Validate config
    _validate_config()

    # 2. Read source leads
    client = get_sheets_client()
    all_leads = read_source_leads(client)
    if not all_leads:
        logger.info("Source tab is empty — nothing to do")
        return

    # 3. Ensure output tab exists
    output_ws = ensure_output_tab(client)

    # 4. Deduplicate
    existing_urls = get_existing_linkedin_urls(output_ws)
    new_leads = deduplicate_leads(all_leads, existing_urls)
    if not new_leads:
        logger.info("All leads already processed — nothing to do")
        return

    # 5. Process in batches
    total_batches = -(-len(new_leads) // BATCH_SIZE)  # ceiling division
    success_count = 0
    fail_count = 0

    for batch_num, batch in enumerate(_chunked(new_leads, BATCH_SIZE), start=1):
        logger.info("Batch %d/%d — %d lead(s)", batch_num, total_batches, len(batch))

        # 5a. Generate messages
        for lead in batch:
            lead.personalised_message = generate_linkedin_message(lead)

        # Split batch by generation outcome
        batch_success = [l for l in batch if l.personalised_message != "(generation_failed)"]
        batch_fail = [l for l in batch if l.personalised_message == "(generation_failed)"]
        success_count += len(batch_success)
        fail_count += len(batch_fail)

        if batch_fail:
            logger.warning(
                "Batch %d: %d lead(s) failed generation — they will still be written to the sheet",
                batch_num, len(batch_fail),
            )

        # 5b. Append full batch to sheet (failures recorded with sentinel value)
        append_batch(output_ws, batch)

        # 5c. Push only successful leads to HeyReach
        push_to_heyreach(batch_success)

    # 6. Final summary
    logger.info(
        "=== Pipeline complete — %d lead(s) processed: %d message(s) generated, "
        "%d generation failure(s) ===",
        success_count + fail_count, success_count, fail_count,
    )


if __name__ == "__main__":
    run()
