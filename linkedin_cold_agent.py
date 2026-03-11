#!/usr/bin/env python3
"""LinkedIn Cold Agent — orchestrates the full pipeline."""

import logging
import os
import sys
import math
import time
from dataclasses import dataclass, field
from itertools import islice
from typing import Any, Iterator

from google import genai
from google.genai import types as genai_types
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
GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
HEYREACH_API_KEY: str = os.getenv("HEYREACH_API_KEY", "")
HEYREACH_LIST_ID: str = os.getenv("HEYREACH_LIST_ID", "")
EXCLUSION_SPREADSHEET_ID: str = os.getenv("EXCLUSION_SPREADSHEET_ID", "")
EXCLUSION_SHEET_NAME: str = os.getenv("EXCLUSION_SHEET_NAME", "Reality")
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "5"))
RATE_LIMIT_DELAY: float = float(os.getenv("RATE_LIMIT_DELAY", "2.0"))
MAX_LEADS: int = int(os.getenv("MAX_LEADS", "0"))  # 0 = no limit

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
Spectatr.ai — Sports Technology of the Year award-winning AI platform.

PRODUCTS:

PULSE — AI Highlights
- Auto-generates personalised match highlights from live or recorded footage in near real-time
- 9:16, 16:9, and 4:5 clips ready for Instagram, TikTok, YouTube Shorts within seconds of key moments
- Eliminates manual editing — content teams focus on storytelling, not production
- Best for: Leagues, broadcasters, and event orgs with content volume or turnaround problems
- Case study: HockeyOne (Australia, 2025) — 9,500+ AI clips, 10.6M views, 9x fan engagement growth,
  264% Facebook reach increase during global expansion season
- Case study: NSL Canada (2025) — 37K+ moments captured across 80 matches, 51.8M video views,
  2x Instagram follower growth in inaugural season
- Case study: ANOC / ISG Riyadh 2025 — 50,000+ clips across 20+ sports, 7.5M views,
  57 NOCs each receiving daily athlete highlights for the first time
- Case study: Table Tennis England (2025) — 250+ hours saved, 105% surge in total views,
  45% engagement uplift, uploads scaled 3x without extra editing workload

AXIS — Media Management
- AI-based auto-tagging to index, search, discover, and share digital assets in near real-time
- Searchable by player names, action types, match stages, and contextual tags
- Eliminates manual tagging across live and recorded content, enables multi-platform distribution
- Best for: Media teams, broadcast ops, content managers who need to manage and repurpose large archives
- Case study: Table Tennis England — unstructured video archive transformed into fully indexed,
  searchable media library; team scaled from ~1 to ~10 uploads/month without added editing workload

JORDY AI — AI Fan Agent
- Sports-native AI fan agent (not a generic LLM wrapper) — understands game nuance, player context, and match moments
- Pull model (reactive): answers fan questions on stats, standings, player deep-dives, match facts in real-time
- Push model (proactive): triggers personalised ticket offers, merchandise, gamification (polls/trivia),
  geo-gated content based on fan buying intent — pull drives engagement, push drives monetization
- Hyper-personalisation via fan persona, interaction history, team/player affinity, geography, engagement depth
- Fully white-labeled — deploys into the org's existing app or as a standalone experience
- Built-in revenue engine: fan intent mapping → AI logic → merchandise/brand content/ticket conversions
- Case study: FantasyAlarm (USA, Sep 2025) — +13% revenue uplift, 27.3% season-long user retention,
  10 queries/user/week
- Best for: Leagues, teams, and broadcasters with a large fanbase wanting to deepen engagement and monetize fans
""".strip()

# ---------------------------------------------------------------------------
# Gemini system prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """
You are a senior B2B outreach strategist for Spectatr.ai — a Sports Technology of the Year award-winning AI platform that helps sports organisations automate content workflows and maximise fan engagement.

{spectatr_context}

YOUR TASK:
1. Analyse the person — title, seniority, department
2. Analyse the company — type, size, geography. Assess whether it is a Tier 1 org: major professional leagues (NFL, NBA, MLB, Premier League, etc.), global broadcasters (ESPN, Sky Sports, DAZN, etc.), or organisations with millions of fans/viewers.
3. Match the single most relevant Spectatr.ai product to their role:
   - If the company is Tier 1, strongly default to JORDY AI — they have the fanbase to fully exploit its monetization engine. Only override to PULSE or AXIS if the person's role is explicitly content creation, production, or media archive management.
   - PULSE: content creation, social media, digital, broadcast, marketing roles — anyone responsible for producing or distributing match content
   - AXIS: media operations, archive management, content library roles — anyone managing large volumes of existing footage
   - JORDY AI: fan engagement, digital product, commercial, partnership, and general leadership roles — especially at Tier 1 orgs
4. Pick ONE personalisation hook (strongest signal first):
   - Pain point implied by their role + department + company size
   - Geography — reference the most relevant case study:
     * Multi-sport event orgs → ANOC / ISG Riyadh
     * Emerging or new leagues → NSL Canada
     * Domestic leagues expanding reach → HockeyOne
     * Smaller orgs or niche sports → Table Tennis England
     * Fan monetization → FantasyAlarm
   - New in role (< 6 months) — opportunity to make an early impact
5. Write the message:
   - 2–3 sentences MAXIMUM
   - Do NOT open with "I came across your profile", "Hope this finds you well", or any generic opener
   - Name ONE product and ONE specific outcome or metric
   - Naturally reference the most contextually relevant case study metric where it fits
   - End with a soft question — never "book a call" or "schedule a demo"
   - Never use: synergy, leverage, game-changer, revolutionary, innovative, cutting-edge
   - Sound like a human peer, not a marketing email
   - No bullet points, no subject line, no sign-off
   - Output only the message body — nothing else
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
    "department",
    "seniority",
    "industry",
    "employees",
    "country",
    "location",
    "linkedin_profile",
    "personalised_message",
    "score",
]

SKIPPED_SHEET_NAME: str = "Skipped"
SKIPPED_HEADERS: list[str] = [
    "company_name",
    "company_domain",
    "full_name",
    "first_name",
    "last_name",
    "job_title",
    "department",
    "seniority",
    "industry",
    "employees",
    "keywords",
    "country",
    "location",
    "linkedin_profile",
    "skip_reason",
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
    department: str
    seniority: str
    industry: str
    employees: str
    keywords: str
    country: str
    location: str
    linkedin_profile: str
    personalised_message: str = ""
    score: int = 0

    @classmethod
    def from_sheet_row(cls, row: dict[str, Any]) -> "Lead":
        """Construct a Lead from an Apollo.io-enriched gspread row dict."""
        def get(*keys: str) -> str:
            for k in keys:
                v = row.get(k) or ""
                if v:
                    return str(v).strip()
            return ""

        first_name = get("First Name", "first_name")
        last_name = get("Last Name", "last_name")
        country = get("Country", "country")
        location_parts = [get("City"), get("State"), country]
        location = ", ".join(p for p in location_parts if p)

        return cls(
            company_name=get("Company Name", "company_name"),
            company_domain=get("Website", "company_domain"),
            first_name=first_name,
            last_name=last_name,
            full_name=f"{first_name} {last_name}".strip(),
            job_title=get("Title", "job_title"),
            department=get("Departments", "department"),
            seniority=get("Seniority", "seniority"),
            industry=get("Industry", "industry"),
            employees=get("# Employees", "employees"),
            keywords=get("Keywords", "keywords"),
            country=country,
            location=location,
            linkedin_profile=get("Person Linkedin Url", "linkedin_profile"),
        )

    def to_sheet_row(self) -> dict[str, str]:
        """Return a field→value dict keyed by OUTPUT_HEADERS names."""
        return {
            "company_name": self.company_name,
            "company_domain": self.company_domain,
            "full_name": self.full_name,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "job_title": self.job_title,
            "department": self.department,
            "seniority": self.seniority,
            "industry": self.industry,
            "employees": self.employees,
            "country": self.country,
            "location": self.location,
            "linkedin_profile": self.linkedin_profile,
            "personalised_message": self.personalised_message,
            "score": str(self.score),
        }

    def to_skipped_row(self, skip_reason: str) -> dict[str, str]:
        """Return a field→value dict keyed by SKIPPED_HEADERS names."""
        return {
            "company_name": self.company_name,
            "company_domain": self.company_domain,
            "full_name": self.full_name,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "job_title": self.job_title,
            "department": self.department,
            "seniority": self.seniority,
            "industry": self.industry,
            "employees": self.employees,
            "keywords": self.keywords,
            "country": self.country,
            "location": self.location,
            "linkedin_profile": self.linkedin_profile,
            "skip_reason": skip_reason,
        }


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
    leads = [Lead.from_sheet_row(r) for r in records if r.get("Person Linkedin Url", "").strip()]
    logger.info("Read %d leads from '%s'", len(leads), SOURCE_SHEET_NAME)
    return leads


def ensure_output_tab(client: gspread.Client) -> gspread.Worksheet:
    """Return the output worksheet, creating it with headers if it does not exist.

    If the tab already exists but has outdated headers, row 1 is updated to
    match OUTPUT_HEADERS so that header-name-based writes always land in the
    right column.
    """
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(OUTPUT_SHEET_NAME)
        logger.info("Found existing output tab '%s'", OUTPUT_SHEET_NAME)
        existing_headers = ws.row_values(1)
        if existing_headers != OUTPUT_HEADERS:
            ws.update("A1", [OUTPUT_HEADERS])
            logger.info("Updated output tab headers to match OUTPUT_HEADERS")
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


def get_existing_skipped_urls(ws: gspread.Worksheet) -> set[str]:
    """Return the set of normalised LinkedIn URLs already present in the Skipped tab."""
    headers = ws.row_values(1)
    try:
        col_index = headers.index("linkedin_profile") + 1
    except ValueError:
        return set()
    raw_urls = ws.col_values(col_index)
    return {normalize_linkedin_url(u) for u in raw_urls[1:] if u}


def get_existing_linkedin_urls(ws: gspread.Worksheet) -> set[str]:
    """Return the set of normalised LinkedIn URLs already present in the output tab.

    Looks up the linkedin_profile column by header name so the position is
    not hard-coded and survives column reordering.
    """
    headers = ws.row_values(1)
    try:
        col_index = headers.index("linkedin_profile") + 1  # 1-indexed
    except ValueError:
        logger.warning("'linkedin_profile' header not found in output sheet — skipping dedup")
        return set()
    raw_urls = ws.col_values(col_index)
    existing = {normalize_linkedin_url(u) for u in raw_urls[1:] if u}  # skip header row
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
    new_leads = [lead for lead in leads if normalize_linkedin_url(lead.linkedin_profile) not in existing_urls]
    skipped = len(leads) - len(new_leads)
    if skipped:
        logger.info("Skipped %d already-processed lead(s)", skipped)
    logger.info("%d new lead(s) queued for processing", len(new_leads))
    return new_leads


def exclude_existing_clients(client: gspread.Client, leads: list[Lead]) -> list[Lead]:
    """Filter out leads whose company_domain matches an existing client domain.

    Reads the 'company' column from EXCLUSION_SHEET_NAME in EXCLUSION_SPREADSHEET_ID.
    Each value may have a leading '@' which is stripped to get a bare domain.
    Comparison is case-insensitive with whitespace stripped.

    Returns leads unchanged (with a warning) if EXCLUSION_SPREADSHEET_ID is not set.
    """
    if not EXCLUSION_SPREADSHEET_ID:
        logger.warning("EXCLUSION_SPREADSHEET_ID is not set — skipping client exclusion filter")
        return leads

    logger.info(
        "Reading exclusion list from '%s', tab '%s'",
        EXCLUSION_SPREADSHEET_ID, EXCLUSION_SHEET_NAME,
    )
    sh = client.open_by_key(EXCLUSION_SPREADSHEET_ID)
    ws = sh.worksheet(EXCLUSION_SHEET_NAME)
    records = ws.get_all_records()

    excluded_domains: set[str] = set()
    for row in records:
        val = row.get("company") or row.get("Company") or ""
        domain = str(val).strip().lstrip("@").lower()
        if domain:
            excluded_domains.add(domain)

    logger.info("Loaded %d excluded domain(s) from exclusion sheet", len(excluded_domains))

    filtered = [l for l in leads if l.company_domain.strip().lower() not in excluded_domains]
    excluded_count = len(leads) - len(filtered)
    if excluded_count:
        logger.info("Excluded %d lead(s) matching existing client domain(s)", excluded_count)
    return filtered


def ensure_skipped_tab(client: gspread.Client) -> gspread.Worksheet:
    """Return the Skipped worksheet, creating it with headers if it does not exist."""
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SKIPPED_SHEET_NAME)
        logger.info("Found existing skipped tab '%s'", SKIPPED_SHEET_NAME)
        existing_headers = ws.row_values(1)
        if existing_headers != SKIPPED_HEADERS:
            ws.update("A1", [SKIPPED_HEADERS])
            logger.info("Updated skipped tab headers to match SKIPPED_HEADERS")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SKIPPED_SHEET_NAME, rows=5000, cols=len(SKIPPED_HEADERS))
        ws.append_row(SKIPPED_HEADERS)
        logger.info("Created skipped tab '%s'", SKIPPED_SHEET_NAME)
    return ws


def append_skipped(ws: gspread.Worksheet, leads: list[Lead], skip_reason: str) -> None:
    """Append skipped leads to the Skipped tab with a reason column."""
    if not leads:
        return
    sheet_headers = ws.row_values(1)
    rows = [
        [lead.to_skipped_row(skip_reason).get(h, "") for h in sheet_headers]
        for lead in leads
    ]
    try:
        ws.append_rows(rows, value_input_option="RAW")
        logger.info("Appended %d lead(s) to '%s' (reason: %s)", len(leads), SKIPPED_SHEET_NAME, skip_reason)
    except gspread.exceptions.APIError as exc:
        logger.error("Skipped sheet append failed: %s", exc)
        raise
    finally:
        time.sleep(RATE_LIMIT_DELAY)


_KEYWORD_GATE_KEYWORDS: set[str] = {
    "sports", "soccer", "football", "basketball", "baseball", "hockey", "tennis",
    "cricket", "rugby", "golf", "athletics", "swimming", "cycling", "motorsport",
    "esports", "league", "federation", "association", "union", "confederation",
    "club", "broadcast", "broadcaster", "broadcasting", "media", "production",
    "studio", "streaming", "entertainment", "olympic", "noc",
}

_KEYWORD_GATE_INDUSTRIES: set[str] = {
    "sports", "media", "broadcasting", "entertainment", "recreation",
}


def filter_by_keyword_gate(leads: list[Lead]) -> tuple[list[Lead], list[Lead]]:
    """Filter leads by sports/media keyword and industry match.

    PASS if the Keywords field contains any target keyword AND the Industry field
    contains any target industry (case-insensitive). Returns (passed, skipped).
    """
    passed, skipped = [], []
    for lead in leads:
        kw = lead.keywords.lower()
        ind = lead.industry.lower()
        kw_match = any(k in kw for k in _KEYWORD_GATE_KEYWORDS)
        ind_match = any(i in ind for i in _KEYWORD_GATE_INDUSTRIES)
        if kw_match and ind_match:
            passed.append(lead)
        else:
            skipped.append(lead)
    logger.info(
        "%d lead(s) passed keyword gate, %d skipped",
        len(passed), len(skipped),
    )
    return passed, skipped


_TARGET_DEPARTMENTS: set[str] = {
    "content", "marketing", "digital", "innovation", "tech", "technology",
    "product", "strategy", "media", "communications", "brand", "growth",
    "c_suite",  # Apollo labels C-suite executives' dept as "c_suite"
}

_TARGET_SENIORITY: set[str] = {
    "vp", "vice president", "director", "head", "chief",
    "c-level", "c_suite", "owner", "founder", "president", "partner", "managing",
}


def filter_by_department_and_seniority(leads: list[Lead]) -> tuple[list[Lead], list[Lead]]:
    """Keep only leads whose department and seniority match target criteria.

    Both department AND seniority must match (case-insensitive substring check).
    Returns (passed, skipped).
    """
    passed, skipped = [], []
    for lead in leads:
        dept_match = any(t in lead.department.lower() for t in _TARGET_DEPARTMENTS)
        seniority_match = any(t in lead.seniority.lower() for t in _TARGET_SENIORITY)
        if dept_match and seniority_match:
            passed.append(lead)
        else:
            skipped.append(lead)
    logger.info(
        "%d lead(s) passed department/seniority filter, %d skipped",
        len(passed), len(skipped),
    )
    return passed, skipped


def score_lead(lead: Lead) -> int:
    """Score a lead 0–100 based on seniority, industry, company size, and department.

    Used for prioritisation only — does not filter any leads out.
    """
    points = 0
    s = lead.seniority.lower()
    if any(t in s for t in ("chief", "c-suite", "president", "owner", "founder")):
        points += 30
    elif any(t in s for t in ("vp", "vice president")):
        points += 25
    elif any(t in s for t in ("director", "managing")):
        points += 20
    elif "head" in s:
        points += 15
    elif any(t in s for t in ("manager", "senior")):
        points += 8
    else:
        points += 3

    ind = lead.industry.lower()
    if "sport" in ind:
        points += 25
    elif any(t in ind for t in ("media", "broadcast", "entertainment")):
        points += 20
    elif any(t in ind for t in ("recreation",)):
        points += 10

    try:
        emp = int("".join(c for c in lead.employees if c.isdigit()) or "0")
    except ValueError:
        emp = 0
    if emp >= 1000:
        points += 25
    elif emp >= 500:
        points += 20
    elif emp >= 200:
        points += 15
    elif emp >= 50:
        points += 8
    else:
        points += 3

    dept = lead.department.lower()
    if any(t in dept for t in ("marketing", "digital", "content", "fan", "social", "growth")):
        points += 20
    elif any(t in dept for t in ("media", "broadcast", "production")):
        points += 18
    elif any(t in dept for t in ("commercial", "partnership", "sponsorship")):
        points += 15
    elif any(t in dept for t in ("tech", "innovation", "product")):
        points += 10
    else:
        points += 3

    return min(points, 100)


def append_batch(ws: gspread.Worksheet, leads: list[Lead]) -> None:
    """Append a batch of processed leads to the output tab.

    Sleeps for RATE_LIMIT_DELAY after the write to respect the Sheets API quota.
    """
    if not leads:
        logger.debug("append_batch called with empty list — nothing to write")
        return
    sheet_headers = ws.row_values(1)
    rows = [
        [lead.to_sheet_row().get(h, "") for h in sheet_headers]
        for lead in leads
    ]
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

_gemini_client: genai.Client | None = None


def init_gemini() -> genai.Client:
    """Initialise and cache a Gemini REST client.

    Uses the new google-genai package (REST transport) to avoid gRPC credential
    issues that caused the deprecated google-generativeai package to hang on
    remote runners.
    """
    global _gemini_client
    if _gemini_client is not None:
        return _gemini_client
    logger.info("Initialising Gemini client (model: %s)", GEMINI_MODEL)
    _gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    logger.info("Gemini client ready")
    return _gemini_client


def _build_prompt(lead: Lead) -> str:
    """Compose the per-lead prompt that is sent to Gemini alongside the system prompt."""
    return (
        f"Write a LinkedIn outreach message for this lead:\n"
        f"Name: {lead.first_name} {lead.last_name}\n"
        f"Title: {lead.job_title}\n"
        f"Seniority: {lead.seniority}\n"
        f"Department: {lead.department}\n"
        f"Company: {lead.company_name}\n"
        f"Industry: {lead.industry}\n"
        f"Website: {lead.company_domain}\n"
        f"Country: {lead.country}\n"
        f"Location: {lead.location}\n"
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
    client = init_gemini()
    prompt = _build_prompt(lead)
    logger.debug(
        "Sending prompt to Gemini for %s %s (%s)",
        lead.first_name, lead.last_name, lead.linkedin_profile,
    )
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
            ),
        )
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
    """Raise SystemExit if any required env vars are missing or invalid."""
    missing = []
    if not SPREADSHEET_ID:
        missing.append("SPREADSHEET_ID")
    if not GEMINI_API_KEY:
        missing.append("GEMINI_API_KEY")
    if missing:
        for var in missing:
            logger.error("%s is not set — check your .env file", var)
        sys.exit(1)
    if HEYREACH_LIST_ID and not HEYREACH_LIST_ID.strip().isdigit():
        logger.error("HEYREACH_LIST_ID must be a numeric value, got: %r", HEYREACH_LIST_ID)
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
        "Config: BATCH_SIZE=%d  RATE_LIMIT_DELAY=%.1fs  model=%s  MAX_LEADS=%s",
        BATCH_SIZE, RATE_LIMIT_DELAY, GEMINI_MODEL, MAX_LEADS or "unlimited",
    )

    # 1. Validate config
    _validate_config()

    # 2. Read source leads
    client = get_sheets_client()
    all_leads = read_source_leads(client)
    if not all_leads:
        logger.info("Source tab is empty — nothing to do")
        return

    # 3. Ensure output + skipped tabs exist
    output_ws = ensure_output_tab(client)
    skipped_ws = ensure_skipped_tab(client)

    # 4. Deduplicate
    existing_urls = get_existing_linkedin_urls(output_ws)
    new_leads = deduplicate_leads(all_leads, existing_urls)
    if not new_leads:
        logger.info("All leads already processed — nothing to do")
        return

    # 4b. Exclude existing clients
    new_leads = exclude_existing_clients(client, new_leads)
    if not new_leads:
        logger.info("All remaining leads belong to existing clients — nothing to do")
        return

    # 4c. Streaming scan — filter and collect until MAX_LEADS qualified leads found.
    # Only rows actually examined contribute to the Skipped sheet. Each lead is
    # written to Skipped at most once across all runs (URL-based dedup).
    existing_skipped_urls = get_existing_skipped_urls(skipped_ws)
    qualified: list[Lead] = []
    kw_skipped: list[Lead] = []
    dept_skipped: list[Lead] = []
    limit: float = MAX_LEADS if MAX_LEADS > 0 else float("inf")

    for lead in new_leads:
        if len(qualified) >= limit:
            break

        passed_kw, _ = filter_by_keyword_gate([lead])
        if not passed_kw:
            norm = normalize_linkedin_url(lead.linkedin_profile)
            if norm not in existing_skipped_urls:
                kw_skipped.append(lead)
                existing_skipped_urls.add(norm)
            continue

        passed_dept, _ = filter_by_department_and_seniority([lead])
        if not passed_dept:
            norm = normalize_linkedin_url(lead.linkedin_profile)
            if norm not in existing_skipped_urls:
                dept_skipped.append(lead)
                existing_skipped_urls.add(norm)
            continue

        qualified.append(lead)

    append_skipped(skipped_ws, kw_skipped, "keyword_gate")
    append_skipped(skipped_ws, dept_skipped, "dept_seniority_filter")
    logger.info(
        "Streaming scan complete: %d qualified, %d kw-skipped, %d dept-skipped",
        len(qualified), len(kw_skipped), len(dept_skipped),
    )

    if not qualified:
        logger.info("No leads passed all filters — nothing to do")
        return

    new_leads = qualified

    # 4f. Score each lead
    for lead in new_leads:
        lead.score = score_lead(lead)

    # 5. Process in batches
    total_batches = math.ceil(len(new_leads) / BATCH_SIZE)
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
