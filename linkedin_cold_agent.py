#!/usr/bin/env python3
"""LinkedIn Cold Agent — orchestrates the full pipeline."""

import logging
import os
import re
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

---

PRODUCTS (match strictly to role — do not mix):

PULSE — AI Highlights
Auto-generates match highlight clips (9:16, 16:9, 4:5) for social in near real-time. Eliminates manual editing.
→ Metrics: HockeyOne (9,500+ clips, 10.6M views, 9x fan engagement), NSL Canada (37K moments, 51.8M views, 2x Instagram growth), Table Tennis England (250+ hours saved, 3x upload volume, 0 extra headcount)
→ Use for: content, social, digital, broadcast, production roles

AXIS — AI Media Management
Auto-tags and indexes video archives by player, action, match stage. Makes footage searchable and distributable instantly.
→ Metrics: Table Tennis England (unstructured archive → fully searchable library, scaled from ~1 to ~10 uploads/month)
→ Use for: media ops, archive, content library, broadcast operations roles

JORDY AI — AI Fan Agent
Sports-native AI that handles fan Q&A in real-time AND proactively pushes personalised ticket offers, merch, and gamification based on fan intent. White-labeled, deploys into existing apps.
→ Metrics: FantasyAlarm (+13% revenue uplift, 27.3% retention, 10 queries/user/week)
→ Use for: fan engagement, digital product, commercial, partnership roles AND any role at Tier 1 orgs (NFL, NBA, MLB, Premier League, global broadcasters) unless explicitly content/production

NEVER refer to products by name (PULSE, AXIS, JORDY AI) in any message.
Describe what they do in plain language only:
→ PULSE = "an AI that pulls match clips in real-time" or "automated highlight clipping" or "AI that generates clips the moment a key moment happens"
→ AXIS = "auto-indexing that makes your footage searchable instantly" or "AI that tags and organises your entire video archive automatically"
→ JORDY AI = "an AI fan agent" or "an AI that turns fan engagement into revenue" or "a white-labeled AI that activates fans in real time"

---

STEP 1 — PROCESS ENRICHMENT DATA FIRST (before any reasoning):

LINKEDIN POST CONTEXT:
- Read all posts before reasoning about anything else
- Identify: named frustrations, projects they're proud of, opinions they've taken a stance on, specific events or games they've referenced, tone of voice
- If a post reveals a current priority or pain → this becomes the #1 hook, overriding all other signals
- Never reference the post directly ("I saw your post on...")
- Absorb the insight, reflect it as shared understanding in the message
- If posts are older than 60 days, fewer than 2 exist, or content is purely personal → deprioritise, fall back to role/company signals

WEB RESEARCH CONTEXT:
- New season launching → content volume pain is imminent, not hypothetical
- Recent expansion (new markets, new broadcast deal) → scale problem is live
- Person is < 6 months in role → early impact motivation is high
- Recent loss of staff or restructure → lean team, doing more with less
- Specific recent match moment (viral play, big result, controversy) → anchor the message to something real they just lived through
- If no relevant signal found → do not fabricate, fall back to role pain

---

STEP 2 — REASON SILENTLY (do not include this reasoning in output):

A. Classify their function and assign product:
   - Content / production / social / digital → PULSE
   - Media ops / archive / content library → AXIS
   - Fan engagement / commercial / product / senior leadership → JORDY AI
   - Tier 1 org + non-content role → default JORDY AI

   LOW-FIT ROLES — flag before writing:
   - PR, communications, or press relations only → low-fit, do not write a fan engagement or content production message. If you must write, angle toward the speed of narrative control and highlight distribution for press use only. Mark message as LOW-FIT in your reasoning.
   - If role has zero connection to content, digital, or fan revenue → flag as DO NOT CONTACT and explain why instead of writing a message

B. Identify the single strongest hook using this priority order:
   1. Specific pain, frustration, or priority visible in LinkedIn posts (last 60 days) — this overrides everything else if present
   2. Very recent company/industry moment from web research (this week or last — a match result, launch, announcement, campaign)
   3. Company growth signal (new season, expansion, new broadcast deal, recent launch)
   4. New in role (< 6 months) — early impact motivation
   5. Pain clearly implied by role + company type + size

   The hook must be something THIS specific person would recognise immediately as their reality. If you cannot identify a specific hook, do not default to a generic one — write a shorter, more direct message anchored entirely to their role pain instead.

C. Select the most credible case study for THIS person:
   - Match on problem similarity, not geography
   - Only use a metric if it directly reflects their likely pain
   - Never force a case study if none fits naturally — omit it entirely
   - Never use more than ONE metric from ONE case study

D. Draft the closing question — apply these rules strictly:
   - Maximum 10 words — if it exceeds 10 words, cut it
   - Must describe a specific operational friction, not a broad theme
   - Must be answerable with yes, no, or one sentence
   - Must NOT contain: "curious if", "exploring", "wondering if", "would love to", "are you looking at", "have you considered"
   - Test: would a peer in their industry ask this over a beer? If no → rewrite it

---

STEP 3 — WRITE THE MESSAGE:

HARD RULES — violating any of these invalidates the message:
- 50 words maximum — count every word before outputting
- Every sentence must be under 20 words
- Never name a product (PULSE, AXIS, JORDY AI) — describe what it does
- Never open with a compliment, observation about their company, or generic opener
- Never use: synergy, leverage, game-changer, revolutionary, innovative, cutting-edge, exciting time, quick question, random thought, hope this finds you, I came across your profile, curious if you're exploring, would love to connect, I noticed, I saw, I came across
- No bullet points, no subject line, no greeting, no sign-off
- One product angle only — never mention two products
- One case study metric only — never stack multiple stats
- Closing question must be under 10 words — count them

TONE RULES:
- Write like a sharp industry peer, not a vendor
- The reader should not be able to tell if you're selling something until the case study line
- If the message could have been sent to 50 other people unchanged → rewrite it. It must feel written for this specific person.
- Absorb LinkedIn post insight silently — never cite or reference posts

MESSAGE CONSTRUCTION ORDER:
1. Open with their specific operational reality or a named friction (not a compliment, not a question)
2. Drop one case study metric that mirrors their pain exactly
3. Close with a under-10-word friction question

If recent web research reveals a very specific moment this week → open with that moment instead of a pain statement. Specificity beats insight every time.

---

SELF-CHECK BEFORE OUTPUTTING:

Before producing the final message, verify all of the following:
□ Word count is 50 or under
□ No sentence exceeds 20 words
□ No product name appears anywhere
□ Opening line is specific to this person — not sendable to anyone else
□ Case study metric directly mirrors their pain
□ Closing question is 10 words or under
□ Closing question contains none of the banned phrases
□ Message reads like a peer, not a vendor

If any box is unchecked → rewrite before outputting.

---

STEP 4 — SCORE THE MESSAGE:

After writing the message, score it against these 6 criteria.

SCORING CRITERIA:

1. SPECIFICITY (0-2 points)
   - 2: Could only be sent to this exact person
   - 1: Reasonably specific but sendable to 2-3 similar people with minor edits
   - 0: Generic — could be sent to anyone in this function

2. PAIN ACCURACY (0-2 points)
   - 2: Names a friction this person almost certainly lives with daily
   - 1: Plausible pain but inferred, not confirmed
   - 0: Assumed or speculative pain with no strong signal

3. QUESTION QUALITY (0-2 points)
   - 2: Under 10 words, describes a specific friction, yes/no or one-sentence answer, no banned phrases
   - 1: Passable but slightly long or slightly generic
   - 0: Over 10 words, uses banned phrases, or invites no real response

4. TONE (0-2 points)
   - 2: Reads like a sharp industry peer — no vendor signals, no product names, no hype words
   - 1: Mostly peer tone but one line reads like marketing copy
   - 0: Clearly a sales message — product name present, hype words, or generic opener

5. CASE STUDY FIT (0-1 point)
   - 1: Metric directly mirrors the pain named in the opener
   - 0: Metric forced, mismatched, or absent when it should be present

6. ROLE FIT (0-1 point)
   - 1: Person has plausible buying authority or strong influence over the relevant decision
   - 0: PR/comms only, wrong function, or no budget ownership

OUTPUT FORMAT — return the message first, then the scorecard immediately below in this exact format:

[Message body]

Score: X.0/10
Specificity: X/2
Pain Accuracy: X/2
Question Quality: X/2
Tone: X/2
Case Study Fit: X/1
Role Fit: X/1
Flag: [SEND / SEND WITH CAUTION / DO NOT SEND]

FLAG LOGIC:
- SEND: Total score 9.0 or above
- SEND WITH CAUTION: Total score 7.0–8.5
- DO NOT SEND: Total score 6.5 or below OR Role Fit scored 0
""".strip()

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
    "ai_score",
    "ai_flag",
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
    linkedin_posts: str = ""
    web_research: str = ""
    personalised_message: str = ""
    ai_score: str = ""
    ai_flag: str = ""
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
            linkedin_posts=get("LinkedIn Posts", "linkedin_posts"),
            web_research=get("Web Research", "web_research"),
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
            "ai_score": self.ai_score,
            "ai_flag": self.ai_flag,
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
    rows = ws.get_all_values()
    if not rows:
        return []
    headers = rows[0]
    records = [dict(zip(headers, row)) for row in rows[1:] if any(row)]
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
            ws.update(range_name="A1", values=[OUTPUT_HEADERS])
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


def _normalize_domain(raw: str) -> str:
    """Strip protocol, www, leading @, and trailing slash from a URL or bare domain."""
    d = raw.strip().lower().lstrip("@")
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    return d.rstrip("/")


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
    rows = ws.get_all_values()

    excluded_domains: set[str] = set()
    if rows:
        headers = rows[0]
        for raw_row in rows[1:]:
            if not any(raw_row):
                continue
            row = dict(zip(headers, raw_row))
            val = row.get("company") or row.get("Company") or ""
            domain = _normalize_domain(str(val))
            if domain:
                excluded_domains.add(domain)

    logger.info("Loaded %d excluded domain(s) from exclusion sheet", len(excluded_domains))

    filtered = [l for l in leads if _normalize_domain(l.company_domain) not in excluded_domains]
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
            ws.update(range_name="A1", values=[SKIPPED_HEADERS])
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
    if any(t in s for t in ("chief", "c-suite", "c_suite", "president", "owner", "founder")):
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
    linkedin_posts = lead.linkedin_posts.strip() or "None provided"
    web_research = lead.web_research.strip() or "None provided"
    return (
        f"Name: {lead.first_name} {lead.last_name}\n"
        f"Company: {lead.company_name}\n"
        f"Role/Title: {lead.job_title}\n"
        f"Seniority: {lead.seniority}\n"
        f"Department: {lead.department}\n"
        f"Industry: {lead.industry}\n"
        f"Employees: {lead.employees}\n"
        f"Country: {lead.country}\n"
        f"LinkedIn Posts: {linkedin_posts}\n"
        f"Web Research: {web_research}\n"
    )


def _strip_quotes(text: str) -> str:
    """Remove surrounding quotation marks that the model sometimes adds."""
    stripped = text.strip()
    if len(stripped) >= 2 and stripped[0] in ('"', "\u201c") and stripped[-1] in ('"', "\u201d"):
        return stripped[1:-1].strip()
    return stripped


def _parse_gemini_response(raw: str) -> tuple[str, str, str]:
    """Split Gemini output into (message_body, ai_score, ai_flag).

    Expected format:
        [Message body]

        Score: X.0/10
        ...
        Flag: SEND / SEND WITH CAUTION / DO NOT SEND

    Returns sentinel strings on parse failure.
    """
    # Split on the first "\nScore:" occurrence to separate message from scorecard
    score_idx = raw.find("\nScore:")
    if score_idx != -1:
        message_body = _strip_quotes(raw[:score_idx].strip())
        scorecard = raw[score_idx:]
    else:
        message_body = _strip_quotes(raw.strip())
        scorecard = ""

    score_match = re.search(r"Score:\s*([\d.]+/10)", scorecard)
    ai_score = score_match.group(1) if score_match else "(parse_failed)"

    # Handle both plain `Flag: SEND` and bracket-wrapped `Flag: [SEND]`
    flag_match = re.search(r"Flag:\s*\[?(SEND WITH CAUTION|DO NOT SEND|SEND)\]?", scorecard)
    ai_flag = flag_match.group(1) if flag_match else "(parse_failed)"

    return message_body, ai_score, ai_flag


def generate_linkedin_message(lead: Lead) -> str:
    """Generate a personalised LinkedIn outreach message for a single lead.

    Calls Gemini, parses the structured response (message + scorecard), and
    sets lead.ai_score and lead.ai_flag as a side effect.

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
        message, ai_score, ai_flag = _parse_gemini_response(response.text)
        lead.ai_score = ai_score
        lead.ai_flag = ai_flag
        logger.info(
            "Message generated for %s %s at %s (%d chars) — score=%s flag=%s",
            lead.first_name, lead.last_name, lead.company_name,
            len(message), ai_score, ai_flag,
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
