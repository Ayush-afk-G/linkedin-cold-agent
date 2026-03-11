#!/usr/bin/env python3
"""Layer 1 unit tests for the LinkedIn Cold Agent pipeline.

Tests run purely in-memory — no API calls, no Sheets access.
Run with: python test_pipeline.py
"""

import sys
from linkedin_cold_agent import (
    Lead,
    deduplicate_leads,
    filter_by_department_and_seniority,
    filter_by_keyword_gate,
    normalize_linkedin_url,
    score_lead,
)

# ---------------------------------------------------------------------------
# Minimal test framework
# ---------------------------------------------------------------------------

_passed = 0
_failed = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global _passed, _failed
    if condition:
        print(f"  PASS  {name}")
        _passed += 1
    else:
        print(f"  FAIL  {name}{f'  ({detail})' if detail else ''}")
        _failed += 1


# ---------------------------------------------------------------------------
# Helper: build a Lead with defaults for fields not under test
# ---------------------------------------------------------------------------

def make_lead(
    first_name="Jane",
    last_name="Doe",
    job_title="Director of Marketing",
    company_name="Acme Sports",
    company_domain="acmesports.com",
    department="Marketing",
    seniority="Director",
    industry="Sports",
    employees="250",
    keywords="sports, league, broadcast",
    country="United Kingdom",
    location="London, England, United Kingdom",
    linkedin_profile="https://linkedin.com/in/janedoe/",
) -> Lead:
    return Lead(
        first_name=first_name,
        last_name=last_name,
        full_name=f"{first_name} {last_name}".strip(),
        job_title=job_title,
        company_name=company_name,
        company_domain=company_domain,
        department=department,
        seniority=seniority,
        industry=industry,
        employees=employees,
        keywords=keywords,
        country=country,
        location=location,
        linkedin_profile=linkedin_profile,
    )


# ---------------------------------------------------------------------------
# Test 1: Lead.from_sheet_row() — Apollo column mapping
# ---------------------------------------------------------------------------

print("\n── Test 1: Lead.from_sheet_row() ──")

apollo_row = {
    "First Name": "James",
    "Last Name": "Smith",
    "Title": "VP of Content",
    "Company Name": "Premier Sports",
    "Website": "premiersports.com",
    "Person Linkedin Url": "https://linkedin.com/in/jamessmith",
    "Departments": "Content",
    "Seniority": "vp",
    "City": "Manchester",
    "State": "England",
    "Country": "United Kingdom",
}

lead = Lead.from_sheet_row(apollo_row)

check("first_name maps from 'First Name'", lead.first_name == "James")
check("last_name maps from 'Last Name'", lead.last_name == "Smith")
check("full_name derived from first+last", lead.full_name == "James Smith")
check("job_title maps from 'Title'", lead.job_title == "VP of Content")
check("company_name maps from 'Company Name'", lead.company_name == "Premier Sports")
check("company_domain maps from 'Website'", lead.company_domain == "premiersports.com")
check("linkedin_profile maps from 'Person Linkedin Url'", lead.linkedin_profile == "https://linkedin.com/in/jamessmith")
check("department maps from 'Departments'", lead.department == "Content")
check("seniority maps from 'Seniority'", lead.seniority == "vp")
check("location joins City + State + Country", lead.location == "Manchester, England, United Kingdom")

# Missing City/State — only Country present
partial_row = {**apollo_row, "City": "", "State": ""}
lead_partial = Lead.from_sheet_row(partial_row)
check("location with missing City/State → only Country", lead_partial.location == "United Kingdom")

# Completely missing location fields
no_location_row = {**apollo_row, "City": "", "State": "", "Country": ""}
lead_no_loc = Lead.from_sheet_row(no_location_row)
check("location with all blanks → empty string", lead_no_loc.location == "")


# ---------------------------------------------------------------------------
# Test 2: filter_by_department_and_seniority()
# ---------------------------------------------------------------------------

print("\n── Test 2: filter_by_department_and_seniority() ──")

pass_cases = [
    make_lead(department="Marketing", seniority="Director"),
    make_lead(department="Digital", seniority="Head"),
    make_lead(department="Content", seniority="VP"),
    make_lead(department="Tech", seniority="Chief"),
    make_lead(department="Innovation", seniority="vp"),           # lowercase seniority
    make_lead(department="marketing", seniority="director"),      # lowercase both
    make_lead(department="Media & Communications", seniority="Managing Director"),
    make_lead(department="Brand & Growth", seniority="Vice President"),
]

fail_cases = [
    make_lead(department="Sales", seniority="Director"),          # dept no match
    make_lead(department="Finance", seniority="VP"),              # dept no match
    make_lead(department="Marketing", seniority="Analyst"),       # seniority no match
    make_lead(department="Marketing", seniority="Manager"),       # seniority no match
    make_lead(department="IT", seniority="Director"),             # dept no match
    make_lead(department="", seniority="Director"),               # empty dept
    make_lead(department="Marketing", seniority=""),              # empty seniority
    make_lead(department="", seniority=""),                       # both empty
]

result_pass, _ = filter_by_department_and_seniority(pass_cases)
result_fail, _ = filter_by_department_and_seniority(fail_cases)

check(f"all {len(pass_cases)} target leads pass", len(result_pass) == len(pass_cases),
      f"got {len(result_pass)}/{len(pass_cases)}")
check(f"all {len(fail_cases)} non-target leads blocked", len(result_fail) == 0,
      f"got {len(result_fail)} through")

# Mixed batch
mixed = pass_cases + fail_cases
result_mixed, _ = filter_by_department_and_seniority(mixed)
check("mixed batch: only pass_cases survive", len(result_mixed) == len(pass_cases),
      f"got {len(result_mixed)}, expected {len(pass_cases)}")


# ---------------------------------------------------------------------------
# Test 3: normalize_linkedin_url()
# ---------------------------------------------------------------------------

print("\n── Test 3: normalize_linkedin_url() ──")

check("lowercases URL", normalize_linkedin_url("https://LinkedIn.com/in/JaneDoe") == "https://linkedin.com/in/janedoe/")
check("adds trailing slash if missing", normalize_linkedin_url("https://linkedin.com/in/janedoe") == "https://linkedin.com/in/janedoe/")
check("doesn't double trailing slash", normalize_linkedin_url("https://linkedin.com/in/janedoe/") == "https://linkedin.com/in/janedoe/")
check("strips whitespace", normalize_linkedin_url("  https://linkedin.com/in/janedoe  ") == "https://linkedin.com/in/janedoe/")
check("strips multiple trailing slashes", normalize_linkedin_url("https://linkedin.com/in/janedoe///") == "https://linkedin.com/in/janedoe/")


# ---------------------------------------------------------------------------
# Test 4: deduplicate_leads()
# ---------------------------------------------------------------------------

print("\n── Test 4: deduplicate_leads() ──")

lead_a = make_lead(linkedin_profile="https://linkedin.com/in/janedoe/")
lead_b = make_lead(first_name="Bob", linkedin_profile="https://linkedin.com/in/bobsmith/")
lead_c = make_lead(first_name="Carol", linkedin_profile="https://linkedin.com/in/caroljones/")

existing = {
    normalize_linkedin_url("https://linkedin.com/in/janedoe/"),   # lead_a already processed
}

result = deduplicate_leads([lead_a, lead_b, lead_c], existing)
check("already-processed lead filtered out", lead_a not in result)
check("new leads kept", lead_b in result and lead_c in result)
check("correct count: 2 new leads", len(result) == 2)

# URL variant: uppercase, no trailing slash — should still dedup
lead_a_variant = make_lead(linkedin_profile="HTTPS://LINKEDIN.COM/IN/JANEDOE")
result_variant = deduplicate_leads([lead_a_variant, lead_b], existing)
check("uppercase/no-slash URL variant correctly deduped", lead_a_variant not in result_variant)

# Empty existing set — all leads pass through
result_empty = deduplicate_leads([lead_a, lead_b, lead_c], set())
check("empty existing set → all leads pass through", len(result_empty) == 3)


# ---------------------------------------------------------------------------
# Test 5: filter_by_keyword_gate()
# ---------------------------------------------------------------------------

print("\n── Test 5: filter_by_keyword_gate() ──")

kw_pass = [
    make_lead(keywords="sports, league, broadcast", industry="Sports"),
    make_lead(keywords="media production, broadcasting", industry="Media"),
    make_lead(keywords="football, streaming", industry="Entertainment"),
]
kw_fail = [
    make_lead(keywords="noc, olympic, athletics", industry="Other"),       # keyword match, no industry match
    make_lead(keywords="unrelated", industry="Sports"),                    # industry match, no keyword match
    make_lead(keywords="software, saas, b2b", industry="Technology"),
    make_lead(keywords="finance, banking", industry="Financial Services"),
    make_lead(keywords="", industry=""),
]

kw_result_pass, kw_result_skip = filter_by_keyword_gate(kw_pass)
kw_result_fail, kw_result_fail_skip = filter_by_keyword_gate(kw_fail)
kw_mixed_pass, kw_mixed_skip = filter_by_keyword_gate(kw_pass + kw_fail)

check(f"all {len(kw_pass)} keyword-matched leads pass", len(kw_result_pass) == len(kw_pass),
      f"got {len(kw_result_pass)}/{len(kw_pass)}")
check(f"all {len(kw_fail)} non-matched leads blocked", len(kw_result_fail) == 0,
      f"got {len(kw_result_fail)} through")
check("mixed batch: only kw_pass survive", len(kw_mixed_pass) == len(kw_pass),
      f"got {len(kw_mixed_pass)}, expected {len(kw_pass)}")
check("skipped count matches fail count", len(kw_mixed_skip) == len(kw_fail),
      f"got {len(kw_mixed_skip)}, expected {len(kw_fail)}")


# ---------------------------------------------------------------------------
# Test 6: score_lead()
# ---------------------------------------------------------------------------

print("\n── Test 6: score_lead() ──")

high_score = make_lead(seniority="VP", industry="Sports", employees="1500", department="Marketing")
mid_score = make_lead(seniority="Director", industry="Media", employees="300", department="Commercial")
low_score = make_lead(seniority="Manager", industry="Technology", employees="30", department="HR")

s_high = score_lead(high_score)
s_mid = score_lead(mid_score)
s_low = score_lead(low_score)

check("high score lead scores above 70", s_high > 70, f"got {s_high}")
check("mid score lead scores between 40 and 75", 40 <= s_mid <= 75, f"got {s_mid}")
check("low score lead scores below 40", s_low < 40, f"got {s_low}")
check("high > mid > low ordering", s_high > s_mid > s_low,
      f"high={s_high}, mid={s_mid}, low={s_low}")
check("score is capped at 100", score_lead(
    make_lead(seniority="Chief", industry="Sports", employees="5000", department="Marketing")
) <= 100, f"got {score_lead(make_lead(seniority='Chief', industry='Sports', employees='5000', department='Marketing'))}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

total = _passed + _failed
print(f"\n{'─' * 40}")
print(f"Results: {_passed}/{total} passed", end="")
if _failed:
    print(f"  ({_failed} FAILED)")
else:
    print("  ✓ All tests passed")
print()

sys.exit(0 if _failed == 0 else 1)
