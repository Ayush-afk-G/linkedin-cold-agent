import logging
import time

import google.generativeai as genai

from config import RATE_LIMIT_DELAY
from models import Lead

logger = logging.getLogger(__name__)

SPECTATR_CONTEXT = """
Spectatr.ai products:
- PULSE: real-time fan sentiment analytics
- AXIS: multi-platform data aggregation
- JORDY AI: AI-powered insights assistant
- BRAND GAUGE: sponsorship effectiveness measurement
"""

PROMPT_TEMPLATE = """
You are a sales development rep at Spectatr.ai writing a personalized LinkedIn outreach message.

About Spectatr.ai:{context}

Lead details:
- Name: {first_name} {last_name}
- Title: {title}
- Company: {company}
- Industry: {industry}
- Location: {location}

Write a short, personalized LinkedIn connection request message (max 300 characters) for this lead.
- Be specific to their role and company
- Mention one relevant Spectatr.ai product that fits their context
- Do NOT use generic phrases like "I came across your profile"
- Sound human, not salesy
- Do not include a subject line or greeting header — just the message body

Message:
""".strip()


def generate_message(lead: Lead, api_key: str) -> str:
    genai.configure(api_key=api_key)
    from config import GEMINI_MODEL
    model = genai.GenerativeModel(GEMINI_MODEL)
    prompt = PROMPT_TEMPLATE.format(
        context=SPECTATR_CONTEXT,
        first_name=lead.first_name,
        last_name=lead.last_name,
        title=lead.title,
        company=lead.company,
        industry=lead.industry,
        location=lead.location,
    )
    try:
        response = model.generate_content(prompt)
        message = response.text.strip()
        logger.info("Generated message for %s at %s", lead.first_name, lead.company)
        return message
    except Exception as exc:
        logger.error("Gemini generation failed for %s: %s", lead.linkedin_url, exc)
        return "(generation_failed)"
    finally:
        time.sleep(RATE_LIMIT_DELAY)
