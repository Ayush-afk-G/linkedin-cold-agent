import logging
import time

import requests

from models import Lead

logger = logging.getLogger(__name__)

HEYREACH_BASE_URL = "https://api.heyreach.io/api/public"


def push_lead(lead: Lead, message: str, api_key: str, list_id: str) -> bool:
    url = f"{HEYREACH_BASE_URL}/lists/AddLeadsToList"
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }
    payload = {
        "listId": list_id,
        "leads": [
            {
                "linkedInUrl": lead.linkedin_url,
                "firstName": lead.first_name,
                "lastName": lead.last_name,
                "companyName": lead.company,
                "title": lead.title,
                "customVariables": [
                    {"name": "message", "value": message}
                ],
            }
        ],
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        logger.info("Pushed %s to HeyReach list %s", lead.linkedin_url, list_id)
        return True
    except requests.RequestException as exc:
        logger.error("HeyReach push failed for %s: %s", lead.linkedin_url, exc)
        return False
    finally:
        time.sleep(0.5)
