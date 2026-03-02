import os
from dotenv import load_dotenv

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

OUTPUT_HEADERS: list[str] = ["linkedin_url", "message", "status"]
