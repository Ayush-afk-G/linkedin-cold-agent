from dataclasses import dataclass, field


@dataclass
class Lead:
    linkedin_url: str
    first_name: str
    company: str
    title: str
    last_name: str = ""
    industry: str = ""
    location: str = ""
    notes: str = ""


@dataclass
class OutputRow:
    linkedin_url: str
    message: str          # "(generation_failed)" on failure
    status: str           # "success" | "failed" | "skipped"
