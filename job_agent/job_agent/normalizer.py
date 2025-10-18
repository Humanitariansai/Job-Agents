# job_agent/normalize.py
from __future__ import annotations
import re
from typing import Dict, Optional, Tuple

LEVEL_PATTERNS = [
    (r"\b(intern|internship|co[- ]?op)\b", "intern"),
    (r"\b(entry[- ]?level|junior|jr\.?)\b", "junior"),
    (r"\b(senior|sr\.?)\b", "senior"),
    (r"\b(principal|staff|lead)\b", "principal"),
    (r"\b(manager|managing)\b", "manager"),
    (r"\b(director|head of)\b", "director"),
]

WORK_TYPE_PATTERNS = [
    (r"\bon[- ]?site\b", "onsite"),
    (r"\bhybrid\b", "hybrid"),
    (r"\bremote\b", "remote"),
]

EMPLOYMENT_PATTERNS = [
    (r"\bfull[- ]?time\b", "full-time"),
    (r"\bpart[- ]?time\b", "part-time"),
    (r"\b(contract|contractor|temporary|temp|fixed[- ]?term)\b", "contract"),
]

def infer_level(text: str) -> Optional[str]:
    t = text.lower()
    for pat, lvl in LEVEL_PATTERNS:
        if re.search(pat, t):
            return lvl
    return None

def infer_work_type(text: str, location_text: Optional[str]) -> Optional[str]:
    blob = (" ".join([text or "", location_text or ""])).lower()
    for pat, val in WORK_TYPE_PATTERNS:
        if re.search(pat, blob):
            return val
    if location_text and "remote" in location_text.lower():
        return "remote"
    return None

def infer_employment(text: str) -> Optional[str]:
    t = text.lower()
    for pat, val in EMPLOYMENT_PATTERNS:
        if re.search(pat, t):
            return val
    return None

def split_location(loc: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
    if not loc:
        return None, None, None, None
    parts = [p.strip() for p in re.split(r"[;/|-]", loc)]
    first = parts[0] if parts else ""
    frags = [f.strip() for f in first.split(",")]
    city = frags[0] if frags else None
    state = frags[1] if len(frags) >= 2 else None
    country = frags[2] if len(frags) >= 3 else None
    remote = 1 if re.search(r"\bremote\b", loc, re.I) else None
    return city or None, state or None, country or None, remote

def normalize_fields(title: str, desc: str, location: Optional[str]) -> Dict[str, Optional[str]]:
    level = infer_level(title + " " + desc)
    work_type = infer_work_type(desc, location)
    employment = infer_employment(title + " " + desc)
    city, state, country, remote_flag = split_location(location)
    return {
        "role_level": level,
        "work_type": work_type,
        "employment_type": employment,
        "city": city,
        "state": state,
        "country": country,
        "remote": remote_flag,
    }
