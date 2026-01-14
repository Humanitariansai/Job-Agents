"""
Workday Jobs Agent - Company Configurations
"""

CVS = {
    "name": "CVS Health",
    "base_url": "https://cvshealth.wd1.myworkdayjobs.com/wday/cxs/cvshealth/CVS_Health_Careers",
    "categories": ["Technology", "Data and Analytics"],
    "rate_limit": {"requests_per_minute": 20, "concurrent": 2}
}

CENTENE = {
    "name": "Centene",
    "base_url": "https://centene.wd5.myworkdayjobs.com/wday/cxs/centene/Centene_External",
    "categories": ["Information Technology", "Data Analytics", "Executive"],
    "rate_limit": {"requests_per_minute": 20, "concurrent": 2}
}

COMPANIES = {"cvs": CVS, "centene": CENTENE}
COMPANY_NAMES = list(COMPANIES.keys())

def get_company(name):
    name = name.lower()
    if name not in COMPANIES:
        raise ValueError(f"Unknown company: {name}")
    return COMPANIES[name]

def list_companies():
    print("\n📋 Workday Companies:")
    for key, config in COMPANIES.items():
        print(f"  • {key}: {config['name']}")
