# 💼 Workday Jobs Agent

**Automated job scraper for companies using Workday platform**

An ethical, educational job scraping tool that collects job postings from any company using Workday for their careers page.

## 🎯 What It Does

- Scrapes jobs from Workday-powered career sites
- Currently supports: CVS Health, Centene
- Easy to add any Workday company (10 lines of code!)
- Saves data as JSON and CSV
- Rate-limited and respectful

## 🚀 Quick Start
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Scrape CVS Health jobs (test with 10)
python examples/scrape_jobs.py --company cvs --limit 10

# 3. Scrape all configured companies
python examples/scrape_jobs.py --all

# 4. List available companies
python examples/scrape_jobs.py --list
```

## 📊 Results

Jobs are saved to `data/jobs/`:
- `cvs_20260114.json` - Full data
- `cvs_20260114.csv` - Open in Excel

## 🏢 Adding New Workday Companies

Edit `config/companies.py`:
```python
YOUR_COMPANY = {
    "name": "Your Company",
    "base_url": "https://company.wd1.myworkdayjobs.com/...",
    "categories": ["Technology", "Engineering"],
    "rate_limit": {"requests_per_minute": 20, "concurrent": 3}
}

COMPANIES = {
    "cvs": CVS,
    "centene": CENTENE,
    "yourcompany": YOUR_COMPANY
}
```

## 📁 Project Structure
```
workday-jobs-agent/
├── config/
│   └── companies.py       # Company configurations
├── scrapers/
│   └── workday.py         # Core Workday scraper
├── utils/
│   └── storage.py         # Save/load utilities
├── examples/
│   └── scrape_jobs.py     # Main script
├── data/
│   └── jobs/              # Scraped data goes here
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## 🛡️ Ethical Scraping

- ✅ Rate limiting (max 3 concurrent requests)
- ✅ Respectful delays between batches
- ✅ Clear user agent identification
- ✅ Only scrapes public job postings
- ✅ No personal data collection

## 🤝 Contributing

This is part of the **Humanitarians AI** volunteer initiative.

To add more Workday companies:
1. Find their Workday URL (usually `company.wd*.myworkdayjobs.com`)
2. Add configuration to `config/companies.py`
3. Test with `--limit 5`
4. Submit pull request

## 📜 License

MIT License - Free for educational and humanitarian use.

## 🙏 Acknowledgments

Built for [Humanitarians AI](https://humanitarians.ai) - Teaching ethical AI development.

**"Technology for Good, Education for All"** 🌍
