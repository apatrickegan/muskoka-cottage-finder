# MuskokaCottageFinder

AI-powered scraper that finds waterfront cottage and real estate listings in the Muskoka region.

## Features

- 🏠 **Listing Extraction** - Finds individual property listings from broker websites
- 🤖 **AI-Powered** - Uses Claude or GPT to extract structured data from pages
- 📊 **Deduplication** - Tracks listings across runs, identifies new vs existing
- 📝 **Blog Monitoring** - Detects new blog posts on broker sites
- 📈 **Excel Reports** - Generates organized reports with new listings highlighted
- 🔔 **Alerts** - Notifies when new listings appear (WhatsApp/Email)
- ⏰ **Scheduled Runs** - Automated weekly scans via cron

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Import your URLs
python urls.py import your_urls.xlsx

# Run a test (first 3 URLs)
python finder.py --test

# Full run
python finder.py

# Generate report only
python finder.py --report-only
```

## URL Management

```bash
# Add a single URL
python urls.py add "https://broker-website.com/listings"

# Remove a URL
python urls.py remove "https://old-site.com"

# List all active URLs
python urls.py list

# Import from Excel/CSV
python urls.py import urls.xlsx

# Show statistics
python urls.py stats
```

## Configuration

Edit `config.json`:

```json
{
  "anthropic_api_key": "sk-ant-...",
  "openai_api_key": "sk-...",
  "notification": {
    "whatsapp_number": "+14164002800",
    "email": "you@example.com"
  },
  "scraping": {
    "delay_seconds": 1.5,
    "timeout_seconds": 30
  },
  "filters": {
    "lakes": ["Lake Muskoka", "Lake Joseph", "Lake Rosseau"]
  }
}
```

## Output

Reports are saved to `output/report_YYYY-MM-DD.xlsx` with sheets:
- **All Listings** - Complete listing database
- **New This Week** - Listings first seen in the past 7 days
- **Exclusives** - Off-market and exclusive listings
- **New Blog Posts** - Recent blog posts from broker sites

## Scheduled Runs

The scraper is configured to run weekly via Clawdbot cron job.

## Data Storage

All data is stored in SQLite at `data/listings.db`:
- `urls` - Target websites to scrape
- `listings` - Property listings with full details
- `blog_posts` - Blog posts from broker sites
- `scrape_runs` - History of scrape runs

## License

Private - Patrick Egan / Egan Team Real Estate
