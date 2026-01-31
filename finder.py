#!/usr/bin/env python3
"""
finder.py - MuskokaCottageFinder main scraper
Finds waterfront cottage listings from broker websites.
"""

import os
import sys
import json
import time
import random
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
from bs4 import BeautifulSoup

# Local imports
from db import (
    get_connection, get_active_urls, update_url_status, 
    upsert_listing, upsert_blog_post, get_listings, get_new_listings_since,
    get_new_blog_posts_since, start_scrape_run, complete_scrape_run,
    import_urls_from_list, mark_removed_listings
)
from extractor import Extractor, extract_text_from_html

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).parent / 'data' / 'finder.log')
    ]
)
logger = logging.getLogger(__name__)

# Config
CONFIG_PATH = Path(__file__).parent / 'config.json'


def load_config() -> Dict:
    """Load configuration."""
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}


class MuskokaCottageFinder:
    def __init__(self, config: Dict = None):
        self.config = config or load_config()
        
        # Get API keys from config or environment
        anthropic_key = self.config.get('anthropic_api_key') or os.getenv('ANTHROPIC_API_KEY')
        openai_key = self.config.get('openai_api_key') or os.getenv('OPENAI_API_KEY')
        
        logger.info(f"API keys - Anthropic: {'set' if anthropic_key else 'not set'}, OpenAI: {'set' if openai_key else 'not set'}")
        
        self.extractor = Extractor(anthropic_key=anthropic_key, openai_key=openai_key)
        
        # Scraping settings
        scrape_config = self.config.get('scraping', {})
        self.delay = scrape_config.get('delay_seconds', 1.5)
        self.timeout = scrape_config.get('timeout_seconds', 30)
        self.max_retries = scrape_config.get('max_retries', 3)
        self.user_agents = scrape_config.get('user_agents', [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        ])
        
        # Stats
        self.stats = {
            'urls_processed': 0,
            'listings_found': 0,
            'new_listings': 0,
            'blog_posts_found': 0,
            'new_blog_posts': 0,
            'errors': 0
        }
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent."""
        return random.choice(self.user_agents)
    
    def fetch_url(self, url: str) -> Optional[str]:
        """Fetch URL content with retries."""
        headers = {
            'User-Agent': self.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        
        for attempt in range(self.max_retries):
            try:
                with httpx.Client(timeout=self.timeout, follow_redirects=True, max_redirects=5) as client:
                    response = client.get(url, headers=headers)
                    response.raise_for_status()
                    return response.text
            except httpx.TimeoutException:
                logger.warning(f"Timeout fetching {url} (attempt {attempt + 1})")
            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP {e.response.status_code} for {url}")
                if e.response.status_code == 403:
                    # Try with different user agent
                    headers['User-Agent'] = self.get_random_user_agent()
                elif e.response.status_code >= 500:
                    time.sleep(2)  # Server error, wait and retry
                else:
                    return None  # Client error, don't retry
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
            
            if attempt < self.max_retries - 1:
                time.sleep(1)
        
        return None
    
    def process_url(self, url_data: Dict, extract_blogs: bool = True) -> Dict:
        """Process a single URL and extract listings."""
        url = url_data['url']
        result = {
            'url': url,
            'success': False,
            'listings': [],
            'blog_posts': [],
            'error': None
        }
        
        logger.info(f"Processing: {url}")
        
        # Fetch page
        html = self.fetch_url(url)
        if not html:
            result['error'] = 'Failed to fetch'
            update_url_status(url, error=result['error'])
            return result
        
        # Extract text for AI
        text = extract_text_from_html(html)
        
        # Extract listings
        try:
            listings = self.extractor.extract_listings(text, url)
            result['listings'] = listings
            logger.info(f"  Found {len(listings)} listings")
        except Exception as e:
            logger.error(f"  Listing extraction error: {e}")
            result['error'] = str(e)
        
        # Extract blog posts if requested
        if extract_blogs:
            try:
                posts = self.extractor.extract_blog_posts(text, url)
                result['blog_posts'] = posts
                if posts:
                    logger.info(f"  Found {len(posts)} blog posts")
            except Exception as e:
                logger.error(f"  Blog extraction error: {e}")
        
        result['success'] = True
        update_url_status(url)
        return result
    
    def run(self, max_urls: int = None, extract_blogs: bool = True) -> Dict:
        """Run the scraper on all active URLs."""
        run_id = start_scrape_run()
        logger.info(f"Starting scrape run #{run_id}")
        
        urls = get_active_urls()
        if max_urls:
            urls = urls[:max_urls]
        
        logger.info(f"Processing {len(urls)} URLs")
        
        all_listing_ids = set()
        
        for i, url_data in enumerate(urls, 1):
            logger.info(f"[{i}/{len(urls)}] {url_data['url']}")
            
            result = self.process_url(url_data, extract_blogs=extract_blogs)
            self.stats['urls_processed'] += 1
            
            if result['error']:
                self.stats['errors'] += 1
            
            # Save listings
            for listing in result['listings']:
                listing_id, is_new, has_changes = upsert_listing(listing)
                all_listing_ids.add(listing_id)
                self.stats['listings_found'] += 1
                if is_new:
                    self.stats['new_listings'] += 1
                    logger.info(f"  NEW: {listing.get('address', 'Unknown')} - {listing.get('price', 'N/A')}")
            
            # Save blog posts
            for post in result['blog_posts']:
                post_id, is_new = upsert_blog_post(post)
                self.stats['blog_posts_found'] += 1
                if is_new:
                    self.stats['new_blog_posts'] += 1
            
            # Rate limiting
            time.sleep(self.delay + random.uniform(0, 0.5))
        
        # Mark removed listings
        # mark_removed_listings(all_listing_ids)  # Disabled for now - can cause false positives
        
        complete_scrape_run(
            run_id,
            self.stats['urls_processed'],
            self.stats['listings_found'],
            self.stats['new_listings'],
            self.stats['errors']
        )
        
        logger.info(f"Scrape complete: {self.stats}")
        return self.stats
    
    def generate_report(self, output_path: str = None, since_days: int = 7) -> str:
        """Generate Excel report of listings."""
        import pandas as pd
        
        if not output_path:
            output_path = Path(__file__).parent / 'output' / f'report_{datetime.now().strftime("%Y-%m-%d")}.xlsx'
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Get data
        all_listings = get_listings()
        since_date = (datetime.utcnow() - timedelta(days=since_days)).isoformat()
        new_listings = get_new_listings_since(since_date)
        exclusive_listings = [l for l in all_listings if l.get('exclusive')]
        new_blogs = get_new_blog_posts_since(since_date)
        
        # Create DataFrames
        columns = ['address', 'price', 'bedrooms', 'bathrooms', 'sqft', 'lake', 
                   'waterfront', 'exclusive', 'listing_url', 'first_seen', 'status']
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # All listings
            if all_listings:
                df_all = pd.DataFrame(all_listings)[columns]
                df_all.to_excel(writer, sheet_name='All Listings', index=False)
            
            # New listings
            if new_listings:
                df_new = pd.DataFrame(new_listings)[columns]
                df_new.to_excel(writer, sheet_name='New This Week', index=False)
            
            # Exclusives
            if exclusive_listings:
                df_excl = pd.DataFrame(exclusive_listings)[columns]
                df_excl.to_excel(writer, sheet_name='Exclusives', index=False)
            
            # Blog posts
            if new_blogs:
                df_blogs = pd.DataFrame(new_blogs)
                df_blogs.to_excel(writer, sheet_name='New Blog Posts', index=False)
        
        logger.info(f"Report saved to {output_path}")
        return str(output_path)


def main():
    parser = argparse.ArgumentParser(description='MuskokaCottageFinder - Find waterfront listings')
    parser.add_argument('--max-urls', type=int, help='Maximum URLs to process')
    parser.add_argument('--no-blogs', action='store_true', help='Skip blog extraction')
    parser.add_argument('--report-only', action='store_true', help='Generate report without scraping')
    parser.add_argument('--import-urls', type=str, help='Import URLs from file')
    parser.add_argument('--test', action='store_true', help='Test mode - process first 3 URLs only')
    args = parser.parse_args()
    
    finder = MuskokaCottageFinder()
    
    if args.import_urls:
        # Import URLs from file
        with open(args.import_urls) as f:
            if args.import_urls.endswith('.json'):
                urls = json.load(f)
            else:
                urls = [line.strip() for line in f if line.strip()]
        count = import_urls_from_list(urls)
        print(f"Imported {count} URLs")
        return
    
    if args.report_only:
        report_path = finder.generate_report()
        print(f"Report: {report_path}")
        return
    
    # Run scraper
    max_urls = 3 if args.test else args.max_urls
    stats = finder.run(max_urls=max_urls, extract_blogs=not args.no_blogs)
    
    # Generate report
    report_path = finder.generate_report()
    
    print(f"\n{'='*50}")
    print(f"Scrape Complete!")
    print(f"URLs processed: {stats['urls_processed']}")
    print(f"Listings found: {stats['listings_found']}")
    print(f"New listings: {stats['new_listings']}")
    print(f"New blog posts: {stats['new_blog_posts']}")
    print(f"Errors: {stats['errors']}")
    print(f"Report: {report_path}")


if __name__ == '__main__':
    main()
