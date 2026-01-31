#!/usr/bin/env python3
"""
extractor.py - AI-powered listing extraction using Claude or OpenAI
"""

import os
import re
import json
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# Try Anthropic first, then OpenAI
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


EXTRACTION_PROMPT = '''Extract all real estate listings from this webpage content. Focus on waterfront properties, cottages, and homes in the Muskoka region (Lake Muskoka, Lake Joseph, Lake Rosseau, Lake of Bays, etc.).

For each listing found, extract:
- address: Full address or location description
- price: Listed price (keep original format like "$2,450,000")
- bedrooms: Number of bedrooms (e.g., "4" or "4+1")
- bathrooms: Number of bathrooms (e.g., "3" or "2.5")
- sqft: Square footage if available
- acreage: Lot size in acres if available (e.g., "2.5 acres")
- frontage: Water frontage in feet if available (e.g., "150 ft")
- garage: Garage info if available (e.g., "2-car", "detached", "boathouse")
- lake: Which lake (Lake Muskoka, Lake Joseph, Lake Rosseau, etc.)
- waterfront: true if waterfront property, false otherwise
- exclusive: true if marked as "exclusive", "off-market", "pocket listing", or "private listing"
- listing_url: URL to the specific listing if different from source
- description: Brief description of the property (max 200 chars) - include key features like style, views, amenities

Return a JSON object with a "listings" array. If no listings found, return {"listings": []}.
Only include actual property listings, not agent profiles or general content.

Webpage content:
'''

BLOG_EXTRACTION_PROMPT = '''Extract all blog posts or news articles from this webpage content.

For each post found, extract:
- title: Post/article title
- post_url: URL to the full article
- date: Publication date if available

Return a JSON object with a "posts" array. If no posts found, return {"posts": []}.
Only include actual blog posts/articles, not navigation links or other content.

Webpage content:
'''


class Extractor:
    def __init__(self, anthropic_key: str = None, openai_key: str = None):
        self.anthropic_key = anthropic_key or os.getenv('ANTHROPIC_API_KEY')
        self.openai_key = openai_key or os.getenv('OPENAI_API_KEY')
        self.client = None
        self.provider = None
        
        if self.anthropic_key and ANTHROPIC_AVAILABLE:
            self.client = anthropic.Anthropic(api_key=self.anthropic_key)
            self.provider = 'anthropic'
            logger.info("Using Anthropic Claude for extraction")
        elif self.openai_key and OPENAI_AVAILABLE:
            self.client = OpenAI(api_key=self.openai_key)
            self.provider = 'openai'
            logger.info("Using OpenAI for extraction")
        else:
            logger.warning("No AI provider available - extraction will use fallback methods")
    
    def extract_listings(self, html_text: str, source_url: str) -> List[Dict]:
        """Extract listings from page content using AI."""
        if not self.client:
            return self._fallback_extract_listings(html_text, source_url)
        
        # Truncate content to avoid token limits
        content = html_text[:80000]
        
        try:
            if self.provider == 'anthropic':
                return self._extract_with_claude(content, source_url, 'listings')
            else:
                return self._extract_with_openai(content, source_url, 'listings')
        except Exception as e:
            logger.error(f"AI extraction failed: {e}")
            return self._fallback_extract_listings(html_text, source_url)
    
    def extract_blog_posts(self, html_text: str, source_url: str) -> List[Dict]:
        """Extract blog posts from page content using AI."""
        if not self.client:
            return self._fallback_extract_blogs(html_text, source_url)
        
        content = html_text[:40000]
        
        try:
            if self.provider == 'anthropic':
                return self._extract_with_claude(content, source_url, 'blogs')
            else:
                return self._extract_with_openai(content, source_url, 'blogs')
        except Exception as e:
            logger.error(f"Blog extraction failed: {e}")
            return self._fallback_extract_blogs(html_text, source_url)
    
    def _extract_with_claude(self, content: str, source_url: str, mode: str) -> List[Dict]:
        """Use Claude for extraction."""
        prompt = EXTRACTION_PROMPT if mode == 'listings' else BLOG_EXTRACTION_PROMPT
        
        response = self.client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=4000,
            messages=[
                {"role": "user", "content": prompt + content}
            ]
        )
        
        # Parse JSON from response
        text = response.content[0].text
        return self._parse_json_response(text, source_url, mode)
    
    def _extract_with_openai(self, content: str, source_url: str, mode: str) -> List[Dict]:
        """Use OpenAI for extraction."""
        prompt = EXTRACTION_PROMPT if mode == 'listings' else BLOG_EXTRACTION_PROMPT
        
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Extract data and return valid JSON only."},
                {"role": "user", "content": prompt + content}
            ],
            temperature=0.1,
            max_tokens=4000
        )
        
        text = response.choices[0].message.content
        return self._parse_json_response(text, source_url, mode)
    
    def _parse_json_response(self, text: str, source_url: str, mode: str) -> List[Dict]:
        """Parse JSON from AI response."""
        try:
            # Try to find JSON in response
            json_match = re.search(r'\{[\s\S]*\}', text)
            if json_match:
                data = json.loads(json_match.group())
                items = data.get('listings' if mode == 'listings' else 'posts', [])
                
                # Add source_url to each item
                for item in items:
                    item['source_url'] = source_url
                
                return items
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error: {e}")
        
        return []
    
    def _fallback_extract_listings(self, html_text: str, source_url: str) -> List[Dict]:
        """Fallback extraction using regex patterns."""
        listings = []
        
        # Look for price patterns
        price_pattern = r'\$[\d,]+(?:,\d{3})*'
        prices = re.findall(price_pattern, html_text)
        
        # Look for lake mentions
        lake_pattern = r'Lake\s*(Muskoka|Joseph|Rosseau|of Bays|Skeleton|Peninsula)'
        lakes = re.findall(lake_pattern, html_text, re.I)
        
        # Look for exclusive keywords
        exclusive_pattern = r'\b(exclusive|off-market|pocket listing|private listing)\b'
        exclusive = bool(re.search(exclusive_pattern, html_text, re.I))
        
        # If we found both prices and lakes, create a basic listing
        if prices and lakes:
            listings.append({
                'address': None,
                'price': prices[0] if prices else None,
                'bedrooms': None,
                'bathrooms': None,
                'sqft': None,
                'lake': f"Lake {lakes[0]}" if lakes else None,
                'waterfront': True,
                'exclusive': exclusive,
                'source_url': source_url,
                'listing_url': source_url,
                'description': f"Found on {source_url}"
            })
        
        return listings
    
    def _fallback_extract_blogs(self, html_text: str, source_url: str) -> List[Dict]:
        """Fallback blog extraction - look for common blog patterns."""
        posts = []
        
        # Look for article/post patterns in URLs
        blog_url_pattern = r'href=["\']([^"\']*(?:blog|news|article|post)[^"\']*)["\']'
        matches = re.findall(blog_url_pattern, html_text, re.I)
        
        for url in matches[:10]:  # Limit to 10
            if url.startswith('/'):
                from urllib.parse import urljoin
                url = urljoin(source_url, url)
            
            posts.append({
                'title': 'Blog Post',
                'post_url': url,
                'date': None,
                'source_url': source_url
            })
        
        return posts


def extract_text_from_html(html: str) -> str:
    """Extract readable text from HTML."""
    from bs4 import BeautifulSoup
    
    soup = BeautifulSoup(html, 'lxml')
    
    # Remove script and style elements
    for element in soup(['script', 'style', 'nav', 'footer', 'header']):
        element.decompose()
    
    # Get text
    text = soup.get_text(separator=' ', strip=True)
    
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)
    
    return text


if __name__ == '__main__':
    # Test
    extractor = Extractor()
    print(f"Provider: {extractor.provider or 'fallback'}")
