#!/usr/bin/env python3
"""
db.py - SQLite database for MuskokaCottageFinder
Handles listings, URLs, and blog posts persistence.
"""

import sqlite3
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict

DB_PATH = Path(__file__).parent / "data" / "listings.db"


@dataclass
class Listing:
    id: str  # hash of address + source
    address: Optional[str]
    price: Optional[str]
    price_numeric: Optional[float]
    bedrooms: Optional[str]
    bathrooms: Optional[str]
    sqft: Optional[str]
    lake: Optional[str]
    waterfront: bool
    exclusive: bool
    source_url: str
    listing_url: Optional[str]
    description: Optional[str]
    first_seen: str
    last_seen: str
    status: str  # active, removed, price_change


@dataclass
class BlogPost:
    id: str  # hash of url
    source_url: str
    post_url: str
    title: str
    date: Optional[str]
    first_seen: str


def get_connection() -> sqlite3.Connection:
    """Get database connection, creating tables if needed."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    _init_tables(conn)
    return conn


def _init_tables(conn: sqlite3.Connection):
    """Create tables if they don't exist."""
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            name TEXT,
            category TEXT DEFAULT 'broker',
            active INTEGER DEFAULT 1,
            last_scraped TEXT,
            error_count INTEGER DEFAULT 0,
            last_error TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS listings (
            id TEXT PRIMARY KEY,
            address TEXT,
            price TEXT,
            price_numeric REAL,
            bedrooms TEXT,
            bathrooms TEXT,
            sqft TEXT,
            acreage TEXT,
            frontage TEXT,
            garage TEXT,
            lake TEXT,
            waterfront INTEGER DEFAULT 0,
            exclusive INTEGER DEFAULT 0,
            source_url TEXT,
            listing_url TEXT,
            description TEXT,
            first_seen TEXT,
            last_seen TEXT,
            status TEXT DEFAULT 'active',
            raw_data TEXT
        );
        
        CREATE TABLE IF NOT EXISTS blog_posts (
            id TEXT PRIMARY KEY,
            source_url TEXT,
            post_url TEXT,
            title TEXT,
            date TEXT,
            first_seen TEXT
        );
        
        CREATE TABLE IF NOT EXISTS scrape_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            completed_at TEXT,
            urls_processed INTEGER,
            listings_found INTEGER,
            new_listings INTEGER,
            errors INTEGER,
            status TEXT
        );
        
        CREATE INDEX IF NOT EXISTS idx_listings_lake ON listings(lake);
        CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);
        CREATE INDEX IF NOT EXISTS idx_listings_first_seen ON listings(first_seen);
    ''')
    conn.commit()


def generate_listing_id(address: str, source_url: str) -> str:
    """Generate unique ID for a listing based on address only (dedupe across sites)."""
    # Normalize address for deduplication across different sources
    addr = (address or '').lower().strip()
    # Remove common variations
    addr = addr.replace(',', '').replace('.', '').replace('-', ' ')
    addr = ' '.join(addr.split())  # Normalize whitespace
    
    if addr:
        # Use address only for ID - same property from different sites will match
        return hashlib.md5(addr.encode()).hexdigest()[:16]
    else:
        # No address - use source URL to keep separate
        return hashlib.md5(source_url.lower().encode()).hexdigest()[:16]


def generate_blog_id(post_url: str) -> str:
    """Generate unique ID for a blog post."""
    return hashlib.md5(post_url.lower().encode()).hexdigest()[:16]


# URL Management
def add_url(url: str, name: str = None, category: str = 'broker') -> bool:
    """Add a new URL to scrape."""
    conn = get_connection()
    try:
        conn.execute(
            'INSERT OR IGNORE INTO urls (url, name, category) VALUES (?, ?, ?)',
            (url.strip(), name, category)
        )
        conn.commit()
        return conn.total_changes > 0
    finally:
        conn.close()


def remove_url(url: str) -> bool:
    """Remove a URL (set inactive)."""
    conn = get_connection()
    try:
        conn.execute('UPDATE urls SET active = 0 WHERE url = ?', (url.strip(),))
        conn.commit()
        return conn.total_changes > 0
    finally:
        conn.close()


def get_active_urls() -> List[Dict]:
    """Get all active URLs to scrape."""
    conn = get_connection()
    try:
        rows = conn.execute(
            'SELECT * FROM urls WHERE active = 1 ORDER BY last_scraped ASC NULLS FIRST'
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def update_url_status(url: str, error: str = None):
    """Update URL after scraping."""
    conn = get_connection()
    try:
        if error:
            conn.execute('''
                UPDATE urls SET last_scraped = ?, error_count = error_count + 1, last_error = ?
                WHERE url = ?
            ''', (datetime.utcnow().isoformat(), error, url))
        else:
            conn.execute('''
                UPDATE urls SET last_scraped = ?, error_count = 0, last_error = NULL
                WHERE url = ?
            ''', (datetime.utcnow().isoformat(), url))
        conn.commit()
    finally:
        conn.close()


def import_urls_from_list(urls: List[str], category: str = 'broker') -> int:
    """Import multiple URLs."""
    conn = get_connection()
    try:
        added = 0
        for url in urls:
            url = url.strip()
            if url and url.startswith('http'):
                try:
                    conn.execute(
                        'INSERT OR IGNORE INTO urls (url, category) VALUES (?, ?)',
                        (url, category)
                    )
                    if conn.total_changes > 0:
                        added += 1
                except:
                    pass
        conn.commit()
        return added
    finally:
        conn.close()


# Listing Management
def upsert_listing(listing_data: Dict) -> tuple:
    """Insert or update a listing. Returns (listing_id, is_new, has_changes)."""
    conn = get_connection()
    try:
        listing_id = generate_listing_id(
            listing_data.get('address', ''),
            listing_data.get('source_url', '')
        )
        now = datetime.utcnow().isoformat()
        
        existing = conn.execute(
            'SELECT * FROM listings WHERE id = ?', (listing_id,)
        ).fetchone()
        
        price_numeric = None
        if listing_data.get('price'):
            try:
                price_str = str(listing_data['price']).replace('$', '').replace(',', '').strip()
                price_numeric = float(price_str)
            except:
                pass
        
        if existing:
            # Update existing
            old_price = existing['price_numeric']
            has_changes = old_price != price_numeric if old_price and price_numeric else False
            status = 'price_change' if has_changes else 'active'
            
            conn.execute('''
                UPDATE listings SET
                    price = ?, price_numeric = ?, bedrooms = ?, bathrooms = ?,
                    sqft = ?, acreage = ?, frontage = ?, garage = ?,
                    lake = ?, waterfront = ?, exclusive = ?,
                    listing_url = ?, description = ?, last_seen = ?, status = ?,
                    raw_data = ?
                WHERE id = ?
            ''', (
                listing_data.get('price'),
                price_numeric,
                listing_data.get('bedrooms'),
                listing_data.get('bathrooms'),
                listing_data.get('sqft'),
                listing_data.get('acreage'),
                listing_data.get('frontage'),
                listing_data.get('garage'),
                listing_data.get('lake'),
                1 if listing_data.get('waterfront') else 0,
                1 if listing_data.get('exclusive') else 0,
                listing_data.get('listing_url'),
                listing_data.get('description'),
                now,
                status,
                json.dumps(listing_data),
                listing_id
            ))
            conn.commit()
            return listing_id, False, has_changes
        else:
            # Insert new
            conn.execute('''
                INSERT INTO listings (
                    id, address, price, price_numeric, bedrooms, bathrooms,
                    sqft, acreage, frontage, garage, lake, waterfront, exclusive, 
                    source_url, listing_url, description, first_seen, last_seen, status, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                listing_id,
                listing_data.get('address'),
                listing_data.get('price'),
                price_numeric,
                listing_data.get('bedrooms'),
                listing_data.get('bathrooms'),
                listing_data.get('sqft'),
                listing_data.get('acreage'),
                listing_data.get('frontage'),
                listing_data.get('garage'),
                listing_data.get('lake'),
                1 if listing_data.get('waterfront') else 0,
                1 if listing_data.get('exclusive') else 0,
                listing_data.get('source_url'),
                listing_data.get('listing_url'),
                listing_data.get('description'),
                now,
                now,
                'active',
                json.dumps(listing_data)
            ))
            conn.commit()
            return listing_id, True, False
    finally:
        conn.close()


def get_listings(status: str = None, lake: str = None, since: str = None) -> List[Dict]:
    """Get listings with optional filters."""
    conn = get_connection()
    try:
        query = 'SELECT * FROM listings WHERE 1=1'
        params = []
        
        if status:
            query += ' AND status = ?'
            params.append(status)
        if lake:
            query += ' AND lake LIKE ?'
            params.append(f'%{lake}%')
        if since:
            query += ' AND first_seen >= ?'
            params.append(since)
        
        query += ' ORDER BY first_seen DESC'
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_new_listings_since(since: str) -> List[Dict]:
    """Get listings first seen since a date."""
    return get_listings(since=since)


def mark_removed_listings(current_ids: set):
    """Mark listings not in current_ids as removed."""
    conn = get_connection()
    try:
        if not current_ids:
            return
        placeholders = ','.join('?' * len(current_ids))
        conn.execute(f'''
            UPDATE listings SET status = 'removed'
            WHERE id NOT IN ({placeholders}) AND status = 'active'
        ''', list(current_ids))
        conn.commit()
    finally:
        conn.close()


# Blog Post Management
def upsert_blog_post(post_data: Dict) -> tuple:
    """Insert blog post if new. Returns (post_id, is_new)."""
    conn = get_connection()
    try:
        post_id = generate_blog_id(post_data.get('post_url', ''))
        now = datetime.utcnow().isoformat()
        
        existing = conn.execute(
            'SELECT id FROM blog_posts WHERE id = ?', (post_id,)
        ).fetchone()
        
        if existing:
            return post_id, False
        
        conn.execute('''
            INSERT INTO blog_posts (id, source_url, post_url, title, date, first_seen)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            post_id,
            post_data.get('source_url'),
            post_data.get('post_url'),
            post_data.get('title'),
            post_data.get('date'),
            now
        ))
        conn.commit()
        return post_id, True
    finally:
        conn.close()


def get_new_blog_posts_since(since: str) -> List[Dict]:
    """Get blog posts first seen since a date."""
    conn = get_connection()
    try:
        rows = conn.execute(
            'SELECT * FROM blog_posts WHERE first_seen >= ? ORDER BY first_seen DESC',
            (since,)
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


# Scrape Run Tracking
def start_scrape_run() -> int:
    """Start a new scrape run and return its ID."""
    conn = get_connection()
    try:
        cursor = conn.execute(
            'INSERT INTO scrape_runs (started_at, status) VALUES (?, ?)',
            (datetime.utcnow().isoformat(), 'running')
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def complete_scrape_run(run_id: int, urls_processed: int, listings_found: int, 
                         new_listings: int, errors: int, status: str = 'completed'):
    """Complete a scrape run with stats."""
    conn = get_connection()
    try:
        conn.execute('''
            UPDATE scrape_runs SET
                completed_at = ?, urls_processed = ?, listings_found = ?,
                new_listings = ?, errors = ?, status = ?
            WHERE id = ?
        ''', (
            datetime.utcnow().isoformat(),
            urls_processed, listings_found, new_listings, errors, status,
            run_id
        ))
        conn.commit()
    finally:
        conn.close()


if __name__ == '__main__':
    # Test database
    conn = get_connection()
    print(f"Database initialized at {DB_PATH}")
    print(f"Active URLs: {len(get_active_urls())}")
    print(f"Total listings: {len(get_listings())}")
    conn.close()
