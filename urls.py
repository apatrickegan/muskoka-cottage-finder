#!/usr/bin/env python3
"""
urls.py - URL management for MuskokaCottageFinder
Add, remove, list, and import target URLs.
"""

import sys
import argparse
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from db import (
    add_url, remove_url, get_active_urls, import_urls_from_list, get_connection
)


def cmd_add(url: str, name: str = None, category: str = 'broker'):
    """Add a URL."""
    if add_url(url, name, category):
        print(f"Added: {url}")
    else:
        print(f"URL already exists: {url}")


def cmd_remove(url: str):
    """Remove a URL."""
    if remove_url(url):
        print(f"Removed: {url}")
    else:
        print(f"URL not found: {url}")


def cmd_list(show_all: bool = False):
    """List URLs."""
    conn = get_connection()
    try:
        if show_all:
            rows = conn.execute('SELECT * FROM urls ORDER BY created_at DESC').fetchall()
        else:
            rows = conn.execute('SELECT * FROM urls WHERE active = 1 ORDER BY created_at DESC').fetchall()
        
        if not rows:
            print("No URLs found.")
            return
        
        print(f"{'URL':<60} {'Category':<10} {'Active':<8} {'Errors':<6}")
        print("-" * 90)
        for row in rows:
            active = "Yes" if row['active'] else "No"
            print(f"{row['url'][:58]:<60} {row['category'] or '-':<10} {active:<8} {row['error_count']:<6}")
        
        print(f"\nTotal: {len(rows)} URLs")
    finally:
        conn.close()


def cmd_import(filepath: str, category: str = 'broker'):
    """Import URLs from file (txt, csv, xlsx)."""
    from pathlib import Path
    
    path = Path(filepath)
    if not path.exists():
        print(f"File not found: {filepath}")
        return
    
    urls = []
    ext = path.suffix.lower()
    
    if ext in ['.xlsx', '.xls']:
        try:
            import pandas as pd
            df = pd.read_excel(filepath)
            # Try to find URL column
            for col in df.columns:
                if 'url' in str(col).lower():
                    urls = df[col].dropna().astype(str).tolist()
                    break
            if not urls:
                # Take first column
                urls = df.iloc[:, 0].dropna().astype(str).tolist()
        except ImportError:
            print("pandas/openpyxl required for Excel import. Install with: pip install pandas openpyxl")
            return
    else:
        # Text file - one URL per line
        with open(filepath, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
    
    # Filter to valid URLs
    urls = [u for u in urls if u.startswith('http')]
    
    if not urls:
        print("No valid URLs found in file.")
        return
    
    count = import_urls_from_list(urls, category)
    print(f"Imported {count} new URLs (of {len(urls)} total in file)")


def cmd_stats():
    """Show URL statistics."""
    conn = get_connection()
    try:
        total = conn.execute('SELECT COUNT(*) FROM urls').fetchone()[0]
        active = conn.execute('SELECT COUNT(*) FROM urls WHERE active = 1').fetchone()[0]
        with_errors = conn.execute('SELECT COUNT(*) FROM urls WHERE error_count > 0').fetchone()[0]
        
        print(f"Total URLs: {total}")
        print(f"Active: {active}")
        print(f"With errors: {with_errors}")
        
        # By category
        categories = conn.execute(
            'SELECT category, COUNT(*) as cnt FROM urls WHERE active = 1 GROUP BY category'
        ).fetchall()
        if categories:
            print("\nBy category:")
            for row in categories:
                print(f"  {row['category'] or 'uncategorized'}: {row['cnt']}")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Manage MuskokaCottageFinder URLs')
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # add
    add_parser = subparsers.add_parser('add', help='Add a URL')
    add_parser.add_argument('url', help='URL to add')
    add_parser.add_argument('--name', help='Friendly name')
    add_parser.add_argument('--category', default='broker', help='Category (default: broker)')
    
    # remove
    rm_parser = subparsers.add_parser('remove', help='Remove a URL')
    rm_parser.add_argument('url', help='URL to remove')
    
    # list
    list_parser = subparsers.add_parser('list', help='List URLs')
    list_parser.add_argument('--all', action='store_true', help='Show inactive URLs too')
    
    # import
    import_parser = subparsers.add_parser('import', help='Import URLs from file')
    import_parser.add_argument('file', help='File path (txt, csv, xlsx)')
    import_parser.add_argument('--category', default='broker', help='Category for imported URLs')
    
    # stats
    subparsers.add_parser('stats', help='Show statistics')
    
    args = parser.parse_args()
    
    if args.command == 'add':
        cmd_add(args.url, args.name, args.category)
    elif args.command == 'remove':
        cmd_remove(args.url)
    elif args.command == 'list':
        cmd_list(args.all)
    elif args.command == 'import':
        cmd_import(args.file, args.category)
    elif args.command == 'stats':
        cmd_stats()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
