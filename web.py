#!/usr/bin/env python3
"""
web.py - Web interface for MuskokaCottageFinder
View listings, add notes, and get email notifications.
"""

import os
import sqlite3
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_from_directory
import subprocess

app = Flask(__name__, 
            template_folder='templates',
            static_folder='static')

DB_PATH = Path(__file__).parent / "data" / "listings.db"
PHOTOS_PATH = Path(__file__).parent / "data" / "photos"
DEFAULT_PHOTO = "/static/egan-team-logo.svg"

# Email settings (using gog CLI)
NOTIFY_EMAIL = "patrick.egan@royallepage.ca"


def get_db():
    """Get database connection."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def send_email_notification(listing_address: str, note: str, created_by: str):
    """Send email notification when a note is added."""
    try:
        subject = f"New Note on Listing: {listing_address}"
        body = f"""A new note has been added to a listing in MuskokaCottageFinder.

Listing: {listing_address}
Note: {note}
Added by: {created_by or 'Anonymous'}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}

View all listings: https://muskoka.patrickegan.com
"""
        # Use gog CLI to send email
        env = os.environ.copy()
        env['GOG_KEYRING_PASSWORD'] = 'clawdbot2026'
        env['GOG_ACCOUNT'] = 'patrick.egan@royallepage.ca'
        
        result = subprocess.run([
            'gog', 'gmail', 'send',
            '--to', NOTIFY_EMAIL,
            '--subject', subject,
            '--body', body
        ], env=env, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print(f"Email sent: {subject}")
        else:
            print(f"Email failed: {result.stderr}")
    except Exception as e:
        print(f"Email notification error: {e}")


def parse_price(price_str):
    """Extract numeric price from string like '$1,234,567'."""
    if not price_str:
        return None
    import re
    numbers = re.sub(r'[^\d]', '', str(price_str))
    return int(numbers) if numbers else None


@app.route('/')
def index():
    """Main listing view."""
    conn = get_db()
    
    # Get filter parameters
    lake = request.args.get('lake', '')
    status = request.args.get('status', '')
    exclusive_only = request.args.get('exclusive', '')
    sort_by = request.args.get('sort', 'newest')  # Default: newest first
    
    query = '''
        SELECT l.*, 
               (SELECT COUNT(*) FROM listing_notes WHERE listing_id = l.id) as note_count
        FROM listings l
        WHERE l.status != 'removed'
    '''
    params = []
    
    if lake:
        query += ' AND l.lake LIKE ?'
        params.append(f'%{lake}%')
    if status:
        query += ' AND l.status = ?'
        params.append(status)
    if exclusive_only:
        query += ' AND l.exclusive = 1'
    
    # Sort options (SQL-based where possible)
    sort_orders = {
        'newest': 'l.first_seen DESC',
        'oldest': 'l.first_seen ASC',
        'beds_high': 'CAST(l.bedrooms AS INTEGER) DESC',
        'beds_low': 'CAST(l.bedrooms AS INTEGER) ASC',
    }
    
    # Price sorting needs Python (price is stored as string like "$1,234,567")
    if sort_by in ('price_low', 'price_high'):
        query += ' ORDER BY l.first_seen DESC'  # Fallback order
    else:
        query += f' ORDER BY {sort_orders.get(sort_by, "l.first_seen DESC")}'
    
    listings = conn.execute(query, params).fetchall()
    listings = [dict(l) for l in listings]
    
    # Sort by price in Python (since price is stored as formatted string)
    if sort_by == 'price_low':
        listings.sort(key=lambda x: parse_price(x.get('price')) or float('inf'))
    elif sort_by == 'price_high':
        listings.sort(key=lambda x: parse_price(x.get('price')) or 0, reverse=True)
    
    # Get unique lakes for filter
    lakes = conn.execute(
        'SELECT DISTINCT lake FROM listings WHERE lake IS NOT NULL ORDER BY lake'
    ).fetchall()
    lakes = [l['lake'] for l in lakes]
    
    conn.close()
    
    return render_template('listings.html', 
                          listings=listings, 
                          lakes=lakes,
                          default_photo=DEFAULT_PHOTO,
                          current_lake=lake,
                          current_status=status,
                          exclusive_only=exclusive_only,
                          current_sort=sort_by)


@app.route('/listing/<listing_id>')
def listing_detail(listing_id):
    """Single listing detail view."""
    conn = get_db()
    
    listing = conn.execute(
        'SELECT * FROM listings WHERE id = ?', (listing_id,)
    ).fetchone()
    
    if not listing:
        return "Listing not found", 404
    
    listing = dict(listing)
    
    # Get notes
    notes = conn.execute(
        'SELECT * FROM listing_notes WHERE listing_id = ? ORDER BY created_at DESC',
        (listing_id,)
    ).fetchall()
    notes = [dict(n) for n in notes]
    
    conn.close()
    
    return render_template('listing_detail.html', 
                          listing=listing, 
                          notes=notes,
                          default_photo=DEFAULT_PHOTO)


@app.route('/api/notes', methods=['POST'])
def add_note():
    """Add a note to a listing."""
    data = request.json
    listing_id = data.get('listing_id')
    note = data.get('note', '').strip()
    created_by = data.get('created_by', '').strip() or 'Anonymous'
    
    if not listing_id or not note:
        return jsonify({'error': 'Missing listing_id or note'}), 400
    
    conn = get_db()
    
    # Get listing address for email
    listing = conn.execute(
        'SELECT address FROM listings WHERE id = ?', (listing_id,)
    ).fetchone()
    
    if not listing:
        conn.close()
        return jsonify({'error': 'Listing not found'}), 404
    
    # Insert note
    conn.execute('''
        INSERT INTO listing_notes (listing_id, note, created_by, created_at)
        VALUES (?, ?, ?, ?)
    ''', (listing_id, note, created_by, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    
    # Send email notification
    send_email_notification(listing['address'], note, created_by)
    
    return jsonify({'success': True, 'message': 'Note added'})


@app.route('/api/notes/<listing_id>')
def get_notes(listing_id):
    """Get all notes for a listing."""
    conn = get_db()
    notes = conn.execute(
        'SELECT * FROM listing_notes WHERE listing_id = ? ORDER BY created_at DESC',
        (listing_id,)
    ).fetchall()
    conn.close()
    
    return jsonify([dict(n) for n in notes])


@app.route('/photos/<path:filename>')
def serve_photo(filename):
    """Serve listing photos."""
    return send_from_directory(PHOTOS_PATH, filename)


@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files."""
    static_path = Path(__file__).parent / 'static'
    return send_from_directory(static_path, filename)


if __name__ == '__main__':
    # Create templates and static directories
    (Path(__file__).parent / 'templates').mkdir(exist_ok=True)
    (Path(__file__).parent / 'static').mkdir(exist_ok=True)
    
    app.run(host='0.0.0.0', port=8081, debug=True)
