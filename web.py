#!/usr/bin/env python3
"""
web.py - Web dashboard for MuskokaCottageFinder
Password-protected listing viewer.
"""

import os
import json
import hashlib
import secrets
from functools import wraps
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, render_template_string, request, redirect, url_for, session, jsonify

# Local imports
from db import get_listings, get_new_blog_posts_since, get_active_urls, get_connection

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

# Load credentials from config
CONFIG_PATH = Path(__file__).parent / 'config.json'
LOCAL_CONFIG_PATH = Path(__file__).parent / 'config.local.json'

def load_web_config():
    config = {}
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            config = json.load(f)
    if LOCAL_CONFIG_PATH.exists():
        with open(LOCAL_CONFIG_PATH) as f:
            config.update(json.load(f))
    return config.get('web', {})

WEB_CONFIG = load_web_config()
USERNAME = WEB_CONFIG.get('username', 'admin')
# Hash the password for comparison
PASSWORD_HASH = hashlib.sha256(WEB_CONFIG.get('password', 'muskoka2026').encode()).hexdigest()


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '')
        password = request.form.get('password', '')
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        if username == USERNAME and password_hash == PASSWORD_HASH:
            session['logged_in'] = True
            session['username'] = username
            return redirect(url_for('dashboard'))
        else:
            error = 'Invalid credentials'
    
    return render_template_string(LOGIN_TEMPLATE, error=error)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/')
@login_required
def dashboard():
    listings = get_listings()
    
    # Deduplicate by normalized address
    seen_addresses = {}
    unique_listings = []
    for l in listings:
        addr = (l.get('address') or '').lower().strip()
        if addr and addr in seen_addresses:
            # Keep the one with more data or higher price
            continue
        if addr:
            seen_addresses[addr] = True
        unique_listings.append(l)
    
    # Sort by price descending
    unique_listings.sort(key=lambda x: x.get('price_numeric') or 0, reverse=True)
    
    # Get blog posts from last 7 days
    since = (datetime.utcnow() - timedelta(days=7)).isoformat()
    blogs = get_new_blog_posts_since(since)
    
    # Stats
    stats = {
        'total_listings': len(unique_listings),
        'total_urls': len(get_active_urls()),
        'exclusives': len([l for l in unique_listings if l.get('exclusive')]),
        'new_blogs': len(blogs)
    }
    
    return render_template_string(
        DASHBOARD_TEMPLATE, 
        listings=unique_listings, 
        blogs=blogs,
        stats=stats
    )


@app.route('/api/listings')
@login_required
def api_listings():
    listings = get_listings()
    return jsonify(listings)


LOGIN_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>MuskokaCottageFinder - Login</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
        body { background: linear-gradient(135deg, #1a5f7a 0%, #159895 100%); min-height: 100vh; display: flex; align-items: center; justify-content: center; margin: 0; }
        .login-box { background: white; padding: 40px; border-radius: 12px; box-shadow: 0 10px 40px rgba(0,0,0,0.2); width: 100%; max-width: 400px; }
        h1 { color: #1a5f7a; margin: 0 0 30px; font-size: 24px; text-align: center; }
        .subtitle { color: #666; text-align: center; margin-bottom: 30px; }
        input { width: 100%; padding: 14px; margin-bottom: 16px; border: 2px solid #e0e0e0; border-radius: 8px; font-size: 16px; }
        input:focus { outline: none; border-color: #159895; }
        button { width: 100%; padding: 14px; background: #1a5f7a; color: white; border: none; border-radius: 8px; font-size: 16px; cursor: pointer; font-weight: 600; }
        button:hover { background: #159895; }
        .error { background: #ffe0e0; color: #c00; padding: 12px; border-radius: 8px; margin-bottom: 20px; text-align: center; }
        .icon { font-size: 48px; text-align: center; margin-bottom: 20px; }
    </style>
</head>
<body>
    <div class="login-box">
        <div class="icon">🏠</div>
        <h1>MuskokaCottageFinder</h1>
        <p class="subtitle">Waterfront Listings Dashboard</p>
        {% if error %}<div class="error">{{ error }}</div>{% endif %}
        <form method="post">
            <input type="text" name="username" placeholder="Username" required autocomplete="username">
            <input type="password" name="password" placeholder="Password" required autocomplete="current-password">
            <button type="submit">Sign In</button>
        </form>
    </div>
</body>
</html>
'''

DASHBOARD_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>MuskokaCottageFinder - Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
        body { background: #f5f7fa; margin: 0; padding: 20px; }
        .header { background: linear-gradient(135deg, #1a5f7a 0%, #159895 100%); color: white; padding: 30px; border-radius: 12px; margin-bottom: 30px; }
        .header h1 { margin: 0 0 10px; }
        .header p { margin: 0; opacity: 0.9; }
        .logout { position: absolute; top: 30px; right: 30px; color: white; text-decoration: none; opacity: 0.8; }
        .logout:hover { opacity: 1; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat { background: white; padding: 20px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); text-align: center; }
        .stat-value { font-size: 32px; font-weight: bold; color: #1a5f7a; }
        .stat-label { color: #666; margin-top: 5px; }
        .section { background: white; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-bottom: 30px; overflow: hidden; }
        .section-header { padding: 20px; border-bottom: 1px solid #eee; font-weight: 600; font-size: 18px; }
        .listing { display: grid; grid-template-columns: 1fr auto; gap: 20px; padding: 20px; border-bottom: 1px solid #f0f0f0; align-items: center; }
        .listing:last-child { border-bottom: none; }
        .listing:hover { background: #f8fafb; }
        .listing-address { font-weight: 600; color: #333; margin-bottom: 5px; }
        .listing-details { color: #666; font-size: 14px; }
        .listing-price { font-size: 20px; font-weight: bold; color: #1a5f7a; text-align: right; }
        .listing-lake { font-size: 12px; color: #888; text-align: right; }
        .tag { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: 500; margin-left: 8px; }
        .tag-exclusive { background: #fff3cd; color: #856404; }
        .tag-new { background: #d4edda; color: #155724; }
        .tag-waterfront { background: #cce5ff; color: #004085; }
        .blog { padding: 15px 20px; border-bottom: 1px solid #f0f0f0; }
        .blog:last-child { border-bottom: none; }
        .blog a { color: #1a5f7a; text-decoration: none; font-weight: 500; }
        .blog a:hover { text-decoration: underline; }
        .blog-source { color: #888; font-size: 13px; margin-top: 5px; }
        .empty { padding: 40px; text-align: center; color: #888; }
        @media (max-width: 600px) {
            .listing { grid-template-columns: 1fr; }
            .listing-price { text-align: left; margin-top: 10px; }
        }
    </style>
</head>
<body>
    <div style="position: relative;">
        <a href="/logout" class="logout">Logout</a>
        <div class="header">
            <h1>🏠 MuskokaCottageFinder</h1>
            <p>Waterfront Listings Dashboard</p>
        </div>
    </div>
    
    <div class="stats">
        <div class="stat">
            <div class="stat-value">{{ stats.total_listings }}</div>
            <div class="stat-label">Total Listings</div>
        </div>
        <div class="stat">
            <div class="stat-value">{{ stats.total_urls }}</div>
            <div class="stat-label">Sources Monitored</div>
        </div>
        <div class="stat">
            <div class="stat-value">{{ stats.exclusives }}</div>
            <div class="stat-label">Exclusives</div>
        </div>
        <div class="stat">
            <div class="stat-value">{{ stats.new_blogs }}</div>
            <div class="stat-label">New Blog Posts</div>
        </div>
    </div>
    
    <div class="section">
        <div class="section-header">📋 Listings ({{ listings|length }})</div>
        {% if listings %}
            {% for l in listings %}
            <div class="listing">
                <div>
                    <div class="listing-address">
                        {{ l.address or 'Address Not Available' }}
                        {% if l.exclusive %}<span class="tag tag-exclusive">EXCLUSIVE</span>{% endif %}
                        {% if l.waterfront %}<span class="tag tag-waterfront">Waterfront</span>{% endif %}
                    </div>
                    <div class="listing-details">
                        {% if l.bedrooms %}{{ l.bedrooms }} bed{% endif %}
                        {% if l.bathrooms %} · {{ l.bathrooms }} bath{% endif %}
                        {% if l.sqft %} · {{ l.sqft }} sqft{% endif %}
                        {% if l.description %}<br>{{ l.description[:100] }}{% endif %}
                    </div>
                </div>
                <div>
                    <div class="listing-price">{{ l.price or 'Price N/A' }}</div>
                    <div class="listing-lake">{{ l.lake or 'Muskoka Region' }}</div>
                </div>
            </div>
            {% endfor %}
        {% else %}
            <div class="empty">No listings found. Run a scan to populate.</div>
        {% endif %}
    </div>
    
    {% if blogs %}
    <div class="section">
        <div class="section-header">📝 Recent Blog Posts ({{ blogs|length }})</div>
        {% for b in blogs %}
        <div class="blog">
            <a href="{{ b.post_url }}" target="_blank">{{ b.title }}</a>
            <div class="blog-source">{{ b.source_url }}</div>
        </div>
        {% endfor %}
    </div>
    {% endif %}
    
    <p style="text-align: center; color: #888; margin-top: 40px;">
        Last updated: {{ now() }} · 
        <a href="https://github.com/apatrickegan/muskoka-cottage-finder" style="color: #1a5f7a;">GitHub</a>
    </p>
</body>
</html>
'''

@app.context_processor
def utility_processor():
    def now():
        return datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')
    return dict(now=now)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=5050)
    parser.add_argument('--host', default='127.0.0.1')
    args = parser.parse_args()
    
    print(f"Starting MuskokaCottageFinder Dashboard on {args.host}:{args.port}")
    print(f"Login: {USERNAME} / [password from config]")
    app.run(host=args.host, port=args.port, debug=False)
