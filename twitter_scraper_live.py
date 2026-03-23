#!/usr/bin/env python3
"""
Twitter Alpha Scraper - Works with running Chrome
Uses copied cookies from logged-in Chrome session to authenticate in Playwright

Usage:
    python3 twitter_scraper_live.py --accounts --limit 20
    python3 twitter_scraper_live.py --accounts --limit 92  # All accounts
    python3 twitter_scraper_live.py --bookmarks
    python3 twitter_scraper_live.py --all --limit 50
"""

import asyncio
import json
import csv
import re
import sqlite3
import shutil
import tempfile
import os
from pathlib import Path
from datetime import datetime
from playwright.async_api import async_playwright
import argparse

# Paths
PROJECT_DIR = Path(__file__).resolve().parent.parent
LEDGER_DIR = PROJECT_DIR / "LEDGER"
ALPHA_STAGING = LEDGER_DIR / "ALPHA_STAGING.csv"
HIGH_SIGNAL_SOURCES = LEDGER_DIR / "HIGH_SIGNAL_SOURCES.csv"
OUTPUT_DIR = PROJECT_DIR / "AUTOMATIONS" / "twitter_scraper_output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Chrome paths
CHROME_USER_DATA = Path.home() / "Library/Application Support/Google/Chrome"
DEFAULT_PROFILE = CHROME_USER_DATA / "Default"

class TwitterScraperLive:
    def __init__(self):
        self.existing_urls = self.load_existing_urls()
        self.next_alpha_id = self.get_next_alpha_id()
        self.cookies = []

    def load_existing_urls(self):
        """Load existing source URLs to avoid duplicates"""
        urls = set()
        if ALPHA_STAGING.exists():
            with open(ALPHA_STAGING, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('source_url'):
                        # Normalize URL
                        url = row['source_url'].strip()
                        urls.add(url)
                        # Also add without https:// prefix variations
                        if 'x.com' in url or 'twitter.com' in url:
                            # Extract status ID for matching
                            match = re.search(r'/status/(\d+)', url)
                            if match:
                                urls.add(match.group(1))
        print(f"Loaded {len(urls)} existing URLs for deduplication")
        return urls

    def get_next_alpha_id(self):
        """Get next available ALPHA ID"""
        max_id = 0
        if ALPHA_STAGING.exists():
            with open(ALPHA_STAGING, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    alpha_id = row.get('alpha_id', '')
                    match = re.match(r'ALPHA(\d+)', alpha_id)
                    if match:
                        max_id = max(max_id, int(match.group(1)))
        return max_id + 1

    def extract_cookies_from_chrome(self):
        """Extract Twitter cookies from Chrome's SQLite database (read-only copy)"""
        print("Extracting cookies from Chrome...")

        cookies_file = DEFAULT_PROFILE / "Cookies"
        if not cookies_file.exists():
            print(f"Cookies file not found at {cookies_file}")
            return []

        # Copy to temp file (Chrome locks the original)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
            tmp_path = tmp.name

        try:
            shutil.copy2(cookies_file, tmp_path)

            conn = sqlite3.connect(tmp_path)
            cursor = conn.cursor()

            # Get Twitter/X cookies
            cursor.execute("""
                SELECT host_key, name, value, path, expires_utc, is_secure, is_httponly
                FROM cookies
                WHERE host_key LIKE '%twitter.com%' OR host_key LIKE '%x.com%'
            """)

            cookies = []
            for row in cursor.fetchall():
                host, name, value, path, expires, secure, httponly = row
                if value:  # Only include cookies with values
                    cookie = {
                        'name': name,
                        'value': value,
                        'domain': host,
                        'path': path or '/',
                        'secure': bool(secure),
                        'httpOnly': bool(httponly)
                    }
                    if expires:
                        # Chrome stores as microseconds since Jan 1, 1601
                        # Convert to unix timestamp
                        cookie['expires'] = (expires / 1000000) - 11644473600
                    cookies.append(cookie)

            conn.close()
            print(f"Extracted {len(cookies)} Twitter cookies")
            return cookies

        except Exception as e:
            print(f"Error extracting cookies: {e}")
            return []
        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass  # Temp file cleanup is best-effort

    async def scrape_account(self, page, handle):
        """Scrape recent posts from a Twitter account"""
        print(f"Scraping @{handle}...")

        try:
            await page.goto(f"https://x.com/{handle}", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
        except Exception as e:
            print(f"  Error loading @{handle}: {e}")
            return []

        posts = []
        seen_urls = set()
        scroll_attempts = 0
        max_scrolls = 5

        while scroll_attempts < max_scrolls:
            tweets = await page.query_selector_all('article[data-testid="tweet"]')

            for tweet in tweets:
                try:
                    link = await tweet.query_selector('a[href*="/status/"]')
                    if link:
                        href = await link.get_attribute('href')
                        if not href:
                            continue

                        full_url = f"https://x.com{href}" if href.startswith('/') else href

                        # Extract status ID for dedup
                        status_match = re.search(r'/status/(\d+)', full_url)
                        status_id = status_match.group(1) if status_match else None

                        # Skip if duplicate
                        if full_url in seen_urls:
                            continue
                        if full_url in self.existing_urls:
                            continue
                        if status_id and status_id in self.existing_urls:
                            continue

                        seen_urls.add(full_url)

                        text_elem = await tweet.query_selector('[data-testid="tweetText"]')
                        text = await text_elem.inner_text() if text_elem else ""

                        if self.is_business_content(text):
                            posts.append({
                                'url': full_url,
                                'text': text,
                                'handle': handle
                            })

                except Exception as e:
                    continue

            await page.evaluate("window.scrollBy(0, 800)")
            await asyncio.sleep(1.5)
            scroll_attempts += 1

        print(f"  Found {len(posts)} new posts from @{handle}")
        return posts

    async def scrape_bookmarks(self, page):
        """Scrape Twitter bookmarks"""
        print("Navigating to Twitter bookmarks...")

        try:
            await page.goto("https://x.com/i/bookmarks", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
        except Exception as e:
            print(f"Error loading bookmarks: {e}")
            return []

        bookmarks = []
        seen_urls = set()
        scroll_attempts = 0
        max_scrolls = 20

        while scroll_attempts < max_scrolls:
            tweets = await page.query_selector_all('article[data-testid="tweet"]')

            for tweet in tweets:
                try:
                    link = await tweet.query_selector('a[href*="/status/"]')
                    if link:
                        href = await link.get_attribute('href')
                        if not href:
                            continue

                        full_url = f"https://x.com{href}" if href.startswith('/') else href

                        status_match = re.search(r'/status/(\d+)', full_url)
                        status_id = status_match.group(1) if status_match else None

                        if full_url in seen_urls:
                            continue
                        if full_url in self.existing_urls:
                            continue
                        if status_id and status_id in self.existing_urls:
                            continue

                        seen_urls.add(full_url)

                        text_elem = await tweet.query_selector('[data-testid="tweetText"]')
                        text = await text_elem.inner_text() if text_elem else ""

                        if self.is_business_content(text):
                            bookmarks.append({
                                'url': full_url,
                                'text': text,
                                'handle': self.extract_handle(href)
                            })

                except Exception as e:
                    continue

            await page.evaluate("window.scrollBy(0, 1000)")
            await asyncio.sleep(2)
            scroll_attempts += 1
            print(f"  Scrolled {scroll_attempts}/{max_scrolls}, found {len(bookmarks)} new bookmarks")

        return bookmarks

    def is_business_content(self, text):
        """Filter for business/tech content only"""
        if len(text) < 50:
            return False

        business_keywords = [
            'revenue', 'mrr', 'arr', 'users', 'growth', 'launch', 'build',
            'app', 'saas', 'startup', 'indie', 'maker', 'founder',
            'email', 'seo', 'marketing', 'conversion', 'funnel',
            'automation', 'ai', 'tool', 'product', 'api', 'code',
            'ship', 'profit', 'customer', 'subscriber', 'traffic',
            'monetize', 'paywall', 'pricing', 'sale', 'client',
            '$', 'k/mo', 'k mrr', '/month', 'per month'
        ]

        text_lower = text.lower()
        return any(keyword in text_lower for keyword in business_keywords)

    def extract_handle(self, url):
        """Extract @handle from URL"""
        match = re.search(r'/([^/]+)/status/', url)
        return match.group(1) if match else "unknown"

    def categorize_content(self, text):
        """Auto-categorize based on content"""
        text_lower = text.lower()

        if any(word in text_lower for word in ['app', 'mobile', 'ios', 'android', 'store']):
            return 'APP_FACTORY'
        elif any(word in text_lower for word in ['email', 'cold', 'outbound', 'deliverability', 'inbox']):
            return 'COLD_OUTBOUND'
        elif any(word in text_lower for word in ['seo', 'search', 'google', 'ranking', 'backlink']):
            return 'SEO_GEO_ASO'
        elif any(word in text_lower for word in ['content', 'tiktok', 'instagram', 'youtube', 'reel', 'video']):
            return 'CONTENT_FARM'
        elif any(word in text_lower for word in ['ai', 'gpt', 'claude', 'llm', 'automation', 'api']):
            return 'TOOL_ALPHA'
        elif any(word in text_lower for word in ['revenue', 'pricing', 'monetization', 'paywall', 'subscription']):
            return 'MONETIZATION'
        elif any(word in text_lower for word in ['growth', 'traffic', 'viral', 'distribution', 'acquisition']):
            return 'GROWTH_HACK'
        elif any(word in text_lower for word in ['crypto', 'bitcoin', 'trading', 'defi', 'nft']):
            return 'ALGO_TRADING'
        else:
            return 'GENERAL'

    def save_to_csv(self, data, source_type):
        """Save extracted data to ALPHA_STAGING.csv"""
        if not data:
            print(f"No new {source_type} to save")
            return 0

        print(f"Saving {len(data)} {source_type} to ALPHA_STAGING.csv...")

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Read existing headers
        fieldnames = [
            'alpha_id', 'source', 'source_url', 'category', 'tactic',
            'roi_potential', 'priority', 'status', 'applicable_methods',
            'applicable_niches', 'synergy_score', 'reviewer_notes',
            'quality_issues', 'engagement_authenticity', 'earnings_verified',
            'extracted_method', 'compliance_notes', 'date_added'
        ]

        # Read existing rows
        existing_rows = []
        if ALPHA_STAGING.exists():
            with open(ALPHA_STAGING, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_fieldnames = reader.fieldnames or []
                # Merge fieldnames
                for fn in existing_fieldnames:
                    if fn not in fieldnames:
                        fieldnames.append(fn)
                for row in reader:
                    existing_rows.append(row)

        # Add new entries
        new_count = 0
        for item in data:
            alpha_id = f"ALPHA{self.next_alpha_id}"
            self.next_alpha_id += 1

            category = self.categorize_content(item['text'])

            row = {
                'alpha_id': alpha_id,
                'source': f"@{item['handle']} (high-signal Twitter)",
                'source_url': item['url'],
                'category': category,
                'tactic': item['text'][:200] + '...' if len(item['text']) > 200 else item['text'],
                'roi_potential': 'MEDIUM',
                'priority': '',
                'status': 'PENDING_REVIEW',
                'applicable_methods': category,
                'applicable_niches': '',
                'synergy_score': '',
                'reviewer_notes': f"Auto-scraped via twitter_scraper_live.py. Scraped {timestamp}",
                'quality_issues': '',
                'engagement_authenticity': 'AUTHENTIC',
                'earnings_verified': '',
                'extracted_method': '',
                'compliance_notes': '',
                'date_added': timestamp
            }
            existing_rows.append(row)
            new_count += 1

        # Write back
        with open(ALPHA_STAGING, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(existing_rows)

        print(f"Saved {new_count} new entries")
        return new_count

    def load_high_signal_accounts(self):
        """Load Twitter accounts marked auto_monitor=TRUE"""
        accounts = []
        with open(HIGH_SIGNAL_SOURCES, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                platform = row.get('platform', '').strip()
                if platform in ['X', 'Twitter'] and row.get('auto_monitor') == 'TRUE':
                    handle = row.get('source_name', '').replace('@', '')
                    if handle:
                        accounts.append({
                            'handle': handle,
                            'signal_quality': row.get('signal_quality', 'MEDIUM')
                        })

        # Sort by signal quality (HIGHEST first)
        priority = {'HIGHEST': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
        accounts.sort(key=lambda x: priority.get(x['signal_quality'], 4))

        return accounts


async def main():
    parser = argparse.ArgumentParser(description='Twitter Alpha Scraper - Works with running Chrome')
    parser.add_argument('--bookmarks', action='store_true', help='Scrape bookmarks')
    parser.add_argument('--accounts', action='store_true', help='Scrape high-signal accounts')
    parser.add_argument('--all', action='store_true', help='Scrape everything')
    parser.add_argument('--limit', type=int, default=20, help='Max accounts to scrape')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    args = parser.parse_args()

    if not any([args.bookmarks, args.accounts, args.all]):
        print("Specify --bookmarks, --accounts, or --all")
        return

    scraper = TwitterScraperLive()

    # Extract cookies from Chrome
    cookies = scraper.extract_cookies_from_chrome()

    if not cookies:
        print("WARNING: No Twitter cookies found. You may need to log in manually.")

    async with async_playwright() as p:
        print("Launching browser...")

        # Use a fresh temp profile (doesn't conflict with running Chrome)
        browser = await p.chromium.launch(
            headless=args.headless,
            channel="chrome"
        )

        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
        )

        # Add cookies to context
        if cookies:
            print(f"Adding {len(cookies)} cookies to browser context...")
            try:
                await context.add_cookies(cookies)
            except Exception as e:
                print(f"Error adding cookies: {e}")

        page = await context.new_page()

        # Navigate to Twitter to verify login
        print("Verifying Twitter login...")
        await page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)

        # Check if logged in by looking for compose button
        compose_btn = await page.query_selector('a[href="/compose/tweet"]')
        if compose_btn:
            print("Logged in to Twitter/X!")
        else:
            print("WARNING: May not be logged in. Bookmarks may not work.")

        total_new = 0

        try:
            # Scrape bookmarks
            if args.bookmarks or args.all:
                bookmarks = await scraper.scrape_bookmarks(page)
                count = scraper.save_to_csv(bookmarks, 'bookmarks')
                total_new += count

                # Save raw JSON backup
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                with open(OUTPUT_DIR / f"bookmarks_{timestamp}.json", 'w') as f:
                    json.dump(bookmarks, f, indent=2)

            # Scrape high-signal accounts
            if args.accounts or args.all:
                accounts = scraper.load_high_signal_accounts()
                print(f"\nFound {len(accounts)} high-signal accounts to scrape")

                all_posts = []
                for i, account in enumerate(accounts[:args.limit], 1):
                    print(f"\n[{i}/{min(args.limit, len(accounts))}] ", end='')
                    posts = await scraper.scrape_account(page, account['handle'])
                    all_posts.extend(posts)

                    # Save incrementally every 10 accounts
                    if i % 10 == 0:
                        count = scraper.save_to_csv(all_posts, 'high-signal-accounts')
                        total_new += count
                        all_posts = []  # Reset after saving

                    # Small delay between accounts to avoid rate limiting
                    await asyncio.sleep(2)

                # Save remaining
                if all_posts:
                    count = scraper.save_to_csv(all_posts, 'high-signal-accounts')
                    total_new += count

                # Save raw JSON backup
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                with open(OUTPUT_DIR / f"accounts_{timestamp}.json", 'w') as f:
                    json.dump(all_posts, f, indent=2)

            print(f"\n{'='*50}")
            print(f"SCRAPING COMPLETE")
            print(f"Total new entries added: {total_new}")
            print(f"Check: {ALPHA_STAGING}")
            print(f"{'='*50}")

        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
