#!/usr/bin/env python3

from __future__ import annotations
"""
Twitter Alpha Scraper (Brave-cookie auth)
========================================

Scrapes:
  - Your X/Twitter bookmarks
  - High-signal accounts from LEDGER/HIGH_SIGNAL_SOURCES.csv (auto_monitor=TRUE)
  - Explicit handles via --handles (useful for targeted "last 30 days" pulls)

Writes:
  - LEDGER/ALPHA_STAGING.csv (canonical 20-column schema)
  - LEDGER/COPY_STYLE_CORPUS.csv (for studying voice/copy patterns)

Notes:
  - Uses your real logged-in Brave cookies (no X API keys required).
  - Intentionally avoids stealth/evasion tricks. If X blocks, that's a human problem.

Usage:
  python3 AUTOMATIONS/twitter_alpha_scraper.py --handles @eptwts @pipelineabuser --days 30
  python3 AUTOMATIONS/twitter_alpha_scraper.py --accounts --limit 20
  python3 AUTOMATIONS/twitter_alpha_scraper.py --all --deep
"""

import argparse
import asyncio
import csv
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import sys

csv.field_size_limit(sys.maxsize)
import tempfile
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("ERROR: playwright not installed. Run: pip3 install playwright && python3 -m playwright install chromium")
    sys.exit(1)

try:
    from Crypto.Cipher import AES
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False
    print("WARNING: pycryptodome not installed. Run: pip3 install pycryptodome")

# Paths (auto-detected from script location)
PROJECT_DIR = Path(__file__).resolve().parent.parent
OPS_DIR = PROJECT_DIR / "OPS"
LEDGER_DIR = PROJECT_DIR / "LEDGER"
ALPHA_STAGING = LEDGER_DIR / "ALPHA_STAGING.csv"
HIGH_SIGNAL_SOURCES = LEDGER_DIR / "HIGH_SIGNAL_SOURCES.csv"
OUTPUT_DIR = PROJECT_DIR / "AUTOMATIONS" / "twitter_scraper_output"
MEDIA_DIR = PROJECT_DIR / "AUTOMATIONS" / "media_downloads"
OUTPUT_DIR.mkdir(exist_ok=True)
MEDIA_DIR.mkdir(exist_ok=True)

COPY_STYLE_HANDLES_FILE = OPS_DIR / "COPY_STYLE_HANDLES.txt"
COPY_STYLE_CORPUS = LEDGER_DIR / "COPY_STYLE_CORPUS.csv"

# Canonical ALPHA_STAGING schema (keep aligned with LEDGER/ALPHA_STAGING.csv).
ALPHA_FIELDS = [
    "alpha_id",
    "source",
    "source_url",
    "category",
    "tactic",
    "roi_potential",
    "priority",
    "status",
    "applicable_methods",
    "applicable_niches",
    "synergy_score",
    "cross_sell_products",
    "implementation_priority",
    "engagement_authenticity",
    "earnings_verified",
    "extracted_method",
    "compliance_notes",
    "reviewer_notes",
    "created_at",
    "ops_generated",
]

# Brave Browser paths
BRAVE_USER_DATA = Path.home() / "Library/Application Support/BraveSoftware/Brave-Browser"
BRAVE_KEY_FILE = PROJECT_DIR / "AUTOMATIONS" / ".brave_cookie_key"
CREDS_FILE = PROJECT_DIR / "AUTOMATIONS" / ".twitter_creds.json"

# JS to extract full tweet data including engagement metrics and media
EXTRACT_TWEETS_JS = """
() => {
    const results = [];
    const articles = document.querySelectorAll('article[data-testid="tweet"]');
    articles.forEach(article => {
        try {
            const link = article.querySelector('a[href*="/status/"]');
            const textElem = article.querySelector('[data-testid="tweetText"]');
            if (!link) return;

            const url = link.href;
            const text = textElem ? textElem.innerText : '';

            // Engagement metrics from aria-labels on action buttons
            const getMetric = (testId) => {
                const btn = article.querySelector(`[data-testid="${testId}"]`);
                if (!btn) return 0;
                const label = btn.getAttribute('aria-label') || btn.closest('[aria-label]')?.getAttribute('aria-label') || '';
                const match = label.match(/([\\d,\\.]+[KMkm]?)\\s/);
                if (!match) return 0;
                let val = match[1].replace(/,/g, '');
                if (val.endsWith('K') || val.endsWith('k')) return parseFloat(val) * 1000;
                if (val.endsWith('M') || val.endsWith('m')) return parseFloat(val) * 1000000;
                return parseInt(val) || 0;
            };

            const likes = getMetric('like') || getMetric('unlike');
            const retweets = getMetric('retweet');
            const replies = getMetric('reply');

            // Views - look for analytics link with view count
            let views = 0;
            const analyticsLink = article.querySelector('a[href*="/analytics"]');
            if (analyticsLink) {
                const viewLabel = analyticsLink.getAttribute('aria-label') || '';
                const viewMatch = viewLabel.match(/([\\d,\\.]+[KMkm]?)\\s/);
                if (viewMatch) {
                    let v = viewMatch[1].replace(/,/g, '');
                    if (v.endsWith('K') || v.endsWith('k')) views = parseFloat(v) * 1000;
                    else if (v.endsWith('M') || v.endsWith('m')) views = parseFloat(v) * 1000000;
                    else views = parseInt(v) || 0;
                }
            }

            // Media URLs (images)
            const images = [];
            article.querySelectorAll('img[src*="pbs.twimg.com/media/"]').forEach(img => {
                let src = img.src;
                // Get highest quality version
                if (src.includes('?')) src = src.split('?')[0] + '?format=jpg&name=large';
                else src += '?format=jpg&name=large';
                images.push(src);
            });

            // Video detection
            const hasVideo = article.querySelector('video') !== null ||
                           article.querySelector('[data-testid="videoPlayer"]') !== null;

            // Handle extraction
            const handleMatch = url.match(/x\\.com\\/([^/]+)\\/status/);
            const handle = handleMatch ? handleMatch[1] : 'unknown';

            // Timestamp
            const timeElem = article.querySelector('time');
            const timestamp = timeElem ? timeElem.getAttribute('datetime') : '';

            results.push({
                url, text, handle, timestamp,
                likes, retweets, replies, views,
                images, hasVideo,
                engagement_ratio: likes > 0 ? (replies / likes).toFixed(3) : '0'
            });
        } catch(e) {}
    });
    return results;
}
"""

# JS to extract replies from a tweet detail page
EXTRACT_REPLIES_JS = """
() => {
    const results = [];
    // Skip the first article (the original tweet) and grab replies
    const articles = document.querySelectorAll('article[data-testid="tweet"]');
    let isFirst = true;
    articles.forEach(article => {
        if (isFirst) { isFirst = false; return; } // Skip original tweet
        try {
            const textElem = article.querySelector('[data-testid="tweetText"]');
            const text = textElem ? textElem.innerText : '';
            if (!text) return;

            const link = article.querySelector('a[href*="/status/"]');
            const url = link ? link.href : '';

            const handleMatch = url.match(/x\\.com\\/([^/]+)\\/status/);
            const handle = handleMatch ? handleMatch[1] : 'unknown';

            // Check for funnel signals in reply
            const textLower = text.toLowerCase();
            const hasCTA = /dm me|link in bio|check out|sign up|grab it|get it|reply .* and i/i.test(text);
            const hasLink = /https?:\\/\\//i.test(text) || /\\.(com|io|ai|co|app|xyz)\\/\\S/i.test(text);
            const hasPitch = /\\$\\d|free|discount|offer|limited|exclusive|course|ebook|pdf|template/i.test(text);

            // Engagement on reply
            const getMetric = (testId) => {
                const btn = article.querySelector(`[data-testid="${testId}"]`);
                if (!btn) return 0;
                const label = btn.getAttribute('aria-label') || '';
                const match = label.match(/([\\d,\\.]+[KMkm]?)\\s/);
                if (!match) return 0;
                let val = match[1].replace(/,/g, '');
                if (val.endsWith('K') || val.endsWith('k')) return parseFloat(val) * 1000;
                if (val.endsWith('M') || val.endsWith('m')) return parseFloat(val) * 1000000;
                return parseInt(val) || 0;
            };

            results.push({
                url, text, handle,
                likes: getMetric('like') || getMetric('unlike'),
                retweets: getMetric('retweet'),
                hasCTA, hasLink, hasPitch,
                funnel_type: hasCTA ? 'DM_FUNNEL' : hasLink ? 'LINK_FUNNEL' : hasPitch ? 'PITCH' : 'ORGANIC'
            });
        } catch(e) {}
    });
    return results;
}
"""


def extract_brave_cookies(domain_filter=".x.com"):
    """Extract and decrypt cookies from Brave's cookie database.
    Brave stays open - we just copy and read the SQLite DB."""
    if not HAS_CRYPTO:
        return []

    cookie_db = BRAVE_USER_DATA / "Default" / "Cookies"
    if not cookie_db.exists():
        print(f"ERROR: Cookie database not found at {cookie_db}")
        return []

    # Get decryption key: cached file first, then Keychain
    keychain_pass = None
    if BRAVE_KEY_FILE.exists():
        keychain_pass = BRAVE_KEY_FILE.read_text().strip()

    if not keychain_pass:
        try:
            result = subprocess.run(
                ["security", "find-generic-password", "-s", "Brave Safe Storage", "-w"],
                capture_output=True, text=True, timeout=15
            )
            keychain_pass = result.stdout.strip()
            if keychain_pass:
                BRAVE_KEY_FILE.write_text(keychain_pass)
        except (subprocess.TimeoutExpired, Exception) as e:
            print(f"ERROR: Keychain failed: {e}")
            return []

    if not keychain_pass:
        return []

    aes_key = hashlib.pbkdf2_hmac('sha1', keychain_pass.encode('utf-8'), b'saltysalt', 1003, dklen=16)

    temp_db = tempfile.mktemp(suffix=".db", prefix="brave_cookies_")
    shutil.copy2(str(cookie_db), temp_db)
    journal = str(cookie_db) + "-journal"
    if Path(journal).exists():
        shutil.copy2(journal, temp_db + "-journal")

    cookies = []
    try:
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT host_key, name, path, encrypted_value, is_secure, is_httponly, "
            "expires_utc, samesite FROM cookies WHERE host_key = ? OR host_key = ?",
            (domain_filter, domain_filter.lstrip('.')),
        )

        for row in cursor.fetchall():
            host_key, name, path, encrypted_value, is_secure, is_httponly, expires_utc, samesite = row
            value = ""
            if encrypted_value and encrypted_value[:3] == b'v10':
                iv = b' ' * 16
                enc_data = encrypted_value[3:]
                if len(enc_data) % 16 != 0:
                    enc_data += b'\x00' * (16 - len(enc_data) % 16)
                try:
                    cipher = AES.new(aes_key, AES.MODE_CBC, iv)
                    decrypted = cipher.decrypt(enc_data)
                    pad_len = decrypted[-1]
                    if 0 < pad_len <= 16:
                        decrypted = decrypted[:-pad_len]
                    else:
                        decrypted = decrypted.rstrip(b'\x00')
                    if len(decrypted) > 32:
                        value = decrypted[32:].decode('utf-8', errors='replace')
                    else:
                        value = decrypted.decode('utf-8', errors='replace')
                except Exception:
                    continue

            value = value.strip('\x00').strip()
            if not value:
                continue

            if expires_utc and expires_utc > 0:
                epoch_start = datetime(1601, 1, 1, tzinfo=timezone.utc)
                expires = (epoch_start + timedelta(microseconds=expires_utc)).timestamp()
            else:
                expires = -1

            if samesite in (-1, 0):
                ss = "None" if is_secure else "Lax"
            elif samesite == 1:
                ss = "Lax"
            else:
                ss = "Strict"

            cookie = {
                "name": name, "value": value, "domain": host_key,
                "path": path, "secure": bool(is_secure),
                "httpOnly": bool(is_httponly), "sameSite": ss,
            }
            if expires > 0:
                cookie["expires"] = expires
            cookies.append(cookie)

        conn.close()
    finally:
        Path(temp_db).unlink(missing_ok=True)
        Path(temp_db + "-journal").unlink(missing_ok=True)

    return cookies


class TwitterScraper:
    def __init__(self, deep=False, download_media=False, meme_mode=False, days: int | None = None):
        self.existing_urls = self._load_existing_urls()
        self.next_alpha_id = self._get_next_alpha_id()
        self.deep = deep
        self.download_media = download_media
        self.meme_mode = meme_mode
        self.all_results = []  # Accumulate everything for JSON dump
        self.days = int(days) if days else None
        self.cutoff_dt = (
            (datetime.now(timezone.utc) - timedelta(days=self.days)) if self.days else None
        )
        self.copy_style_handles = self._load_copy_style_handles()
        self.alpha_fieldnames = self._load_alpha_fieldnames()

    def _load_alpha_fieldnames(self) -> list[str]:
        if not ALPHA_STAGING.exists():
            return ALPHA_FIELDS
        try:
            with open(ALPHA_STAGING, "r", encoding="utf-8", errors="replace", newline="") as f:
                header = next(csv.reader(f))
            if header == ALPHA_FIELDS:
                return header
        except Exception:
            pass
        # Fall back to canonical; better to be strict than append misaligned rows.
        return ALPHA_FIELDS

    def _load_copy_style_handles(self) -> set[str]:
        handles: set[str] = set()
        if COPY_STYLE_HANDLES_FILE.exists():
            try:
                for line in COPY_STYLE_HANDLES_FILE.read_text(encoding="utf-8", errors="replace").splitlines():
                    s = line.strip()
                    if not s or s.startswith("#"):
                        continue
                    handles.add(s.lstrip("@").strip().lower())
            except Exception:
                pass
        return handles

    def _parse_ts(self, ts: str) -> datetime | None:
        s = (ts or "").strip()
        if not s:
            return None
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s)
        except Exception:
            return None

    def _load_existing_urls(self):
        urls = set()
        if ALPHA_STAGING.exists():
            with open(ALPHA_STAGING, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    if row.get('source_url'):
                        urls.add(row['source_url'])
        return urls

    def _get_next_alpha_id(self):
        max_id = 0
        if ALPHA_STAGING.exists():
            with open(ALPHA_STAGING, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    m = re.match(r'ALPHA(\d+)', row.get('alpha_id', ''))
                    if m:
                        max_id = max(max_id, int(m.group(1)))
        return max_id + 1

    async def scrape_bookmarks(self, page, max_scrolls=30):
        """Scrape Twitter bookmarks with full engagement data."""
        print("📚 Navigating to Twitter bookmarks...")
        await page.goto("https://x.com/i/bookmarks", wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(4)

        all_tweets = []
        seen_urls = set()

        for scroll in range(max_scrolls):
            tweets = await page.evaluate(EXTRACT_TWEETS_JS)

            new_count = 0
            for t in tweets:
                if t['url'] not in seen_urls and t['url'] not in self.existing_urls:
                    seen_urls.add(t['url'])
                    # In bookmark mode, keep everything (user bookmarked it for a reason)
                    all_tweets.append(t)
                    new_count += 1

            if scroll % 5 == 0 or new_count > 0:
                print(f"  Scroll {scroll+1}/{max_scrolls} | {len(all_tweets)} bookmarks found")

            prev_height = await page.evaluate("document.body.scrollHeight")
            await page.evaluate("window.scrollBy(0, 1200)")
            await asyncio.sleep(2)
            new_height = await page.evaluate("document.body.scrollHeight")

            if new_height == prev_height and scroll > 3:
                print(f"  Reached end of bookmarks at scroll {scroll+1}")
                break

        # Deep mode: click into high-engagement tweets for replies
        if self.deep:
            await self._deep_scrape_replies(page, all_tweets)

        # Download media if requested
        if self.download_media:
            await self._download_media(all_tweets, "bookmarks")

        return all_tweets

    async def scrape_account(self, page, handle, max_scrolls=5):
        """Scrape an account's tweets with engagement metrics."""
        await page.goto(f"https://x.com/{handle}", wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(3)

        all_tweets = []
        seen_urls = set()
        consecutive_old_scrolls = 0

        for scroll in range(max_scrolls):
            tweets = await page.evaluate(EXTRACT_TWEETS_JS)

            new_within = 0
            new_old = 0
            for t in tweets:
                if t['url'] not in seen_urls and t['url'] not in self.existing_urls:
                    seen_urls.add(t['url'])
                    t['handle'] = handle  # Override with known handle

                    ts = self._parse_ts(t.get("timestamp", ""))
                    if self.cutoff_dt and ts and ts < self.cutoff_dt:
                        new_old += 1
                        continue

                    # In meme mode, keep everything. Otherwise filter for business content.
                    if self.meme_mode or self._is_signal_content(t['text']):
                        all_tweets.append(t)
                        new_within += 1

            if self.cutoff_dt:
                if new_within == 0 and new_old > 0:
                    consecutive_old_scrolls += 1
                elif new_within > 0:
                    consecutive_old_scrolls = 0

                # Stop once we hit older content twice in a row (avoids early stop on pinned tweets).
                if consecutive_old_scrolls >= 2 and scroll >= 3:
                    break

            await page.evaluate("window.scrollBy(0, 1000)")
            await asyncio.sleep(1.5)

        print(f"  Found {len(all_tweets)} tweets from @{handle}")
        return all_tweets

    async def scrape_meme_accounts(self, page, handles):
        """Scrape meme/viral accounts - no business filter, download all media."""
        print(f"\n🎭 MEME MODE: Scraping {len(handles)} viral accounts")
        all_tweets = []

        for i, handle in enumerate(handles, 1):
            handle = handle.lstrip('@')
            print(f"\n[{i}/{len(handles)}] @{handle}...")

            try:
                await page.goto(f"https://x.com/{handle}", wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(3)

                seen_urls = set()
                for scroll in range(8):  # More scrolls for meme accounts
                    tweets = await page.evaluate(EXTRACT_TWEETS_JS)
                    for t in tweets:
                        if t['url'] not in seen_urls:
                            seen_urls.add(t['url'])
                            t['handle'] = handle
                            t['content_type'] = 'meme_viral'
                            all_tweets.append(t)

                    await page.evaluate("window.scrollBy(0, 1000)")
                    await asyncio.sleep(1.5)

                print(f"  Found {len([t for t in all_tweets if t['handle'] == handle])} posts from @{handle}")

                # Download media for this account
                if self.download_media:
                    account_tweets = [t for t in all_tweets if t['handle'] == handle]
                    await self._download_media(account_tweets, handle)

                await asyncio.sleep(2)

            except Exception as e:
                print(f"  Error scraping @{handle}: {str(e)[:80]}")
                continue

        return all_tweets

    async def _deep_scrape_replies(self, page, tweets):
        """Click into high-engagement tweets and scrape top replies for funnel analysis."""
        # Sort by engagement, scrape replies on top tweets
        high_engagement = sorted(tweets, key=lambda t: t.get('likes', 0), reverse=True)
        top_tweets = [t for t in high_engagement if t.get('likes', 0) >= 50][:15]

        if not top_tweets:
            top_tweets = high_engagement[:5]

        print(f"\n🔍 DEEP MODE: Analyzing replies on {len(top_tweets)} high-engagement tweets...")

        for i, tweet in enumerate(top_tweets, 1):
            try:
                print(f"  [{i}/{len(top_tweets)}] {tweet['url'][:60]}... ({tweet.get('likes', 0)} likes)")
                await page.goto(tweet['url'], wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(3)

                # Scroll to load more replies
                for _ in range(3):
                    await page.evaluate("window.scrollBy(0, 800)")
                    await asyncio.sleep(1.5)

                replies = await page.evaluate(EXTRACT_REPLIES_JS)

                # Analyze reply funnels
                funnel_replies = [r for r in replies if r.get('hasCTA') or r.get('hasLink') or r.get('hasPitch')]
                organic_replies = [r for r in replies if r.get('funnel_type') == 'ORGANIC']

                tweet['replies_scraped'] = len(replies)
                tweet['funnel_replies'] = len(funnel_replies)
                tweet['top_replies'] = replies[:10]  # Keep top 10 replies

                if funnel_replies:
                    tweet['funnel_analysis'] = {
                        'dm_funnels': len([r for r in funnel_replies if r['funnel_type'] == 'DM_FUNNEL']),
                        'link_funnels': len([r for r in funnel_replies if r['funnel_type'] == 'LINK_FUNNEL']),
                        'pitches': len([r for r in funnel_replies if r['funnel_type'] == 'PITCH']),
                        'examples': [r['text'][:200] for r in funnel_replies[:3]]
                    }
                    print(f"    Funnel activity: {len(funnel_replies)} pitches/CTAs in replies")

                # Engagement authenticity check
                if tweet.get('likes', 0) > 0:
                    ratio = tweet.get('replies', 0) / tweet['likes']
                    if ratio < 0.01 and tweet['likes'] > 1000:
                        tweet['engagement_authenticity'] = 'SUSPICIOUS'
                        print(f"    ⚠️  Suspicious: {tweet['likes']} likes but only {tweet.get('replies', 0)} replies")
                    else:
                        tweet['engagement_authenticity'] = 'AUTHENTIC'

                await asyncio.sleep(2)

            except Exception as e:
                print(f"    Error on deep scrape: {str(e)[:60]}")
                continue

    async def _download_media(self, tweets, folder_name):
        """Download images from tweets to AUTOMATIONS/media_downloads/{folder}/"""
        save_dir = MEDIA_DIR / folder_name.replace('@', '')
        save_dir.mkdir(exist_ok=True)

        total_images = 0
        total_videos = 0

        for tweet in tweets:
            images = tweet.get('images', [])
            for img_url in images:
                try:
                    # Generate filename from URL
                    img_name = re.search(r'/media/([^?]+)', img_url)
                    if img_name:
                        filename = img_name.group(1)
                    else:
                        filename = f"img_{total_images}.jpg"

                    filepath = save_dir / filename
                    if not filepath.exists():
                        urllib.request.urlretrieve(img_url, str(filepath))
                        total_images += 1
                except Exception:
                    continue

            if tweet.get('hasVideo'):
                # Log video URLs for yt-dlp download
                video_log = save_dir / "video_urls.txt"
                with open(video_log, 'a') as f:
                    f.write(f"{tweet['url']}\n")
                total_videos += 1

        if total_images > 0 or total_videos > 0:
            print(f"  📥 Downloaded {total_images} images to {save_dir}")
            if total_videos > 0:
                print(f"  🎬 {total_videos} videos logged to {save_dir}/video_urls.txt")
                print(f"     Download with: yt-dlp -a {save_dir}/video_urls.txt -o '{save_dir}/%(id)s.%(ext)s'")

    def _is_signal_content(self, text):
        """Filter for actionable business/tech content."""
        if len(text) < 30:
            return False
        text_lower = text.lower()
        keywords = [
            'revenue', 'mrr', 'arr', 'users', 'growth', 'launch', 'build', 'built',
            'app', 'saas', 'startup', 'indie', 'maker', 'founder', 'ship',
            'email', 'seo', 'marketing', 'conversion', 'funnel', 'landing',
            'automation', 'ai', 'tool', 'product', 'api', 'code', 'framework',
            '$', 'profit', 'income', 'subscribers', 'customers', 'paying',
            'how to', 'step 1', 'playbook', 'strategy', 'tactic', 'hack',
            'cold', 'outbound', 'newsletter', 'content', 'viral', 'distribution',
            'tiktok', 'youtube', 'instagram', 'twitter', 'threads',
            'affiliate', 'commission', 'dropship', 'ecom', 'shopify',
            'gumroad', 'whop', 'stripe', 'notion', 'template',
            'mcp', 'claude', 'gpt', 'llm', 'agent', 'cursor',
        ]
        return any(kw in text_lower for kw in keywords)

    def _categorize(self, text):
        text_lower = text.lower()
        cats = [
            (['cold email', 'outbound', 'deliverability', 'inbox', 'smtp', 'warmup'], 'OUTBOUND'),
            (['app store', 'ios', 'android', 'mobile app', 'react native', 'swift'], 'APP_FACTORY'),
            (['mcp', 'claude', 'gpt', 'llm', 'ai agent', 'cursor', 'automation tool'], 'TOOL_ALPHA'),
            (['tiktok', 'reels', 'youtube', 'shorts', 'content farm', 'faceless'], 'CONTENT_FARM'),
            (['saas', 'mrr', 'arr', 'subscription', 'churn', 'b2b'], 'MONETIZATION'),
            (['seo', 'google', 'ranking', 'organic traffic', 'backlink', 'aso', 'keyword'], 'SEO_GEO_ASO'),
            (['$', 'revenue', 'profit', 'income', 'monetiz', 'pricing'], 'MONETIZATION'),
            (['growth', 'viral', 'followers', 'engagement', 'distribution'], 'GROWTH_HACK'),
            (['ecom', 'dropship', 'amazon', 'etsy', 'shopify', 'temu'], 'ECOM_ARB'),
        ]
        for keywords, cat in cats:
            if any(kw in text_lower for kw in keywords):
                return cat
        return 'GENERAL'

    def _estimate_roi(self, tweet):
        text = tweet.get('text', '').lower()
        score = 0
        if re.search(r'\$[\d,]+k?', text): score += 3
        if re.search(r'\d+%', text): score += 2
        if re.search(r'\d+x', text): score += 2
        if any(kw in text for kw in ['step 1', 'how to', 'framework', 'playbook']): score += 2
        if re.search(r'\.(com|io|ai|app|co)\b', tweet.get('text', '')): score += 1
        if tweet.get('likes', 0) > 1000: score += 2
        if tweet.get('likes', 0) > 5000: score += 2
        if tweet.get('funnel_replies', 0) > 0: score += 1

        if score >= 7: return 'HIGHEST'
        if score >= 4: return 'HIGH'
        if score >= 2: return 'MEDIUM'
        return 'LOW'

    def save_to_csv(self, tweets, source_type):
        """Append extracted tweets to LEDGER/ALPHA_STAGING.csv (canonical schema)."""
        if not tweets:
            print(f"⚠️  No new {source_type} to save")
            return

        if self.alpha_fieldnames != ALPHA_FIELDS:
            # Refuse to append into an unknown schema.
            print("❌ ALPHA_STAGING.csv header mismatch. Run: python3 AUTOMATIONS/alpha_staging_migrate.py")
            return

        print(f"💾 Saving {len(tweets)} {source_type} to ALPHA_STAGING.csv...")

        new_rows: list[dict] = []
        for t in tweets:
            alpha_id = f"ALPHA{self.next_alpha_id}"
            self.next_alpha_id += 1

            engagement = f"👍{t.get('likes',0)} 🔁{t.get('retweets',0)} 💬{t.get('replies',0)} 👁{t.get('views',0)}"
            funnel_info = ""
            if t.get('funnel_analysis'):
                fa = t['funnel_analysis']
                funnel_info = f" | Funnels: {fa.get('dm_funnels',0)} DM, {fa.get('link_funnels',0)} link, {fa.get('pitches',0)} pitch"
            media_info = ""
            img_count = len(t.get('images', []))
            if img_count > 0:
                media_info += f" | {img_count} images"
            if t.get('hasVideo'):
                media_info += " | has video"
            reply_context = ""
            if t.get('top_replies'):
                top_3 = t['top_replies'][:3]
                reply_excerpts = [f"@{r['handle']}: {r['text'][:100]}" for r in top_3]
                reply_context = " | TOP REPLIES: " + " /// ".join(reply_excerpts)

            tactic_text = t.get('text', '')[:500]
            roi = self._estimate_roi(t)

            new_rows.append(
                {
                    "alpha_id": alpha_id,
                    "source": f"@{t.get('handle', 'unknown')} ({source_type})",
                    "source_url": t.get("url", ""),
                    "category": self._categorize(t.get("text", "")),
                    "tactic": tactic_text,
                    "roi_potential": roi,
                    "priority": "HIGH" if roi in {"HIGHEST", "HIGH"} else "MEDIUM" if roi == "MEDIUM" else "LOW",
                    "status": "PENDING_REVIEW",
                    "applicable_methods": "",
                    "applicable_niches": "",
                    "synergy_score": "",
                    "cross_sell_products": "",
                    "implementation_priority": "",
                    "engagement_authenticity": t.get("engagement_authenticity", "UNCHECKED"),
                    "earnings_verified": "N/A",
                    "extracted_method": "",
                    "compliance_notes": "",
                    "reviewer_notes": f"{engagement}{funnel_info}{media_info}{reply_context}".strip(),
                    "created_at": datetime.now().isoformat(),
                    "ops_generated": "FALSE",
                }
            )

        with open(ALPHA_STAGING, 'a', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.alpha_fieldnames, extrasaction="ignore")
            writer.writerows(new_rows)

        print(f"✅ Saved {len(tweets)} entries (ALPHA{self.next_alpha_id - len(tweets)}-ALPHA{self.next_alpha_id - 1})")

        # Store for JSON dump
        self.all_results.extend(tweets)

        # Also store into copy-style corpus for configured handles.
        self.save_to_copy_corpus(tweets, source_type=source_type)

    def save_to_copy_corpus(self, tweets, source_type: str) -> None:
        """Append tweets into LEDGER/COPY_STYLE_CORPUS.csv for voice study."""
        if not tweets or not self.copy_style_handles:
            return

        rows = []
        for t in tweets:
            handle = (t.get("handle") or "").strip().lstrip("@").lower()
            if not handle or handle not in self.copy_style_handles:
                continue
            rows.append(
                {
                    "handle": f"@{handle}",
                    "url": t.get("url", ""),
                    "timestamp": t.get("timestamp", ""),
                    "text": (t.get("text", "") or "").replace("\n", " ").strip(),
                    "likes": str(int(t.get("likes", 0) or 0)),
                    "retweets": str(int(t.get("retweets", 0) or 0)),
                    "replies": str(int(t.get("replies", 0) or 0)),
                    "views": str(int(t.get("views", 0) or 0)),
                    "source_type": source_type,
                    "scraped_at": datetime.now().isoformat(),
                }
            )

        if not rows:
            return

        existing_urls = set()
        if COPY_STYLE_CORPUS.exists():
            try:
                with open(COPY_STYLE_CORPUS, "r", encoding="utf-8", errors="replace", newline="") as f:
                    for row in csv.DictReader(f):
                        u = (row.get("url") or "").strip()
                        if u:
                            existing_urls.add(u)
            except Exception:
                existing_urls = set()

        write_rows = [r for r in rows if r.get("url") and r["url"] not in existing_urls]
        if not write_rows:
            return

        COPY_STYLE_CORPUS.parent.mkdir(parents=True, exist_ok=True)
        exists = COPY_STYLE_CORPUS.exists()
        with open(COPY_STYLE_CORPUS, "a", encoding="utf-8", newline="") as f:
            fieldnames = [
                "handle",
                "url",
                "timestamp",
                "text",
                "likes",
                "retweets",
                "replies",
                "views",
                "source_type",
                "scraped_at",
            ]
            w = csv.DictWriter(f, fieldnames=fieldnames)
            if not exists:
                w.writeheader()
            w.writerows(write_rows)

        print(f"🧠 Copy-style corpus: +{len(write_rows)} rows -> {COPY_STYLE_CORPUS}")

    def load_high_signal_accounts(self):
        """Load ALL Twitter accounts marked auto_monitor=TRUE from HIGH_SIGNAL_SOURCES.csv."""
        accounts = []
        with open(HIGH_SIGNAL_SOURCES, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                platform = row.get('platform', '').strip()
                if platform in ['X', 'Twitter'] and row.get('auto_monitor') == 'TRUE':
                    handle = row.get('source_name', '').replace('@', '').strip()
                    if handle:
                        accounts.append({
                            'handle': handle,
                            'signal_quality': row.get('signal_quality', 'MEDIUM'),
                            'focus_area': row.get('focus_area', ''),
                        })

        priority = {'HIGHEST': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
        accounts.sort(key=lambda x: priority.get(x['signal_quality'], 4))
        return accounts


async def launch_browser_with_cookies(*, visible: bool = False):
    """Launch Chromium with Brave's Twitter cookies injected."""
    print("🔑 Extracting Twitter cookies from Brave Browser...")
    cookies = extract_brave_cookies(".x.com")
    cookies += extract_brave_cookies(".twitter.com")

    if not cookies:
        print("ERROR: No Twitter cookies found. Make sure you're logged into Twitter in Brave.")
        return None, None, None

    print(f"   Got {len(cookies)} cookies")

    p = await async_playwright().start()
    browser = await p.chromium.launch(headless=not visible, args=["--no-first-run"])
    context = await browser.new_context(
        viewport={'width': 1920, 'height': 1080},
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    injected = 0
    for cookie in cookies:
        try:
            await context.add_cookies([cookie])
            injected += 1
        except Exception:
            pass
    print(f"   {injected}/{len(cookies)} cookies injected")

    page = await context.new_page()

    # Verify login
    print("🔍 Checking Twitter login...")
    await page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=60000)
    await asyncio.sleep(5)

    if "login" in page.url.lower() or "flow" in page.url.lower():
        print("❌ Not logged in. Make sure Twitter is logged in via Brave Browser.")
        await browser.close()
        await p.stop()
        return None, None, None

    print("✅ Logged into Twitter\n")
    return p, browser, page


async def main():
    parser = argparse.ArgumentParser(description="Twitter Alpha Scraper (Brave cookie auth)")
    parser.add_argument("--bookmarks", action="store_true", help="Scrape your bookmarks")
    parser.add_argument("--accounts", action="store_true", help="Scrape ALL high-signal accounts (auto_monitor=TRUE)")
    parser.add_argument("--handles", nargs="+", metavar="@handle", help="Scrape explicit handles (targeted mode)")
    parser.add_argument("--all", action="store_true", help="Scrape bookmarks + all accounts")
    parser.add_argument("--meme", nargs="+", metavar="@handle", help="Scrape meme/viral accounts (no business filter)")
    parser.add_argument("--deep", action="store_true", help="Click into tweets, scrape replies, analyze funnels")
    parser.add_argument("--download-media", action="store_true", help="Download images (videos logged for yt-dlp)")
    parser.add_argument("--limit", type=int, help="Limit number of accounts (default: ALL)")
    parser.add_argument("--days", type=int, default=30, help="Only keep tweets within the last N days (default: 30)")
    parser.add_argument("--max-scrolls", type=int, default=None, help="Max scrolls per account (default depends on mode)")
    parser.add_argument("--visible", action="store_true", help="Show browser window (debug)")
    args = parser.parse_args()

    if not any([args.bookmarks, args.accounts, args.all, args.meme, args.handles]):
        parser.print_help()
        print("\nExamples:")
        print("  python3 AUTOMATIONS/twitter_alpha_scraper.py --handles @eptwts @pipelineabuser --days 30")
        print("  python3 AUTOMATIONS/twitter_alpha_scraper.py --accounts --limit 20")
        print("  python3 AUTOMATIONS/twitter_alpha_scraper.py --all --deep")
        return

    scraper = TwitterScraper(
        deep=args.deep,
        download_media=args.download_media,
        meme_mode=bool(args.meme),
        days=args.days if args.handles else None,
    )

    p, browser, page = await launch_browser_with_cookies(visible=bool(args.visible))
    if not page:
        return

    try:
        # Scrape bookmarks
        if args.bookmarks or args.all:
            bookmarks = await scraper.scrape_bookmarks(page)
            scraper.save_to_csv(bookmarks, 'bookmarks')

        # Scrape explicit handles (targeted mode)
        if args.handles:
            handles = [h.lstrip("@").strip() for h in args.handles if h.strip()]
            handles = [h for h in handles if h]
            max_scrolls = args.max_scrolls if args.max_scrolls is not None else 20
            all_posts = []
            print(f"\n🎯 Scraping {len(handles)} explicit handles (days={args.days}, max_scrolls={max_scrolls})...")
            for i, h in enumerate(handles, 1):
                print(f"\n[{i}/{len(handles)}] @{h}...", end="")
                try:
                    posts = await scraper.scrape_account(page, h, max_scrolls=max_scrolls)
                    all_posts.extend(posts)
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f" Error: {str(e)[:50]}")
                    continue

            if args.deep and all_posts:
                await scraper._deep_scrape_replies(page, all_posts)
            if args.download_media and all_posts:
                await scraper._download_media(all_posts, "explicit_handles")
            scraper.save_to_csv(all_posts, "explicit-handles")

        # Scrape high-signal accounts (ALL by default, no limit unless specified)
        if (args.accounts or args.all) and not args.handles:
            accounts = scraper.load_high_signal_accounts()
            total = len(accounts)
            if args.limit:
                accounts = accounts[:args.limit]
            print(f"\n📡 Scraping {len(accounts)}/{total} high-signal accounts...")

            all_posts = []
            max_scrolls = args.max_scrolls if args.max_scrolls is not None else 5
            for i, account in enumerate(accounts, 1):
                print(f"\n[{i}/{len(accounts)}] @{account['handle']} ({account['signal_quality']})...", end='')
                try:
                    posts = await scraper.scrape_account(page, account['handle'], max_scrolls=max_scrolls)
                    all_posts.extend(posts)
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f" Error: {str(e)[:50]}")
                    continue

            # Deep scrape replies on high-engagement posts
            if args.deep and all_posts:
                await scraper._deep_scrape_replies(page, all_posts)

            # Download media if requested
            if args.download_media and all_posts:
                await scraper._download_media(all_posts, "signal_accounts")

            scraper.save_to_csv(all_posts, 'high-signal-accounts')

        # Meme mode: scrape viral/meme accounts
        if args.meme:
            meme_tweets = await scraper.scrape_meme_accounts(page, args.meme)

            # Always download media for meme accounts
            if not args.download_media:
                await scraper._download_media(meme_tweets, "memes")

            scraper.save_to_csv(meme_tweets, 'meme-accounts')

        # Save full JSON backup with all data (engagement, replies, media URLs)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = OUTPUT_DIR / f"scrape_{timestamp}.json"
        with open(json_path, 'w') as f:
            json.dump(scraper.all_results, f, indent=2, default=str)

        print(f"\n{'='*60}")
        print(f"SCRAPE COMPLETE")
        print(f"{'='*60}")
        print(f"Total entries saved: {len(scraper.all_results)}")
        print(f"CSV: {ALPHA_STAGING}")
        print(f"JSON: {json_path}")
        if args.download_media or args.meme:
            print(f"Media: {MEDIA_DIR}")
        print(f"{'='*60}")

    finally:
        await browser.close()
        await p.stop()


if __name__ == "__main__":
    asyncio.run(main())
