#!/usr/bin/env python3

from __future__ import annotations
"""
Twitter Content Scraper - Download viral/meme content for repurposing
Scrapes accounts, downloads images + videos, creates repurpose queue.

This is NOT for alpha extraction. This is for:
- Downloading meme/viral images and videos from accounts
- Building a content library for your own accounts
- Creating quote tweet drafts with reply bait captions
- Downloading info/educational content to rework with your own angle

Uses same Brave cookie injection as twitter_alpha_scraper.py.
Brave stays open and untouched.

Usage:
    # Scrape specific accounts
    python3 twitter_content_scraper.py @daquan @pubity @memezar

    # Scrape from a file (one @handle per line)
    python3 twitter_content_scraper.py --from-file meme_accounts.txt

    # Scrape with more depth (more scrolls per account)
    python3 twitter_content_scraper.py @daquan --scrolls 15

    # Scrape + auto-generate quote tweet captions
    python3 twitter_content_scraper.py @daquan --captions

    # Show browser for debugging
    python3 twitter_content_scraper.py @daquan --visible
"""

import asyncio
import json
import csv
import re
import hashlib
import shutil
import sqlite3
import subprocess
import tempfile
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta, timezone
from playwright.async_api import async_playwright
import argparse

try:
    from Crypto.Cipher import AES
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False
    print("WARNING: pycryptodome not installed. Run: pip3 install pycryptodome")

# Paths
PROJECT_DIR = Path(__file__).resolve().parent.parent
MEDIA_DIR = PROJECT_DIR / "AUTOMATIONS" / "media_downloads"
OUTPUT_DIR = PROJECT_DIR / "AUTOMATIONS" / "twitter_scraper_output"
REPURPOSE_QUEUE = PROJECT_DIR / "AUTOMATIONS" / "repurpose_queue.csv"
BRAVE_USER_DATA = Path.home() / "Library/Application Support/BraveSoftware/Brave-Browser"
BRAVE_KEY_FILE = PROJECT_DIR / "AUTOMATIONS" / ".brave_cookie_key"
MEDIA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# JS to extract ALL tweets with media, engagement, and full data
EXTRACT_ALL_JS = """
() => {
    const results = [];
    const articles = document.querySelectorAll('article[data-testid="tweet"]');
    articles.forEach(article => {
        try {
            const link = article.querySelector('a[href*="/status/"]');
            if (!link) return;
            const url = link.href;

            const textElem = article.querySelector('[data-testid="tweetText"]');
            const text = textElem ? textElem.innerText : '';

            // Engagement metrics
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

            // ALL images (not just media - includes profile pics, so filter for media)
            const images = [];
            article.querySelectorAll('img[src*="pbs.twimg.com/media/"]').forEach(img => {
                let src = img.src;
                if (src.includes('?')) src = src.split('?')[0] + '?format=jpg&name=large';
                else src += '?format=jpg&name=large';
                images.push(src);
            });

            // Video detection
            const hasVideo = article.querySelector('video') !== null ||
                           article.querySelector('[data-testid="videoPlayer"]') !== null;

            // GIF detection
            const hasGif = article.querySelector('video[poster*="tweet_video_thumb"]') !== null;

            // Handle
            const handleMatch = url.match(/x\\.com\\/([^/]+)\\/status/);
            const handle = handleMatch ? handleMatch[1] : 'unknown';

            // Timestamp
            const timeElem = article.querySelector('time');
            const timestamp = timeElem ? timeElem.getAttribute('datetime') : '';

            // Content type detection
            let contentType = 'text';
            if (images.length > 0 && hasVideo) contentType = 'mixed';
            else if (hasVideo) contentType = 'video';
            else if (hasGif) contentType = 'gif';
            else if (images.length > 0) contentType = 'image';

            results.push({
                url, text, handle, timestamp,
                likes, retweets, replies, views,
                images, hasVideo, hasGif, contentType,
            });
        } catch(e) {}
    });
    return results;
}
"""


def extract_brave_cookies(domain_filter=".x.com"):
    """Extract and decrypt cookies from Brave's cookie database."""
    if not HAS_CRYPTO:
        return []

    cookie_db = BRAVE_USER_DATA / "Default" / "Cookies"
    if not cookie_db.exists():
        return []

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
        except Exception:
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


async def launch_browser():
    """Launch headless Chromium with Twitter cookies."""
    print("🔑 Extracting Twitter cookies from Brave...")
    cookies = extract_brave_cookies(".x.com")
    cookies += extract_brave_cookies(".twitter.com")

    if not cookies:
        print("ERROR: No cookies. Log into Twitter in Brave first.")
        return None, None, None

    print(f"   {len(cookies)} cookies extracted")

    p = await async_playwright().start()
    browser = await p.chromium.launch(
        headless=True,
        args=["--disable-blink-features=AutomationControlled", "--no-first-run"]
    )
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
    await page.goto("https://x.com/home", wait_until="domcontentloaded", timeout=60000)
    await asyncio.sleep(5)

    if "login" in page.url.lower() or "flow" in page.url.lower():
        print("ERROR: Not logged in. Log into Twitter in Brave first.")
        await browser.close()
        await p.stop()
        return None, None, None

    print("✅ Logged into Twitter\n")
    return p, browser, page


async def scrape_account_content(page, handle, max_scrolls=10):
    """Scrape ALL content from an account (no business filter)."""
    handle = handle.lstrip('@')
    print(f"📥 Scraping @{handle}...")

    await page.goto(f"https://x.com/{handle}", wait_until="domcontentloaded", timeout=60000)
    await asyncio.sleep(3)

    all_tweets = []
    seen_urls = set()

    for scroll in range(max_scrolls):
        tweets = await page.evaluate(EXTRACT_ALL_JS)

        for t in tweets:
            if t['url'] not in seen_urls:
                seen_urls.add(t['url'])
                t['handle'] = handle
                all_tweets.append(t)

        prev_height = await page.evaluate("document.body.scrollHeight")
        await page.evaluate("window.scrollBy(0, 1200)")
        await asyncio.sleep(1.5)
        new_height = await page.evaluate("document.body.scrollHeight")

        if new_height == prev_height and scroll > 2:
            break

    # Sort by engagement (most viral first)
    all_tweets.sort(key=lambda t: t.get('likes', 0), reverse=True)

    stats = {
        'total': len(all_tweets),
        'images': len([t for t in all_tweets if t.get('contentType') == 'image']),
        'videos': len([t for t in all_tweets if t.get('contentType') == 'video']),
        'mixed': len([t for t in all_tweets if t.get('contentType') == 'mixed']),
        'text': len([t for t in all_tweets if t.get('contentType') == 'text']),
    }
    print(f"  {stats['total']} posts: {stats['images']} images, {stats['videos']} videos, {stats['mixed']} mixed, {stats['text']} text-only")

    return all_tweets


def download_media(tweets, handle):
    """Download images from scraped tweets. Videos logged for yt-dlp."""
    handle = handle.lstrip('@')
    save_dir = MEDIA_DIR / handle
    save_dir.mkdir(exist_ok=True)

    img_count = 0
    vid_count = 0

    for tweet in tweets:
        # Download images
        for img_url in tweet.get('images', []):
            try:
                img_match = re.search(r'/media/([^?]+)', img_url)
                filename = img_match.group(1) if img_match else f"img_{img_count}.jpg"
                filepath = save_dir / filename
                if not filepath.exists():
                    urllib.request.urlretrieve(img_url, str(filepath))
                    img_count += 1
            except Exception:
                continue

        # Log video URLs for batch download with yt-dlp
        if tweet.get('hasVideo') or tweet.get('hasGif'):
            video_log = save_dir / "video_urls.txt"
            with open(video_log, 'a') as f:
                f.write(f"{tweet['url']}\n")
            vid_count += 1

    print(f"  📥 {img_count} images saved to {save_dir}/")
    if vid_count > 0:
        print(f"  🎬 {vid_count} video URLs logged. Download with:")
        print(f"     yt-dlp -a '{save_dir}/video_urls.txt' -o '{save_dir}/%(id)s.%(ext)s'")

    return img_count, vid_count


def generate_captions(tweets, handle):
    """Generate quote tweet / reply bait caption suggestions for each post."""
    handle = handle.lstrip('@')
    captions = []

    for t in tweets:
        text = t.get('text', '')
        likes = t.get('likes', 0)
        content_type = t.get('contentType', 'text')

        # Skip text-only posts for repurposing (we want media)
        if content_type == 'text' and not t.get('images'):
            continue

        # Generate caption based on content
        caption_ideas = []

        # Reply bait style
        if likes > 5000:
            caption_ideas.append(f"this is insane. thoughts?")
        elif likes > 1000:
            caption_ideas.append(f"no way this is real")

        # Hot take / add-on style
        if any(kw in text.lower() for kw in ['how', 'way', 'method', 'hack']):
            caption_ideas.append(f"this but also...")
        if any(kw in text.lower() for kw in ['money', '$', 'revenue', 'income']):
            caption_ideas.append(f"the real flex is consistency")
        if any(kw in text.lower() for kw in ['fail', 'mistake', 'wrong']):
            caption_ideas.append(f"needed to hear this")

        # Engagement bait
        caption_ideas.append(f"save this")
        caption_ideas.append(f"bookmark worthy")

        captions.append({
            'source_url': t['url'],
            'source_handle': f"@{handle}",
            'original_text': text[:200],
            'content_type': content_type,
            'likes': likes,
            'views': t.get('views', 0),
            'caption_options': caption_ideas[:3],
            'repurpose_type': 'quote_tweet' if content_type in ('image', 'video', 'mixed') else 'reply_thread',
            'media_downloaded': bool(t.get('images')),
            'status': 'PENDING',
        })

    return captions


def save_repurpose_queue(captions):
    """Append to repurpose_queue.csv."""
    if not captions:
        return

    fieldnames = ['source_url', 'source_handle', 'original_text', 'content_type',
                  'likes', 'views', 'caption_options', 'repurpose_type',
                  'media_downloaded', 'status', 'created_at']

    file_exists = REPURPOSE_QUEUE.exists()
    with open(REPURPOSE_QUEUE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        if not file_exists:
            writer.writeheader()

        for c in captions:
            c['caption_options'] = ' | '.join(c['caption_options'])
            c['created_at'] = datetime.now().isoformat()
            writer.writerow(c)

    print(f"📋 {len(captions)} items added to repurpose queue: {REPURPOSE_QUEUE}")


async def main():
    parser = argparse.ArgumentParser(
        description='Twitter Content Scraper - Download viral content for repurposing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 twitter_content_scraper.py @daquan @pubity @memezar
  python3 twitter_content_scraper.py --from-file meme_accounts.txt
  python3 twitter_content_scraper.py @daquan --scrolls 15 --captions
  python3 twitter_content_scraper.py @daquan --visible
        """
    )
    parser.add_argument('handles', nargs='*', help='Twitter handles to scrape (@handle)')
    parser.add_argument('--from-file', help='File with handles (one per line)')
    parser.add_argument('--scrolls', type=int, default=10, help='Scrolls per account (default 10)')
    parser.add_argument('--captions', action='store_true', help='Generate quote tweet caption suggestions')
    parser.add_argument('--no-download', action='store_true', help='Skip media download')
    parser.add_argument('--visible', action='store_true', help='Show browser (debug)')
    args = parser.parse_args()

    # Collect handles
    handles = [h.lstrip('@') for h in args.handles if h]

    if args.from_file:
        try:
            with open(args.from_file) as f:
                for line in f:
                    h = line.strip().lstrip('@')
                    if h and not h.startswith('#'):
                        handles.append(h)
        except FileNotFoundError:
            print(f"ERROR: File not found: {args.from_file}")
            return

    if not handles:
        parser.print_help()
        print("\nProvide at least one Twitter handle or --from-file")
        return

    # Dedupe
    handles = list(dict.fromkeys(handles))
    print(f"🎯 Scraping {len(handles)} accounts for content repurposing\n")

    p, browser, page = await launch_browser()
    if not page:
        return

    try:
        all_captions = []
        total_images = 0
        total_videos = 0
        all_tweets = []

        for i, handle in enumerate(handles, 1):
            print(f"\n{'='*50}")
            print(f"[{i}/{len(handles)}] @{handle}")
            print(f"{'='*50}")

            tweets = await scrape_account_content(page, handle, max_scrolls=args.scrolls)
            all_tweets.extend(tweets)

            # Download media
            if not args.no_download:
                imgs, vids = download_media(tweets, handle)
                total_images += imgs
                total_videos += vids

            # Generate captions
            if args.captions:
                captions = generate_captions(tweets, handle)
                all_captions.extend(captions)
                print(f"  ✍️  {len(captions)} caption suggestions generated")

            await asyncio.sleep(2)

        # Save repurpose queue
        if all_captions:
            save_repurpose_queue(all_captions)

        # Save full JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = OUTPUT_DIR / f"content_scrape_{timestamp}.json"
        with open(json_path, 'w') as f:
            json.dump(all_tweets, f, indent=2, default=str)

        print(f"\n{'='*60}")
        print(f"CONTENT SCRAPE COMPLETE")
        print(f"{'='*60}")
        print(f"Accounts scraped: {len(handles)}")
        print(f"Total posts found: {len(all_tweets)}")
        print(f"Images downloaded: {total_images}")
        print(f"Videos logged: {total_videos}")
        if all_captions:
            print(f"Caption suggestions: {len(all_captions)}")
            print(f"Repurpose queue: {REPURPOSE_QUEUE}")
        print(f"Full JSON: {json_path}")
        print(f"Media folder: {MEDIA_DIR}/")
        if total_videos > 0:
            print(f"\n🎬 Download all videos at once:")
            for handle in handles:
                vf = MEDIA_DIR / handle / "video_urls.txt"
                if vf.exists():
                    print(f"   yt-dlp -a '{vf}' -o '{MEDIA_DIR / handle}/%(id)s.%(ext)s'")
        print(f"{'='*60}")

    finally:
        await browser.close()
        await p.stop()


if __name__ == "__main__":
    asyncio.run(main())
