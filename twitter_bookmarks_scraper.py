#!/usr/bin/env python3

from __future__ import annotations
"""
TWITTER BOOKMARKS SCRAPER
=========================
Cron: 0 6 * * * cd /Users/macbookpro/Documents/p/PRINTMAXX_STARTER_KITttttt && /Library/Frameworks/Python.framework/Versions/3.12/bin/python3 AUTOMATIONS/twitter_bookmarks_scraper.py --scrape >> AUTOMATIONS/logs/twitter_bookmarks.log 2>&1

Extracts auth cookies (auth_token, ct0) from Brave Browser's SQLite cookie DB,
then uses Twitter's GraphQL Bookmarks endpoint to fetch all bookmarked tweets.
Tracks already-scraped bookmark IDs to avoid duplicates.
Runs alpha extraction on each new bookmark and appends to ALPHA_STAGING.csv.
Saves raw bookmarks to logs for reference.

Usage:
    python3 twitter_bookmarks_scraper.py --scrape          # Fetch & process bookmarks
    python3 twitter_bookmarks_scraper.py --scrape --limit 20  # Limit to 20 bookmarks
    python3 twitter_bookmarks_scraper.py --status          # Show scrape state
"""

import argparse
import csv
import hashlib
import json
import logging
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from Crypto.Cipher import AES
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent.parent
LEDGER_DIR = PROJECT_DIR / "LEDGER"
ALPHA_STAGING = LEDGER_DIR / "ALPHA_STAGING.csv"

LOGS_DIR = PROJECT_DIR / "AUTOMATIONS" / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

STATE_FILE = LOGS_DIR / "twitter_bookmarks_state.json"
RAW_FILE = LOGS_DIR / "twitter_bookmarks_raw.json"
LOG_FILE = LOGS_DIR / "twitter_bookmarks.log"

BRAVE_USER_DATA = Path.home() / "Library/Application Support/BraveSoftware/Brave-Browser"
BRAVE_KEY_FILE = PROJECT_DIR / "AUTOMATIONS" / ".brave_cookie_key"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("bookmarks")

# ---------------------------------------------------------------------------
# ALPHA_STAGING CSV fieldnames (matches existing format)
# ---------------------------------------------------------------------------
FIELDNAMES = [
    "alpha_id", "source", "source_url", "category", "tactic",
    "roi_potential", "priority", "status", "applicable_methods",
    "applicable_niches", "synergy_score", "cross_sell_products",
    "implementation_priority", "engagement_authenticity", "earnings_verified",
    "extracted_method", "compliance_notes", "reviewer_notes",
    "created_at", "ops_generated",
]

# ---------------------------------------------------------------------------
# Cookie extraction (reuses proven pattern from background_twitter_scraper.py)
# ---------------------------------------------------------------------------

def _get_brave_keychain_password() -> str | None:
    """Get Brave Safe Storage password: cached file first, then Keychain."""
    if BRAVE_KEY_FILE.exists():
        cached = BRAVE_KEY_FILE.read_text().strip()
        if cached:
            return cached

    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", "Brave Safe Storage", "-w"],
            capture_output=True, text=True, timeout=15,
        )
        pw = result.stdout.strip()
        if pw:
            BRAVE_KEY_FILE.write_text(pw)
            return pw
    except subprocess.TimeoutExpired:
        log.error("Keychain timed out. Run manually: security find-generic-password -s 'Brave Safe Storage' -w")
    except Exception as e:
        log.error("Keychain failed: %s", e)

    return None


def _decrypt_cookie_value(encrypted_value: bytes, aes_key: bytes) -> str:
    """Decrypt a single Chromium v10 cookie value."""
    if not encrypted_value or encrypted_value[:3] != b"v10":
        return ""
    iv = b" " * 16
    enc_data = encrypted_value[3:]
    if len(enc_data) % 16 != 0:
        enc_data += b"\x00" * (16 - len(enc_data) % 16)
    try:
        cipher = AES.new(aes_key, AES.MODE_CBC, iv)
        decrypted = cipher.decrypt(enc_data)
        pad_len = decrypted[-1]
        if 0 < pad_len <= 16:
            decrypted = decrypted[:-pad_len]
        else:
            decrypted = decrypted.rstrip(b"\x00")
        # Strip 32-byte Brave header if present
        if len(decrypted) > 32:
            value = decrypted[32:].decode("utf-8", errors="replace")
        else:
            value = decrypted.decode("utf-8", errors="replace")
        return value.strip("\x00").strip()
    except Exception:
        return ""


def extract_auth_cookies() -> dict[str, str]:
    """
    Extract auth_token and ct0 cookies from Brave's SQLite cookie DB.
    Returns dict like {"auth_token": "...", "ct0": "..."} or empty dict on failure.
    """
    if not HAS_CRYPTO:
        log.error("pycryptodome not installed. Run: pip3 install pycryptodome")
        return {}

    cookie_db = BRAVE_USER_DATA / "Default" / "Cookies"
    if not cookie_db.exists():
        log.error("Cookie DB not found at %s", cookie_db)
        return {}

    keychain_pass = _get_brave_keychain_password()
    if not keychain_pass:
        log.error("No Brave cookie decryption key available")
        return {}

    aes_key = hashlib.pbkdf2_hmac("sha1", keychain_pass.encode("utf-8"), b"saltysalt", 1003, dklen=16)

    # Copy the DB (Brave locks it while running)
    temp_db = tempfile.mktemp(suffix=".db", prefix="brave_cookies_bkmk_")
    shutil.copy2(str(cookie_db), temp_db)

    auth_cookies: dict[str, str] = {}
    try:
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name, encrypted_value FROM cookies "
            "WHERE (host_key = '.x.com' OR host_key = 'x.com' OR host_key = '.twitter.com') "
            "AND name IN ('auth_token', 'ct0')"
        )
        for name, encrypted_value in cursor.fetchall():
            value = _decrypt_cookie_value(encrypted_value, aes_key)
            if value:
                auth_cookies[name] = value
        conn.close()
    finally:
        Path(temp_db).unlink(missing_ok=True)

    if "auth_token" in auth_cookies and "ct0" in auth_cookies:
        log.info("Extracted auth_token and ct0 from Brave cookies")
    else:
        found = list(auth_cookies.keys())
        log.warning("Missing cookies. Found: %s (need auth_token + ct0)", found)

    return auth_cookies


# ---------------------------------------------------------------------------
# Twitter GraphQL Bookmarks API
# ---------------------------------------------------------------------------

BOOKMARKS_QUERY_ID = "l7ze4soJCcXWF6erzanACA"  # Updated 2026-02-18 from fa0311/TwitterInternalAPIDocument (Feb 9, 2026)
BOOKMARKS_OPERATION = "Bookmarks"

# Features / variables that the Twitter web client sends
# Updated 2026-02-18 from fa0311/TwitterInternalAPIDocument API.json
BOOKMARKS_FEATURES = {
    "rweb_video_screen_enabled": False,
    "profile_label_improvements_pcf_label_in_post_enabled": True,
    "responsive_web_profile_redirect_enabled": False,
    "rweb_tipjar_consumption_enabled": False,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "premium_content_api_read_enabled": False,
    "communities_web_enable_tweet_community_results_fetch": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
    "responsive_web_grok_analyze_post_followups_enabled": False,
    "responsive_web_jetfuel_frame": True,
    "responsive_web_grok_share_attachment_enabled": True,
    "responsive_web_grok_annotations_enabled": True,
    "articles_preview_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "responsive_web_grok_show_grok_translated_post": False,
    "responsive_web_grok_analysis_button_from_backend": True,
    "post_ctas_fetch_enabled": False,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "responsive_web_grok_image_annotation_enabled": True,
    "responsive_web_grok_imagine_annotation_enabled": True,
    "responsive_web_grok_community_note_auto_translation_is_enabled": False,
    "responsive_web_enhance_cards_enabled": False,
}

BOOKMARKS_FIELD_TOGGLES = {
    "withArticlePlainText": False,
}


def _build_bookmarks_url(cursor: str | None = None, count: int = 100) -> str:
    """Build the GraphQL bookmarks endpoint URL."""
    variables: dict = {"count": count, "includePromotedContent": False}
    if cursor:
        variables["cursor"] = cursor

    import urllib.parse
    params = {
        "variables": json.dumps(variables, separators=(",", ":")),
        "features": json.dumps(BOOKMARKS_FEATURES, separators=(",", ":")),
        "fieldToggles": json.dumps(BOOKMARKS_FIELD_TOGGLES, separators=(",", ":")),
    }
    qs = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    return f"https://x.com/i/api/graphql/{BOOKMARKS_QUERY_ID}/{BOOKMARKS_OPERATION}?{qs}"


def _make_request(url: str, auth_cookies: dict[str, str]) -> dict | None:
    """Make an authenticated GET request to Twitter's API."""
    headers = {
        "Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
        "X-Csrf-Token": auth_cookies["ct0"],
        "Cookie": f"auth_token={auth_cookies['auth_token']}; ct0={auth_cookies['ct0']}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "X-Twitter-Active-User": "yes",
        "X-Twitter-Auth-Type": "OAuth2Session",
        "X-Twitter-Client-Language": "en",
        "Referer": "https://x.com/i/bookmarks",
    }

    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            pass
        if e.code == 429:
            log.warning("Rate limited (429). Waiting 60s...")
            time.sleep(60)
            return None
        elif e.code == 401:
            log.error("Auth failed (401). Cookies may be expired. Body: %s", body)
            return None
        else:
            log.error("HTTP %d: %s", e.code, body)
            return None
    except Exception as e:
        log.error("Request failed: %s", e)
        return None


def _extract_tweets_from_response(data: dict) -> tuple[list[dict], str | None]:
    """
    Parse the GraphQL bookmarks response.
    Returns (list_of_tweet_dicts, next_cursor_or_none).
    """
    tweets = []
    next_cursor = None

    try:
        timeline = data["data"]["bookmark_timeline_v2"]["timeline"]
        instructions = timeline.get("instructions", [])
    except (KeyError, TypeError):
        log.warning("Unexpected response structure")
        return tweets, None

    entries = []
    for inst in instructions:
        if inst.get("type") == "TimelineAddEntries":
            entries.extend(inst.get("entries", []))
        elif inst.get("type") == "TimelineReplaceEntry":
            entry = inst.get("entry")
            if entry:
                entries.append(entry)

    for entry in entries:
        entry_id = entry.get("entryId", "")

        # Cursor entries
        if entry_id.startswith("cursor-bottom"):
            content = entry.get("content", {})
            next_cursor = content.get("value") or content.get("itemContent", {}).get("value")
            continue

        # Tweet entries
        try:
            item_content = entry["content"]["itemContent"]
            tweet_results = item_content.get("tweet_results", {}).get("result", {})
        except (KeyError, TypeError):
            continue

        # Handle tweet with visibility results wrapper
        if tweet_results.get("__typename") == "TweetWithVisibilityResults":
            tweet_results = tweet_results.get("tweet", tweet_results)

        tweet = _parse_tweet_result(tweet_results)
        if tweet:
            tweets.append(tweet)

    return tweets, next_cursor


def _parse_tweet_result(result: dict) -> dict | None:
    """Parse a single tweet result into a clean dict."""
    if not result or result.get("__typename") not in ("Tweet", None):
        if result.get("__typename") == "TweetTombstone":
            return None

    try:
        legacy = result.get("legacy", {})
        core = result.get("core", {}).get("user_results", {}).get("result", {})
        user_legacy = core.get("legacy", {})
    except (AttributeError, TypeError):
        return None

    tweet_id = legacy.get("id_str") or result.get("rest_id", "")
    if not tweet_id:
        return None

    full_text = legacy.get("full_text", "")
    screen_name = user_legacy.get("screen_name", "unknown")
    name = user_legacy.get("name", "")
    created_at_str = legacy.get("created_at", "")

    # Engagement counts
    favorite_count = legacy.get("favorite_count", 0)
    retweet_count = legacy.get("retweet_count", 0)
    reply_count = legacy.get("reply_count", 0)
    quote_count = legacy.get("quote_count", 0)
    bookmark_count = legacy.get("bookmark_count", 0)
    views = 0
    try:
        views = int(result.get("views", {}).get("count", 0))
    except (ValueError, TypeError):
        pass

    # Extract URLs from entities
    urls = []
    for url_obj in legacy.get("entities", {}).get("urls", []):
        expanded = url_obj.get("expanded_url", "")
        if expanded:
            urls.append(expanded)

    # Extract media
    media = []
    for m in legacy.get("entities", {}).get("media", []):
        media.append({
            "type": m.get("type", ""),
            "url": m.get("media_url_https", ""),
        })

    tweet_url = f"https://x.com/{screen_name}/status/{tweet_id}"

    return {
        "tweet_id": tweet_id,
        "screen_name": screen_name,
        "name": name,
        "full_text": full_text,
        "tweet_url": tweet_url,
        "created_at": created_at_str,
        "favorite_count": favorite_count,
        "retweet_count": retweet_count,
        "reply_count": reply_count,
        "quote_count": quote_count,
        "bookmark_count": bookmark_count,
        "views": views,
        "urls": urls,
        "media": media,
    }


def fetch_bookmarks(auth_cookies: dict[str, str], limit: int | None = None) -> list[dict]:
    """Fetch all bookmarks using cursor-based pagination."""
    all_tweets = []
    cursor = None
    page = 0

    while True:
        page += 1
        log.info("Fetching bookmarks page %d (cursor=%s)...", page, cursor[:20] if cursor else "None")

        url = _build_bookmarks_url(cursor=cursor, count=100)
        data = _make_request(url, auth_cookies)

        if data is None:
            log.warning("Request returned None on page %d, stopping.", page)
            break

        tweets, next_cursor = _extract_tweets_from_response(data)
        log.info("  Page %d: got %d tweets", page, len(tweets))

        if not tweets:
            log.info("No more tweets returned, done.")
            break

        all_tweets.extend(tweets)

        if limit and len(all_tweets) >= limit:
            all_tweets = all_tweets[:limit]
            log.info("Reached limit of %d bookmarks", limit)
            break

        if not next_cursor or next_cursor == cursor:
            log.info("No more pages (cursor unchanged or missing).")
            break

        cursor = next_cursor
        # Rate limit courtesy pause
        time.sleep(2)

    log.info("Total bookmarks fetched: %d", len(all_tweets))
    return all_tweets


# ---------------------------------------------------------------------------
# State management (dedup tracking)
# ---------------------------------------------------------------------------

def load_state() -> dict:
    """Load scraped tweet IDs from state file."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            log.warning("Corrupt state file, starting fresh")
    return {"scraped_ids": [], "last_run": None, "total_processed": 0}


def save_state(state: dict):
    """Save state to disk."""
    state["last_run"] = datetime.now().isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def save_raw_bookmarks(tweets: list[dict]):
    """Save raw bookmark data for reference."""
    existing = []
    if RAW_FILE.exists():
        try:
            existing = json.loads(RAW_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            existing = []

    # Merge -- keyed by tweet_id to avoid raw duplicates
    existing_ids = {t["tweet_id"] for t in existing}
    for tweet in tweets:
        if tweet["tweet_id"] not in existing_ids:
            existing.append(tweet)
            existing_ids.add(tweet["tweet_id"])

    RAW_FILE.write_text(json.dumps(existing, indent=2, default=str), encoding="utf-8")
    log.info("Raw bookmarks saved: %d total in %s", len(existing), RAW_FILE.name)


# ---------------------------------------------------------------------------
# Alpha extraction
# ---------------------------------------------------------------------------

def categorize_tweet(text: str) -> str:
    """Auto-categorize tweet into alpha category."""
    t = text.lower()
    if any(kw in t for kw in ["cold email", "outbound", "deliverability", "inbox", "smtp", "warmup"]):
        return "COLD_OUTBOUND"
    if any(kw in t for kw in ["app store", "ios", "android", "mobile app", "react native", "pwa"]):
        return "APP_FACTORY"
    if any(kw in t for kw in ["mcp", "claude", "gpt", "llm", "ai agent", "automation", "n8n", "cursor"]):
        return "TOOL_ALPHA"
    if any(kw in t for kw in ["tiktok", "reels", "youtube", "shorts", "content", "viral"]):
        return "CONTENT_FARM"
    if any(kw in t for kw in ["saas", "mrr", "arr", "subscription", "churn"]):
        return "SAAS"
    if any(kw in t for kw in ["seo", "google", "ranking", "traffic", "organic", "programmatic"]):
        return "SEO_GEO"
    if any(kw in t for kw in ["$", "revenue", "profit", "income", "monetiz", "pricing"]):
        return "MONETIZATION"
    if any(kw in t for kw in ["growth", "viral", "followers", "engagement", "algorithm"]):
        return "GROWTH_HACK"
    if any(kw in t for kw in ["ecom", "dropship", "amazon", "etsy", "shopify", "arbitrage"]):
        return "ECOM_ARB"
    if any(kw in t for kw in ["freelance", "upwork", "fiverr", "client", "agency"]):
        return "FREELANCE"
    if any(kw in t for kw in ["crypto", "defi", "token", "memecoin", "web3"]):
        return "CRYPTO"
    return "ALPHA_GENERAL"


def estimate_roi(text: str) -> str:
    """Estimate ROI potential based on content signals."""
    t = text.lower()
    score = 0
    if re.search(r"\$[\d,]+k?", t):
        score += 3
    if re.search(r"\d+%", t):
        score += 2
    if re.search(r"\d+x\b", t):
        score += 2
    if any(kw in t for kw in ["step 1", "how to", "framework", "playbook", "guide", "here's how"]):
        score += 2
    if re.search(r"\.(com|io|ai|app|co)\b", text):
        score += 1
    if re.search(r"(revenue|mrr|arr|profit)\s*[:=]?\s*\$", t):
        score += 2

    if score >= 6:
        return "HIGHEST"
    if score >= 4:
        return "HIGH"
    if score >= 2:
        return "MEDIUM"
    return "LOW"


def extract_alpha_signals(text: str) -> dict:
    """Extract structured alpha signals from tweet text."""
    t = text.lower()
    signals = {
        "tools": [],
        "revenue_numbers": [],
        "handles": [],
        "subreddits": [],
        "urls": [],
        "tactics": [],
    }

    # Tools/products mentioned
    tool_patterns = [
        r"\b(cursor|claude|gpt[-\s]?4|chatgpt|midjourney|dall[-\s]?e|stable\s*diffusion)\b",
        r"\b(vercel|netlify|supabase|firebase|stripe|gumroad|lemon\s*squeezy)\b",
        r"\b(n8n|zapier|make\.com|airtable|notion|obsidian)\b",
        r"\b(playwright|selenium|puppeteer|scrapy|beautifulsoup)\b",
        r"\b(react\s*native|flutter|expo|capacitor|nextjs|next\.js|remix|astro)\b",
        r"\b(beehiiv|substack|convertkit|mailchimp|instantly|smartlead)\b",
        r"\b(figma|canva|framer|webflow|lovable|bolt)\b",
    ]
    for pat in tool_patterns:
        for match in re.finditer(pat, t):
            signals["tools"].append(match.group(0).strip())

    # Revenue / money numbers
    for match in re.finditer(r"\$[\d,]+(?:\.\d+)?(?:k|K|m|M)?(?:/(?:mo|month|yr|year|day|week))?", text):
        signals["revenue_numbers"].append(match.group(0))

    # Twitter handles
    for match in re.finditer(r"@([A-Za-z0-9_]{1,15})", text):
        signals["handles"].append(f"@{match.group(1)}")

    # Subreddits
    for match in re.finditer(r"r/([A-Za-z0-9_]+)", text):
        signals["subreddits"].append(f"r/{match.group(1)}")

    # URLs
    for match in re.finditer(r"https?://[^\s]+", text):
        signals["urls"].append(match.group(0))

    # Tactic keywords
    tactic_kws = ["cold email", "outbound", "seo", "paid ads", "content marketing",
                   "affiliate", "dropship", "arbitrage", "lead gen", "scraping",
                   "automation", "no-code", "prompt engineering", "clipping"]
    for kw in tactic_kws:
        if kw in t:
            signals["tactics"].append(kw)

    # Deduplicate
    for key in signals:
        signals[key] = list(dict.fromkeys(signals[key]))

    return signals


def is_actionable_bookmark(tweet: dict) -> bool:
    """
    Filter bookmarks for actionable content.
    Bookmarks are already curated by the user, so we use a lighter filter
    than timeline scrapers -- but still skip very short or non-English content.
    """
    text = tweet.get("full_text", "")
    if len(text) < 30:
        return False
    return True


# ---------------------------------------------------------------------------
# ALPHA_STAGING integration
# ---------------------------------------------------------------------------

def get_next_alpha_id() -> int:
    """Get next available ALPHA ID from ALPHA_STAGING.csv."""
    max_id = 0
    if ALPHA_STAGING.exists():
        with open(ALPHA_STAGING, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                alpha_id = row.get("alpha_id", "")
                match = re.match(r"ALPHA(\d+)", alpha_id)
                if match:
                    max_id = max(max_id, int(match.group(1)))
    return max_id + 1


def load_existing_urls() -> set[str]:
    """Load existing source_url values from ALPHA_STAGING to avoid dupes."""
    urls = set()
    if ALPHA_STAGING.exists():
        with open(ALPHA_STAGING, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("source_url", "").strip()
                if url:
                    urls.add(url)
    return urls


def append_to_alpha_staging(entries: list[dict]):
    """Append new entries to ALPHA_STAGING.csv."""
    if not entries:
        return

    file_exists = ALPHA_STAGING.exists() and ALPHA_STAGING.stat().st_size > 0
    with open(ALPHA_STAGING, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(entries)

    log.info("Appended %d entries to ALPHA_STAGING.csv", len(entries))


# ---------------------------------------------------------------------------
# Main scrape logic
# ---------------------------------------------------------------------------

def scrape_bookmarks(limit: int | None = None):
    """Main scrape pipeline: extract cookies -> fetch bookmarks -> alpha extract -> save."""

    log.info("=" * 60)
    log.info("TWITTER BOOKMARKS SCRAPER")
    log.info("=" * 60)
    log.info("Time: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # 1. Extract cookies
    auth_cookies = extract_auth_cookies()
    if "auth_token" not in auth_cookies or "ct0" not in auth_cookies:
        log.error("Missing auth cookies. Make sure you are logged into Twitter/X in Brave Browser.")
        return

    # 2. Load state
    state = load_state()
    scraped_ids = set(state.get("scraped_ids", []))
    log.info("Previously scraped: %d bookmark IDs", len(scraped_ids))

    # 3. Fetch bookmarks
    all_bookmarks = fetch_bookmarks(auth_cookies, limit=limit)
    if not all_bookmarks:
        log.info("No bookmarks returned.")
        save_state(state)
        return

    # 4. Save raw data
    save_raw_bookmarks(all_bookmarks)

    # 5. Filter to new bookmarks only
    new_bookmarks = [t for t in all_bookmarks if t["tweet_id"] not in scraped_ids]
    log.info("New bookmarks (not previously scraped): %d / %d", len(new_bookmarks), len(all_bookmarks))

    if not new_bookmarks:
        log.info("All bookmarks already processed. Nothing to do.")
        save_state(state)
        return

    # 6. Filter for actionable content
    actionable = [t for t in new_bookmarks if is_actionable_bookmark(t)]
    log.info("Actionable bookmarks (len >= 30): %d / %d", len(actionable), len(new_bookmarks))

    # 7. Dedup against existing ALPHA_STAGING URLs
    existing_urls = load_existing_urls()
    fresh = [t for t in actionable if t["tweet_url"] not in existing_urls]
    log.info("Fresh (not already in ALPHA_STAGING): %d / %d", len(fresh), len(actionable))

    # 8. Alpha extraction and staging
    next_id = get_next_alpha_id()
    entries = []

    for tweet in fresh:
        text = tweet["full_text"]
        signals = extract_alpha_signals(text)
        category = categorize_tweet(text)
        roi = estimate_roi(text)

        # Build reviewer notes with extracted signals
        notes_parts = [f"Bookmarks scraper {datetime.now().strftime('%Y-%m-%d')}"]
        if signals["tools"]:
            notes_parts.append(f"Tools: {', '.join(signals['tools'][:5])}")
        if signals["revenue_numbers"]:
            notes_parts.append(f"Revenue: {', '.join(signals['revenue_numbers'][:3])}")
        if signals["tactics"]:
            notes_parts.append(f"Tactics: {', '.join(signals['tactics'][:3])}")
        if signals["handles"]:
            notes_parts.append(f"Handles: {', '.join(signals['handles'][:3])}")
        if signals["subreddits"]:
            notes_parts.append(f"Subreddits: {', '.join(signals['subreddits'][:3])}")

        engagement_str = (
            f"Likes:{tweet['favorite_count']} RT:{tweet['retweet_count']} "
            f"Replies:{tweet['reply_count']} Views:{tweet['views']}"
        )
        notes_parts.append(engagement_str)

        extracted_method = ""
        if signals["tools"] or signals["tactics"]:
            parts = signals["tactics"][:2] + signals["tools"][:2]
            extracted_method = " + ".join(parts)

        entry = {
            "alpha_id": f"ALPHA{next_id}",
            "source": f"@{tweet['screen_name']} (bookmark)",
            "source_url": tweet["tweet_url"],
            "category": category,
            "tactic": text[:500],
            "roi_potential": roi,
            "priority": "",
            "status": "PENDING_REVIEW",
            "applicable_methods": "",
            "applicable_niches": "",
            "synergy_score": "",
            "cross_sell_products": "",
            "implementation_priority": "",
            "engagement_authenticity": "UNCHECKED",
            "earnings_verified": "FALSE",
            "extracted_method": extracted_method,
            "compliance_notes": "",
            "reviewer_notes": " | ".join(notes_parts),
            "created_at": datetime.now().isoformat(),
            "ops_generated": "",
        }
        entries.append(entry)
        next_id += 1

    # 9. Write to ALPHA_STAGING
    append_to_alpha_staging(entries)

    # 10. Update state with ALL fetched tweet IDs (even non-actionable ones)
    for t in all_bookmarks:
        scraped_ids.add(t["tweet_id"])
    state["scraped_ids"] = list(scraped_ids)
    state["total_processed"] = state.get("total_processed", 0) + len(entries)
    save_state(state)

    # Summary
    log.info("=" * 60)
    log.info("SCRAPE COMPLETE")
    log.info("=" * 60)
    log.info("Bookmarks fetched: %d", len(all_bookmarks))
    log.info("New (not previously seen): %d", len(new_bookmarks))
    log.info("Actionable: %d", len(actionable))
    log.info("Added to ALPHA_STAGING: %d", len(entries))
    if entries:
        log.info("IDs: ALPHA%d - ALPHA%d", int(entries[0]["alpha_id"].replace("ALPHA", "")),
                 int(entries[-1]["alpha_id"].replace("ALPHA", "")))
    log.info("State file: %s", STATE_FILE)
    log.info("Raw bookmarks: %s", RAW_FILE)


def show_status():
    """Show current scrape state."""
    state = load_state()
    print("TWITTER BOOKMARKS SCRAPER STATUS")
    print("=" * 40)
    print(f"Last run: {state.get('last_run', 'Never')}")
    print(f"Total scraped IDs: {len(state.get('scraped_ids', []))}")
    print(f"Total processed to ALPHA_STAGING: {state.get('total_processed', 0)}")
    print(f"State file: {STATE_FILE}")
    print(f"Raw bookmarks file: {RAW_FILE}")
    if RAW_FILE.exists():
        try:
            raw = json.loads(RAW_FILE.read_text(encoding="utf-8"))
            print(f"Raw bookmarks saved: {len(raw)}")
        except Exception:
            print("Raw bookmarks file: corrupt/unreadable")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Twitter Bookmarks Scraper (Brave cookies)")
    parser.add_argument("--scrape", action="store_true", help="Fetch and process bookmarks")
    parser.add_argument("--limit", type=int, default=None, help="Max bookmarks to fetch")
    parser.add_argument("--status", action="store_true", help="Show scrape state")
    args = parser.parse_args()

    if args.status:
        show_status()
    elif args.scrape:
        scrape_bookmarks(limit=args.limit)
    else:
        parser.print_help()
        print("\nQuick start:")
        print("  python3 twitter_bookmarks_scraper.py --scrape")
        print("  python3 twitter_bookmarks_scraper.py --scrape --limit 20")
        print("  python3 twitter_bookmarks_scraper.py --status")


if __name__ == "__main__":
    main()
