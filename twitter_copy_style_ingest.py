#!/usr/bin/env python3
"""
Targeted Twitter/X ingest for copy-style + alpha (whitelist handles).

Reads OPS/COPY_STYLE_HANDLES.txt and scrapes those handles via
AUTOMATIONS/twitter_alpha_scraper.py in explicit-handles mode.

This keeps Ship Captain commands simple and avoids brittle shell parsing.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
HANDLES_FILE = BASE_DIR / "OPS" / "COPY_STYLE_HANDLES.txt"


def load_handles() -> list[str]:
    if not HANDLES_FILE.exists():
        return []
    handles: list[str] = []
    for line in HANDLES_FILE.read_text(encoding="utf-8", errors="replace").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        h = s.lstrip("@").strip()
        if not h:
            continue
        handles.append(f"@{h}")
    # de-dupe but keep order
    seen = set()
    out: list[str] = []
    for h in handles:
        key = h.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(h)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="PRINTMAXX targeted X ingest (copy-style handles)")
    ap.add_argument("--days", type=int, default=30, help="Only keep tweets within the last N days (default: 30)")
    ap.add_argument("--max-scrolls", type=int, default=20, help="Max scrolls per handle (default: 20)")
    ap.add_argument("--deep", action="store_true", help="Enable deep reply/funnel analysis")
    ap.add_argument("--download-media", action="store_true", help="Download images (videos logged)")
    args = ap.parse_args()

    handles = load_handles()
    if not handles:
        print(f"twitter_copy_style_ingest: no handles found in {HANDLES_FILE}")
        return 0

    cmd = [
        "python3",
        str(BASE_DIR / "AUTOMATIONS" / "twitter_alpha_scraper.py"),
        "--handles",
        *handles,
        "--days",
        str(int(args.days)),
        "--max-scrolls",
        str(int(args.max_scrolls)),
    ]
    if args.deep:
        cmd.append("--deep")
    if args.download_media:
        cmd.append("--download-media")

    proc = subprocess.run(cmd, cwd=str(BASE_DIR), check=False)
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())

