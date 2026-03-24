"""
EXTRACTION LOG MONITOR
======================
Run this in a separate terminal to watch extraction progress in real-time:
    python extraction_log_monitor.py

It reads logs/extraction.log and shows layer-by-layer status.
Press Ctrl+C to stop.
"""
import json
import time
import sys
import os
from collections import defaultdict, Counter
from datetime import datetime

LOG_FILE = os.path.join(os.path.dirname(__file__), "logs", "extraction.log")
APP_LOG = os.path.join(os.path.dirname(__file__), "logs", "app.log")
ERROR_LOG = os.path.join(os.path.dirname(__file__), "logs", "errors.log")

# Layer classification keywords
LAYER_MAP = {
    "Layer 1: App Discovery": ["PlayStore", "AppStore", "F-Droid", "Microsoft Store", "google-play-scraper"],
    "Layer 2: Web Scraping": ["scrape", "crawl", "website", "fetch_page", "scraping"],
    "Layer 3: ML/NLP": ["NLP", "spacy", "entity", "regex extract"],
    "Layer 4: Dedup/Cache": ["bloom", "cache", "dedup", "duplicate"],
    "Layer 5: OSINT": ["OSINT", "dork", "leadership", "dorking"],
    "Layer 5b: Search Engines": ["ddgs", "DuckDuckGo", "Bing", "search engine", "SearXNG", "circuit breaker"],
    "Layer 6: Email Verification": ["SMTP", "verif", "MX", "deliverability", "warmth"],
    "Layer 7: Social Media": ["social", "linkedin", "github", "hacker", "twitter"],
    "Layer 8: Enrichment": ["enrich", "clearbit", "hunter", "apollo"],
    "Layer 9: Scoring": ["confidence", "score", "scoring", "bounce"],
    "Database": ["MOBIADZ-DB", "persist", "sqlite"],
    "Job Control": ["job", "extraction started", "extraction complete", "cancel"],
}

def classify_log(msg, func=""):
    """Classify a log message into a layer."""
    text = (msg + " " + func).lower()
    for layer, keywords in LAYER_MAP.items():
        for kw in keywords:
            if kw.lower() in text:
                return layer
    return "Other"

def colorize(level):
    colors = {
        "INFO": "\033[92m",     # green
        "WARNING": "\033[93m",  # yellow
        "ERROR": "\033[91m",    # red
        "DEBUG": "\033[94m",    # blue
    }
    reset = "\033[0m"
    return f"{colors.get(level, '')}{level:7s}{reset}"

def print_banner():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=" * 80)
    print("  EXTRACTION LOG MONITOR - Real-time Layer Analysis")
    print("  Watching: logs/app.log + logs/errors.log")
    print("  Press Ctrl+C to stop")
    print("=" * 80)

def tail_and_analyze():
    """Tail the log files and print classified entries."""
    # Track file positions
    positions = {}
    stats = defaultdict(lambda: {"info": 0, "warn": 0, "error": 0, "last": ""})

    for logfile in [APP_LOG, ERROR_LOG]:
        if os.path.exists(logfile):
            # Start from current end of file
            positions[logfile] = os.path.getsize(logfile)
        else:
            positions[logfile] = 0

    print_banner()
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Monitoring started. Waiting for extraction activity...\n")

    try:
        while True:
            new_entries = False
            for logfile in [APP_LOG, ERROR_LOG]:
                if not os.path.exists(logfile):
                    continue

                current_size = os.path.getsize(logfile)
                if current_size <= positions[logfile]:
                    if current_size < positions[logfile]:
                        # File was truncated/rotated
                        positions[logfile] = 0
                    continue

                with open(logfile, 'r', encoding='utf-8', errors='ignore') as f:
                    f.seek(positions[logfile])
                    new_lines = f.readlines()
                    positions[logfile] = f.tell()

                for line in new_lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        log = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    logger_name = log.get("logger", "")
                    # Only show extraction-related logs
                    if not any(kw in logger_name.lower() for kw in ["mobiadz", "extraction", "osint"]):
                        # Also check message content
                        msg = log.get("message", "")
                        if not any(kw in msg for kw in ["PlayStore", "AppStore", "OSINT", "MOBIADZ", "extraction", "dorking", "SMTP verif", "ULTRA DEEP"]):
                            continue

                    msg = log.get("message", "")
                    level = log.get("level", "INFO")
                    func = log.get("function", "")
                    ts = log.get("timestamp", "")[:19].replace("T", " ")

                    layer = classify_log(msg, func)

                    # Update stats
                    if level == "INFO":
                        stats[layer]["info"] += 1
                    elif level == "WARNING":
                        stats[layer]["warn"] += 1
                    elif level == "ERROR":
                        stats[layer]["error"] += 1
                    stats[layer]["last"] = msg[:80]

                    # Print the log entry
                    short_msg = msg[:100]
                    layer_tag = f"[{layer}]"
                    print(f"  {ts} {colorize(level)} {layer_tag:30s} {short_msg}")
                    new_entries = True

            if not new_entries:
                time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n\n" + "=" * 80)
        print("  EXTRACTION SUMMARY")
        print("=" * 80)
        for layer in sorted(stats.keys()):
            s = stats[layer]
            total = s["info"] + s["warn"] + s["error"]
            if s["error"] > s["info"]:
                status = "\033[91mFAILED\033[0m"
            elif s["error"] > 0:
                status = "\033[93mPARTIAL\033[0m"
            else:
                status = "\033[92mOK\033[0m"
            print(f"  {layer:35s} {status:20s}  INFO:{s['info']:4d}  WARN:{s['warn']:4d}  ERR:{s['error']:4d}")
            if s["error"] > 0:
                print(f"    Last error: {s['last']}")
        print("=" * 80)

if __name__ == "__main__":
    tail_and_analyze()
