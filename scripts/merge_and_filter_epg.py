import sys
import re
import gzip
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from io import BytesIO

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from lxml import etree


BASE_URL = "https://epgshare01.online/epgshare01/"

KEEP_PAST_HOURS = 2
KEEP_FUTURE_HOURS = 36

NORMALIZE_TIMES_TO_UTC = True

FILTER_BY_M3U = False
M3U_FILE = "playlist.m3u"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; merged-epg/2.0; +https://github.com/Junior2237/merged-epg-filtered)",
    "Accept": "*/*",
}

# --- Performance knobs ---
MAX_WORKERS = 10          # concurrent downloads instead of sequential
REQUEST_TIMEOUT = 60      # per-attempt timeout (was 180 - too generous, let retries handle it)
GZIP_COMPRESSLEVEL = 9    # 1=fastest/bigger, 9=slowest/smallest. Build runs on GitHub's servers, not your device,
                           # so build speed doesn't affect app load time — smallest file wins here.
WRITE_UNCOMPRESSED_XML = False  # the app only needs the .gz; skip writing plain XML to save I/O

DIST_DIR = "dist"
OUTPUT_XML = os.path.join(DIST_DIR, "epg.xml")
OUTPUT_GZ = os.path.join(DIST_DIR, "epg.xml.gz")
LEGACY_OUTPUT_GZ = "merged_epg.xml.gz"

FILES = [
    "epg_ripper_BEIN1.xml.gz",
    "epg_ripper_BR1.xml.gz",
    "epg_ripper_BR2.xml.gz",
    "epg_ripper_CA2.xml.gz",
    "epg_ripper_UK1.xml.gz",
    "epg_ripper_DELUXEMUSIC1.xml.gz",
    "epg_ripper_DIRECTVSPORTS1.xml.gz",
    "epg_ripper_DISTROTV1.xml.gz",
    "epg_ripper_DRAFTKINGS1.xml.gz",
    "epg_ripper_DUMMY_CHANNELS.xml.gz",
    "epg_ripper_ES1.xml.gz",
    "epg_ripper_FANDUEL1.xml.gz",
    "epg_ripper_FI1.xml.gz",
    "epg_ripper_PEACOCK1.xml.gz",
    "epg_ripper_PLEX1.xml.gz",
    "epg_ripper_POWERNATION1.xml.gz",
    "epg_ripper_RAKUTEN1.xml.gz",
    "epg_ripper_RALLY_TV1.xml.gz",
    "epg_ripper_SPORTKLUB1.xml.gz",
    "epg_ripper_SSPORTPLUS1.xml.gz",
    "epg_ripper_TBNPLUS1.xml.gz",
    "epg_ripper_THESPORTPLUS1.xml.gz",
    "epg_ripper_US2.xml.gz",
    "epg_ripper_US_LOCALS1.xml.gz",
    "epg_ripper_US_SPORTS1.xml.gz",
    "locomotiontv.xml.gz",
    "epg_ripper_CH1.xml.gz",
    "epg_ripper_HK1.xml.gz",
]

BASE_URL = BASE_URL.rstrip("/") + "/"
URLS = [BASE_URL + f for f in FILES]


def make_session():
    """Session with connection pooling + built-in retry/backoff (replaces manual sleep loop)."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def load_m3u_tvg_ids(path):
    if not os.path.exists(path):
        print(f"WARNING: M3U file not found: {path}. No channel filtering applied.")
        return None

    ids = set()

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    for match in re.findall(r'tvg-id="([^"]+)"', text):
        value = match.strip()
        if value:
            ids.add(value)

    print(f"Loaded {len(ids)} tvg-id values from M3U.")
    return ids


def parse_xmltv_time(ts):
    if not ts:
        return None

    m = re.match(r"(\d{14})(?:\s*([+\-]\d{4}|Z))?", ts)
    if not m:
        return None

    base = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
    tz = m.group(2)

    if tz and tz != "Z":
        sign = 1 if tz[0] == "+" else -1
        hours = int(tz[1:3])
        mins = int(tz[3:5])
        offset = timezone(sign * timedelta(hours=hours, minutes=mins))
        return base.replace(tzinfo=offset).astimezone(timezone.utc)

    return base.replace(tzinfo=timezone.utc)


def format_xmltv_utc(dt):
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S") + " +0000"


def normalize_time_string(ts):
    dt = parse_xmltv_time(ts)
    if not dt:
        return ts

    return format_xmltv_utc(dt)


def intersects_window(start_dt, stop_dt, win_start, win_end):
    if not start_dt and not stop_dt:
        return True

    if not start_dt:
        return stop_dt >= win_start

    if not stop_dt:
        return start_dt <= win_end

    return start_dt <= win_end and stop_dt >= win_start


def fetch_bytes(session, url):
    """Just fetch + decompress bytes (parsing happens separately, off the network-wait path)."""
    response = session.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS)
    response.raise_for_status()
    content = response.content

    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)

    return content


def fetch_all(urls):
    """
    Download every source concurrently instead of one-at-a-time.
    Total time ~= slowest single request (bounded by MAX_WORKERS), not the
    sum of all requests. Results stay in original order so merge priority
    (first-seen-wins for channels/programmes) is unchanged.
    """
    session = make_session()
    results = [None] * len(urls)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_to_idx = {
            pool.submit(fetch_bytes, session, url): i
            for i, url in enumerate(urls)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            url = urls[idx]
            try:
                results[idx] = future.result()
            except Exception as e:
                print(f"WARNING: Failed source: {url} -> {e}", file=sys.stderr)
                results[idx] = None

    return results


def fallback_to_previous():
    if os.path.exists(OUTPUT_XML) or os.path.exists(OUTPUT_GZ) or os.path.exists(LEGACY_OUTPUT_GZ):
        print("WARNING: Build failed, but existing output is preserved.")
        sys.exit(0)

    print("ERROR: No valid output exists and no programmes were generated.")
    sys.exit(1)


def main():
    t0 = time.time()
    now = datetime.now(timezone.utc)
    win_start = now - timedelta(hours=KEEP_PAST_HOURS)
    win_end = now +
