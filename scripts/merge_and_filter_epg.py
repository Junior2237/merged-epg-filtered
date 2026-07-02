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
KEEP_FUTURE_HOURS = 24  # was 36 - with no channel filter, every hour here multiplies across all 28 sources

# Tags stripped from each <programme> to cut size. These are rarely rendered by IPTV apps
# and can be some of the heaviest content per entry (esp. <credits> with full cast lists).
# <desc> and <icon> are deliberately kept.
STRIP_PROGRAMME_TAGS = {"credits", "star-rating", "rating", "review"}

NORMALIZE_TIMES_TO_UTC = True

FILTER_BY_M3U = False
M3U_FILE = "playlist.m3u"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; merged-epg/2.0; +https://github.com/Junior2237/merged-epg-filtered)",
    "Accept": "*/*",
}

MAX_WORKERS = 10
REQUEST_TIMEOUT = 60
GZIP_COMPRESSLEVEL = 9
WRITE_UNCOMPRESSED_XML = False

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
    response = session.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS)
    response.raise_for_status()
    content = response.content

    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)

    return content


def fetch_all(urls):
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
    win_end = now + timedelta(hours=KEEP_FUTURE_HOURS)

    if FILTER_BY_M3U:
        allowed_channel_ids = load_m3u_tvg_ids(M3U_FILE)
    else:
        allowed_channel_ids = None

    tv_root = etree.Element("tv")
    channel_ids_seen = set()
    programme_keys_seen = set()

    sources_ok = 0
    skipped_channels = 0
    skipped_programmes = 0

    raw_contents = fetch_all(URLS)
    print(f"Downloads finished in {time.time() - t0:.1f}s")

    for url, content in zip(URLS, raw_contents):
        if content is None:
            continue

        try:
            doc = etree.parse(BytesIO(content))
        except Exception as e:
            print(f"WARNING: Failed to parse: {url} -> {e}", file=sys.stderr)
            continue

        sources_ok += 1
        root = doc.getroot()

        for ch in root.findall("channel"):
            cid = ch.get("id") or ""

            if allowed_channel_ids is not None and cid not in allowed_channel_ids:
                skipped_channels += 1
                continue

            if cid and cid not in channel_ids_seen:
                channel_ids_seen.add(cid)
                tv_root.append(ch)

        for pr in root.findall("programme"):
            ch_id = pr.get("channel") or ""

            if allowed_channel_ids is not None and ch_id not in allowed_channel_ids:
                skipped_programmes += 1
                continue

            start_s = pr.get("start") or ""
            stop_s = pr.get("stop") or ""

            if NORMALIZE_TIMES_TO_UTC:
                if start_s:
                    start_s = normalize_time_string(start_s)
                    pr.set("start", start_s)

                if stop_s:
                    stop_s = normalize_time_string(stop_s)
                    pr.set("stop", stop_s)

            start_dt = parse_xmltv_time(start_s)
            stop_dt = parse_xmltv_time(stop_s)

            if not intersects_window(start_dt, stop_dt, win_start, win_end):
                skipped_programmes += 1
                continue

            title_text = (pr.findtext("title") or "").strip()
            key = (ch_id, start_s, stop_s, title_text)

            if key in programme_keys_seen:
                skipped_programmes += 1
                continue

            for tag in STRIP_PROGRAMME_TAGS:
                for el in pr.findall(tag):
                    pr.remove(el)

            programme_keys_seen.add(key)
            tv_root.append(pr)

        del doc

    if sources_ok == 0 or not programme_keys_seen:
        fallback_to_previous()

    os.makedirs(DIST_DIR, exist_ok=True)

    tree = etree.ElementTree(tv_root)

    if WRITE_UNCOMPRESSED_XML:
        tree.write(
            OUTPUT_XML,
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=False,
        )

    with gzip.GzipFile(OUTPUT_GZ, "wb", compresslevel=GZIP_COMPRESSLEVEL) as gz:
        tree.write(
            gz,
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=False,
        )

    shutil.copyfile(OUTPUT_GZ, LEGACY_OUTPUT_GZ)

    elapsed = time.time() - t0
    gz_size_mb = os.path.getsize(OUTPUT_GZ) / (1024 * 1024)

    print(
        f"Done in {elapsed:.1f}s. Sources: {sources_ok}/{len(URLS)} | "
        f"Channels: {len(channel_ids_seen)} | "
        f"Programmes: {len(programme_keys_seen)} | "
        f"Skipped channels: {skipped_channels} | "
        f"Skipped programmes: {skipped_programmes} | "
        f"Output size: {gz_size_mb:.2f} MB"
    )

    if WRITE_UNCOMPRESSED_XML:
        print(f"XML: {OUTPUT_XML}")
    print(f"GZ: {OUTPUT_GZ}")
    print(f"Legacy GZ: {LEGACY_OUTPUT_GZ}")


if __name__ == "__main__":
    main()
