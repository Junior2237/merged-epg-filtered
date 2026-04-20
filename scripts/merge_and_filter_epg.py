import sys, re, gzip, os, shutil, time
from datetime import datetime, timedelta, timezone
import requests
from lxml import etree
from io import BytesIO

# ==========================================
# Provider Base URL
# ==========================================
BASE_URL = "https://epgshare01.online/epgshare01/"

# ==========================================
# PERFORMANCE SETTINGS (FAST LOADING)
# ==========================================
KEEP_PAST_DAYS = 0
KEEP_FUTURE_DAYS = 1   # 🔥 Reduced to 1 day for much faster IPTV loading

# Normalize all programme times to UTC (+0000)
NORMALIZE_TIMES_TO_UTC = True

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; merged-epg/2.0; +https://github.com/Junior2237/merged-epg-filtered)",
    "Accept": "*/*",
}

# ==========================================
# Output + Backup
# ==========================================
OUTPUT_NAME = "merged_epg.xml.gz"
PREVIOUS_DIST = os.path.join("dist", "epg.xml.gz")

# ==========================================
# Selected Sources
# ==========================================
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
    "epg_ripper_FR1.xml.gz",
    "epg_ripper_HK1.xml.gz",
    "epg_ripper_NL1.xml.gz",
    "epg_ripper_TR1.xml.gz",
    "epg_ripper_PL1.xml.gz",
    "epg_ripper_NZ1.xml.gz",
    "epg_ripper_IL1.xml.gz",
    "epg_ripper_DE1.xml.gz",
    "epg_ripper_CZ1.xml.gz",
    "epg_ripper_BG1.xml.gz",
    "epg_ripper_IT1.xml.gz",
]

BASE_URL = BASE_URL.rstrip("/") + "/"
URLS = [BASE_URL + f for f in FILES]


# ==========================================
# Time Handling
# ==========================================
def parse_xmltv_time(ts: str):
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


def format_xmltv_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S") + " +0000"


def normalize_time_string(ts: str) -> str:
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
    return (start_dt <= win_end) and (stop_dt >= win_start)


# ==========================================
# Fetching
# ==========================================
def fetch_xml(url, retries=3):
    last = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=180, headers=HEADERS)
            r.raise_for_status()
            content = r.content

            if content[:2] == b"\x1f\x8b":
                content = gzip.decompress(content)

            return etree.parse(BytesIO(content))
        except Exception as e:
            last = e
            time.sleep(2 * attempt)
    raise last


# ==========================================
# Backup Handling
# ==========================================
def fallback_to_previous():
    if os.path.exists(PREVIOUS_DIST):
        shutil.copyfile(PREVIOUS_DIST, OUTPUT_NAME)
        print("⚠️ Using backup dist/epg.xml.gz")
        sys.exit(0)

    print("❌ No backup exists and all sources failed.")
    sys.exit(1)


def save_backup():
    os.makedirs("dist", exist_ok=True)
    shutil.copyfile(OUTPUT_NAME, PREVIOUS_DIST)
    print("🧯 Backup updated.")


# ==========================================
# Main
# ==========================================
def main():
    now = datetime.now(timezone.utc)
    win_start = now - timedelta(days=KEEP_PAST_DAYS)
    win_end = now + timedelta(days=KEEP_FUTURE_DAYS)

    tv_root = etree.Element("tv")
    channel_ids_seen = set()
    programme_keys_seen = set()
    sources_ok = 0

    for url in URLS:
        try:
            doc = fetch_xml(url)
            sources_ok += 1
        except Exception as e:
            print(f"⚠️ Failed: {url} -> {e}", file=sys.stderr)
            continue

        root = doc.getroot()

        for ch in root.findall("channel"):
            cid = ch.get("id") or ""
            if cid and cid not in channel_ids_seen:
                channel_ids_seen.add(cid)
                tv_root.append(ch)

        for pr in root.findall("programme"):
            ch_id = pr.get("channel") or ""
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
                continue

            title_text = (pr.findtext("title") or "").strip()
            key = (ch_id, start_s, stop_s, title_text)

            if key in programme_keys_seen:
                continue

            programme_keys_seen.add(key)
            tv_root.append(pr)

    if sources_ok == 0 or not programme_keys_seen:
        fallback_to_previous()

    tree = etree.ElementTree(tv_root)
    with gzip.open(OUTPUT_NAME, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True, pretty_print=False)

    save_backup()

    print(f"✅ Done. Sources: {sources_ok}/{len(URLS)} | Channels: {len(channel_ids_seen)} | Programmes: {len(programme_keys_seen)}")


if __name__ == "__main__":
    main()
