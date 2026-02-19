import sys, re, gzip, os, shutil, time
from datetime import datetime, timedelta, timezone
import requests
from lxml import etree
from io import BytesIO

# ✅ Your provider base folder (confirmed working)
BASE_URL = "https://epgshare01.online/epgshare01/"

# ✅ Selected sources (your list)
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
]

URLS = [BASE_URL + f for f in FILES]

KEEP_PAST_DAYS = 2
KEEP_FUTURE_DAYS = 7

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; merged-epg/1.0; +https://github.com/Junior2237/merged-epg-filtered)",
    "Accept": "*/*",
}

OUTPUT_NAME = "merged_epg.xml.gz"
PREVIOUS_DIST = os.path.join("dist", "epg.xml.gz")  # your last good output


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


def intersects_window(start_dt, stop_dt, win_start, win_end):
    if not start_dt and not stop_dt:
        return True
    if not start_dt:
        return stop_dt >= win_start
    if not stop_dt:
        return start_dt <= win_end
    return (start_dt <= win_end) and (stop_dt >= win_start)


def fetch_xml(url, retries=3):
    last = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=180, headers=HEADERS)
            r.raise_for_status()
            content = r.content

            # If server returns gz bytes, decompress automatically
            if content[:2] == b"\x1f\x8b":
                content = gzip.decompress(content)

            return etree.parse(BytesIO(content))
        except Exception as e:
            last = e
            time.sleep(2 * attempt)
    raise last


def fallback_to_previous():
    # If we have a previous dist file, copy it and exit success (0)
    if os.path.exists(PREVIOUS_DIST):
        shutil.copyfile(PREVIOUS_DIST, OUTPUT_NAME)
        print("⚠️ All sources failed. Reused previous dist/epg.xml.gz so output stays working.")
        sys.exit(0)

    # If no previous exists, fail clearly
    print("❌ All sources failed and no previous dist/epg.xml.gz exists to reuse.")
    sys.exit(1)


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
            print(f"⚠️ Failed to fetch {url}: {e}", file=sys.stderr)
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

    # If everything failed, fallback
    if sources_ok == 0 or (len(channel_ids_seen) == 0 and len(programme_keys_seen) == 0):
        fallback_to_previous()

    tree = etree.ElementTree(tv_root)
    with gzip.open(OUTPUT_NAME, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True, pretty_print=False)

    print(f"✅ Merge OK. Sources: {sources_ok}/{len(URLS)} | Channels: {len(channel_ids_seen)} | Programmes: {len(programme_keys_seen)}")


if __name__ == "__main__":
    main()
