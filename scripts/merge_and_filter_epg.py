import sys
import re
import gzip
import os
import shutil
import time
from datetime import datetime, timedelta, timezone
from io import BytesIO

import requests
from lxml import etree


# ==========================================
# Provider
# ==========================================

BASE_URL = "https://epgshare01.online/epgshare01/"


# ==========================================
# UHF OPTIMIZATION
# ==========================================

# Keep a very small amount of old guide data
KEEP_PAST_HOURS = 2

# Enough future data so UHF does not quickly run out
KEEP_FUTURE_HOURS = 36

# Keep all available channels
FILTER_BY_M3U = False

# Normalize XMLTV timestamps to UTC
NORMALIZE_TIMES_TO_UTC = True


# ==========================================
# HTTP
# ==========================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 "
        "(compatible; merged-epg/3.0; "
        "+https://github.com/Junior2237/merged-epg-filtered)"
    ),
    "Accept": "*/*",
}


# ==========================================
# Output
# ==========================================

DIST_DIR = "dist"

OUTPUT_XML = os.path.join(DIST_DIR, "epg.xml")
OUTPUT_GZ = os.path.join(DIST_DIR, "epg.xml.gz")

# Keep this for compatibility with anything
# still using the old root filename.
LEGACY_OUTPUT_GZ = "merged_epg.xml.gz"


# ==========================================
# EPG Sources
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
    "epg_ripper_HK1.xml.gz",
]

BASE_URL = BASE_URL.rstrip("/") + "/"
URLS = [BASE_URL + filename for filename in FILES]


# ==========================================
# XMLTV TIME HANDLING
# ==========================================

def parse_xmltv_time(ts):
    if not ts:
        return None

    match = re.match(
        r"(\d{14})(?:\s*([+\-]\d{4}|Z))?",
        ts
    )

    if not match:
        return None

    base = datetime.strptime(
        match.group(1),
        "%Y%m%d%H%M%S"
    )

    tz_string = match.group(2)

    if tz_string and tz_string != "Z":
        sign = 1 if tz_string[0] == "+" else -1

        hours = int(tz_string[1:3])
        minutes = int(tz_string[3:5])

        offset = timezone(
            sign * timedelta(
                hours=hours,
                minutes=minutes
            )
        )

        return (
            base
            .replace(tzinfo=offset)
            .astimezone(timezone.utc)
        )

    return base.replace(tzinfo=timezone.utc)


def format_xmltv_utc(dt):
    return (
        dt.astimezone(timezone.utc)
        .strftime("%Y%m%d%H%M%S")
        + " +0000"
    )


def normalize_time_string(ts):
    dt = parse_xmltv_time(ts)

    if dt is None:
        return ts

    return format_xmltv_utc(dt)


def intersects_window(
    start_dt,
    stop_dt,
    window_start,
    window_end
):
    if start_dt is None and stop_dt is None:
        return False

    if start_dt is None:
        return stop_dt >= window_start

    if stop_dt is None:
        return start_dt <= window_end

    return (
        start_dt <= window_end
        and stop_dt >= window_start
    )


# ==========================================
# DOWNLOAD
# ==========================================

def fetch_xml(url, retries=3):
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(
                url,
                timeout=180,
                headers=HEADERS
            )

            response.raise_for_status()

            content = response.content

            # Detect gzip by magic bytes
            if content[:2] == b"\x1f\x8b":
                content = gzip.decompress(content)

            parser = etree.XMLParser(
                recover=True,
                huge_tree=True
            )

            return etree.parse(
                BytesIO(content),
                parser
            )

        except Exception as exc:
            last_error = exc

            print(
                f"Attempt {attempt}/{retries} failed "
                f"for {url}: {exc}",
                file=sys.stderr
            )

            if attempt < retries:
                time.sleep(2 * attempt)

    raise last_error


# ==========================================
# FALLBACK
# ==========================================

def fallback_to_previous():
    if (
        os.path.exists(OUTPUT_GZ)
        or os.path.exists(LEGACY_OUTPUT_GZ)
    ):
        print(
            "WARNING: No new valid EPG was generated. "
            "Existing EPG files are being preserved."
        )
        sys.exit(0)

    print(
        "ERROR: No programmes were generated "
        "and no previous EPG exists."
    )

    sys.exit(1)


# ==========================================
# MAIN
# ==========================================

def main():
    now = datetime.now(timezone.utc)

    window_start = now - timedelta(
        hours=KEEP_PAST_HOURS
    )

    window_end = now + timedelta(
        hours=KEEP_FUTURE_HOURS
    )

    print(
        "EPG window:",
        window_start.isoformat(),
        "->",
        window_end.isoformat()
    )

    # Important:
    # channels and programmes are kept separately.
    #
    # XMLTV expects:
    #
    # <tv>
    #   <channel />
    #   <channel />
    #   ...
    #   <programme />
    #   <programme />
    # </tv>

    channels = []
    programmes = []

    channel_ids_seen = set()
    programme_keys_seen = set()

    sources_ok = 0
    sources_failed = 0

    skipped_programmes_time = 0
    skipped_programmes_duplicate = 0

    for url in URLS:
        print(f"Downloading: {url}")

        try:
            document = fetch_xml(url)
            sources_ok += 1

        except Exception as exc:
            sources_failed += 1

            print(
                f"WARNING: Failed source: "
                f"{url} -> {exc}",
                file=sys.stderr
            )

            continue

        root = document.getroot()

        # ----------------------------------
        # CHANNELS
        # ----------------------------------

        for channel in root.findall("channel"):
            channel_id = channel.get("id")

            if not channel_id:
                continue

            if channel_id in channel_ids_seen:
                continue

            channel_ids_seen.add(channel_id)

            # Detach safely from source tree
            channels.append(channel)

        # ----------------------------------
        # PROGRAMMES
        # ----------------------------------

        for programme in root.findall("programme"):
            channel_id = (
                programme.get("channel")
                or ""
            )

            start_string = (
                programme.get("start")
                or ""
            )

            stop_string = (
                programme.get("stop")
                or ""
            )

            if NORMALIZE_TIMES_TO_UTC:
                if start_string:
                    start_string = (
                        normalize_time_string(
                            start_string
                        )
                    )

                    programme.set(
                        "start",
                        start_string
                    )

                if stop_string:
                    stop_string = (
                        normalize_time_string(
                            stop_string
                        )
                    )

                    programme.set(
                        "stop",
                        stop_string
                    )

            start_dt = parse_xmltv_time(
                start_string
            )

            stop_dt = parse_xmltv_time(
                stop_string
            )

            if not intersects_window(
                start_dt,
                stop_dt,
                window_start,
                window_end
            ):
                skipped_programmes_time += 1
                continue

            title = (
                programme.findtext("title")
                or ""
            ).strip()

            key = (
                channel_id,
                start_string,
                stop_string,
                title
            )

            if key in programme_keys_seen:
                skipped_programmes_duplicate += 1
                continue

            programme_keys_seen.add(key)

            programmes.append(programme)

    # ======================================
    # VALIDATION
    # ======================================

    if sources_ok == 0:
        fallback_to_previous()

    if not channels:
        fallback_to_previous()

    if not programmes:
        fallback_to_previous()

    # ======================================
    # CREATE CORRECT XMLTV ORDER
    # ======================================

    tv_root = etree.Element(
        "tv",
        attrib={
            "generator-info-name": "merged-epg-filtered",
            "generator-info-url": (
                "https://github.com/"
                "Junior2237/"
                "merged-epg-filtered"
            )
        }
    )

    # ALL channels first
    for channel in channels:
        tv_root.append(channel)

    # THEN all programmes
    for programme in programmes:
        tv_root.append(programme)

    tree = etree.ElementTree(tv_root)

    # ======================================
    # OUTPUT
    # ======================================

    os.makedirs(
        DIST_DIR,
        exist_ok=True
    )

    tree.write(
        OUTPUT_XML,
        encoding="utf-8",
        xml_declaration=True,
        pretty_print=False
    )

    with gzip.open(
        OUTPUT_GZ,
        "wb",
        compresslevel=9
    ) as gz_file:

        tree.write(
            gz_file,
            encoding="utf-8",
            xml_declaration=True,
            pretty_print=False
        )

    # Legacy/root copy
    shutil.copyfile(
        OUTPUT_GZ,
        LEGACY_OUTPUT_GZ
    )

    # ======================================
    # RESULT
    # ======================================

    xml_size_mb = (
        os.path.getsize(OUTPUT_XML)
        / 1024
        / 1024
    )

    gz_size_mb = (
        os.path.getsize(OUTPUT_GZ)
        / 1024
        / 1024
    )

    print("")
    print("====================================")
    print("EPG BUILD COMPLETE")
    print("====================================")

    print(
        f"Sources OK: "
        f"{sources_ok}/{len(URLS)}"
    )

    print(
        f"Sources failed: "
        f"{sources_failed}"
    )

    print(
        f"Channels: "
        f"{len(channel_ids_seen)}"
    )

    print(
        f"Programmes: "
        f"{len(programme_keys_seen)}"
    )

    print(
        f"Skipped by time: "
        f"{skipped_programmes_time}"
    )

    print(
        f"Skipped duplicates: "
        f"{skipped_programmes_duplicate}"
    )

    print(
        f"XML size: "
        f"{xml_size_mb:.2f} MB"
    )

    print(
        f"GZIP size: "
        f"{gz_size_mb:.2f} MB"
    )

    print("")
    print(f"XML: {OUTPUT_XML}")
    print(f"GZIP: {OUTPUT_GZ}")
    print(
        f"Compatibility copy: "
        f"{LEGACY_OUTPUT_GZ}"
    )


if __name__ == "__main__":
    main()
