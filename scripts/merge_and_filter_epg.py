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
# UHF SETTINGS
# ==========================================

# Keep a small amount of past EPG.
KEEP_PAST_HOURS = 2

# Give UHF enough future data if its refresh is delayed.
KEEP_FUTURE_HOURS = 48

# Normalize all programme times to UTC.
NORMALIZE_TIMES_TO_UTC = True


# ==========================================
# Programme Metadata
# ==========================================

# IMPORTANT:
# DO NOT remove "icon" or "image".
#
# UHF can use programme artwork URLs from the EPG.
#
# These tags are safe to remove to reduce XML size while
# keeping titles, descriptions, categories, episode data,
# programme icons and programme images.
STRIP_PROGRAMME_TAGS = {
    "credits",
    "rating",
    "star-rating",
    "review",
}


# ==========================================
# HTTP
# ==========================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 "
        "(compatible; merged-epg/5.0; "
        "+https://github.com/Junior2237/merged-epg-filtered)"
    ),
    "Accept": "*/*",
}


# ==========================================
# Output
# ==========================================

DIST_DIR = "dist"

OUTPUT_GZ = os.path.join(
    DIST_DIR,
    "epg.xml.gz"
)

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

URLS = [
    BASE_URL + filename
    for filename in FILES
]


# ==========================================
# XMLTV Time Handling
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

        sign = (
            1
            if tz_string[0] == "+"
            else -1
        )

        hours = int(
            tz_string[1:3]
        )

        minutes = int(
            tz_string[3:5]
        )

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

    return base.replace(
        tzinfo=timezone.utc
    )


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
    if (
        start_dt is None
        and stop_dt is None
    ):
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
# Programme Cleanup
# ==========================================

def strip_unnecessary_metadata(programme):
    for tag in STRIP_PROGRAMME_TAGS:
        elements = programme.findall(tag)

        for element in elements:
            programme.remove(element)


# ==========================================
# Programme Artwork
# ==========================================

def get_existing_icon_url(programme):
    for icon in programme.findall("icon"):
        src = (
            icon.get("src")
            or ""
        ).strip()

        if src.startswith(
            ("http://", "https://")
        ):
            return src

    return None


def get_image_url(programme):
    images = programme.findall("image")

    if not images:
        return None

    # Prefer poster artwork.
    preferred_types = [
        "poster",
        "backdrop",
        "still",
        None,
    ]

    for preferred_type in preferred_types:

        for image in images:

            image_type = image.get("type")

            if preferred_type is not None:
                if image_type != preferred_type:
                    continue

            elif image_type:
                continue

            url = (
                image.text
                or ""
            ).strip()

            if url.startswith(
                ("http://", "https://")
            ):
                return url

    # Fallback to first usable URL.
    for image in images:

        url = (
            image.text
            or ""
        ).strip()

        if url.startswith(
            ("http://", "https://")
        ):
            return url

    return None


def add_icon_from_image(programme):
    """
    Some XMLTV sources use <image>URL</image>
    instead of <icon src="URL"/>.

    If no programme <icon> exists but an <image>
    URL exists, create a compatible <icon>.
    """

    existing_icon = get_existing_icon_url(
        programme
    )

    if existing_icon:
        return False

    image_url = get_image_url(
        programme
    )

    if not image_url:
        return False

    icon = etree.Element(
        "icon"
    )

    icon.set(
        "src",
        image_url
    )

    # XMLTV places <icon> after:
    #
    # title
    # sub-title
    # desc
    # credits
    # date
    # category
    # keyword
    # language
    # orig-language
    # length
    #
    # and before:
    #
    # url
    # country
    # episode-num
    # video
    # audio
    # image
    # etc.

    tags_before_icon = {
        "title",
        "sub-title",
        "desc",
        "credits",
        "date",
        "category",
        "keyword",
        "language",
        "orig-language",
        "length",
    }

    insert_position = 0

    children = list(programme)

    for index, child in enumerate(
        children
    ):
        if child.tag in tags_before_icon:
            insert_position = index + 1

    programme.insert(
        insert_position,
        icon
    )

    return True


def programme_has_artwork(programme):
    if get_existing_icon_url(programme):
        return True

    if get_image_url(programme):
        return True

    return False


# ==========================================
# Download
# ==========================================

def fetch_xml(
    url,
    retries=3
):
    last_error = None

    for attempt in range(
        1,
        retries + 1
    ):
        try:
            response = requests.get(
                url,
                timeout=180,
                headers=HEADERS
            )

            response.raise_for_status()

            content = response.content

            if (
                content[:2]
                == b"\x1f\x8b"
            ):
                content = gzip.decompress(
                    content
                )

            parser = etree.XMLParser(
                recover=True,
                huge_tree=True,
                remove_blank_text=True
            )

            return etree.parse(
                BytesIO(content),
                parser
            )

        except Exception as exc:
            last_error = exc

            print(
                f"Attempt "
                f"{attempt}/{retries} "
                f"failed for {url}: "
                f"{exc}",
                file=sys.stderr
            )

            if attempt < retries:
                time.sleep(
                    2 * attempt
                )

    raise last_error


# ==========================================
# Fallback
# ==========================================

def fallback_to_previous():
    if (
        os.path.exists(OUTPUT_GZ)
        or os.path.exists(
            LEGACY_OUTPUT_GZ
        )
    ):
        print(
            "WARNING: New EPG could not "
            "be generated. Existing EPG preserved."
        )

        sys.exit(0)

    print(
        "ERROR: No valid EPG exists."
    )

    sys.exit(1)


# ==========================================
# Main
# ==========================================

def main():

    now = datetime.now(
        timezone.utc
    )

    window_start = (
        now
        - timedelta(
            hours=KEEP_PAST_HOURS
        )
    )

    window_end = (
        now
        + timedelta(
            hours=KEEP_FUTURE_HOURS
        )
    )

    print(
        "EPG window:"
    )

    print(
        window_start.isoformat()
    )

    print(
        "to"
    )

    print(
        window_end.isoformat()
    )


    channels = []

    programmes = []


    channel_ids_seen = set()

    programme_keys_seen = set()


    sources_ok = 0

    sources_failed = 0


    skipped_time = 0

    skipped_duplicate = 0

    skipped_invalid = 0


    programmes_with_icon = 0

    programmes_with_image = 0

    converted_image_to_icon = 0

    programmes_without_artwork = 0


    # ======================================
    # Process Sources
    # ======================================

    for url in URLS:

        print(
            f"Downloading: {url}"
        )

        try:
            document = fetch_xml(
                url
            )

            sources_ok += 1

        except Exception as exc:

            sources_failed += 1

            print(
                f"WARNING: "
                f"{url} failed: "
                f"{exc}",
                file=sys.stderr
            )

            continue


        root = document.getroot()


        # ==================================
        # Channels
        # ==================================

        for channel in root.findall(
            "channel"
        ):

            channel_id = channel.get(
                "id"
            )

            if not channel_id:
                continue

            if (
                channel_id
                in channel_ids_seen
            ):
                continue

            channel_ids_seen.add(
                channel_id
            )

            channels.append(
                channel
            )


        # ==================================
        # Programmes
        # ==================================

        for programme in root.findall(
            "programme"
        ):

            channel_id = (
                programme.get(
                    "channel"
                )
                or ""
            )

            if not channel_id:

                skipped_invalid += 1

                continue


            start_string = (
                programme.get(
                    "start"
                )
                or ""
            )

            stop_string = (
                programme.get(
                    "stop"
                )
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


            start_dt = (
                parse_xmltv_time(
                    start_string
                )
            )

            stop_dt = (
                parse_xmltv_time(
                    stop_string
                )
            )


            if start_dt is None:

                skipped_invalid += 1

                continue


            if not intersects_window(
                start_dt,
                stop_dt,
                window_start,
                window_end
            ):

                skipped_time += 1

                continue


            title = (
                programme.findtext(
                    "title"
                )
                or ""
            ).strip()


            key = (
                channel_id,
                start_string,
                stop_string,
                title
            )


            if (
                key
                in programme_keys_seen
            ):

                skipped_duplicate += 1

                continue


            programme_keys_seen.add(
                key
            )


            # --------------------------------
            # Preserve artwork
            # --------------------------------

            original_icon = (
                get_existing_icon_url(
                    programme
                )
            )

            original_image = (
                get_image_url(
                    programme
                )
            )


            if original_icon:
                programmes_with_icon += 1


            if original_image:
                programmes_with_image += 1


            # If the provider supplies <image>
            # but no <icon>, create an icon too.
            if (
                not original_icon
                and original_image
            ):

                if add_icon_from_image(
                    programme
                ):
                    converted_image_to_icon += 1


            if not programme_has_artwork(
                programme
            ):
                programmes_without_artwork += 1


            # Remove unnecessary metadata,
            # but NEVER icon/image artwork.
            strip_unnecessary_metadata(
                programme
            )


            programmes.append(
                programme
            )


    # ======================================
    # Validation
    # ======================================

    if sources_ok == 0:
        fallback_to_previous()


    if not channels:
        fallback_to_previous()


    if not programmes:
        fallback_to_previous()


    # ======================================
    # XMLTV Root
    # ======================================

    tv_root = etree.Element(
        "tv",
        attrib={
            "generator-info-name":
                "merged-epg-filtered",

            "generator-info-url":
                (
                    "https://github.com/"
                    "Junior2237/"
                    "merged-epg-filtered"
                )
        }
    )


    # All channels first.

    for channel in channels:

        tv_root.append(
            channel
        )


    # Programmes second.

    for programme in programmes:

        tv_root.append(
            programme
        )


    tree = etree.ElementTree(
        tv_root
    )


    # ======================================
    # Output
    # ======================================

    os.makedirs(
        DIST_DIR,
        exist_ok=True
    )


    old_xml = os.path.join(
        DIST_DIR,
        "epg.xml"
    )


    if os.path.exists(
        old_xml
    ):

        os.remove(
            old_xml
        )


    # Write directly to gzip.
    #
    # Images themselves are NOT stored in
    # the XML file. Only their URLs are kept.

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


    shutil.copyfile(
        OUTPUT_GZ,
        LEGACY_OUTPUT_GZ
    )


    # ======================================
    # Results
    # ======================================

    gz_size_mb = (
        os.path.getsize(
            OUTPUT_GZ
        )
        / 1024
        / 1024
    )


    print("")

    print(
        "=================================="
    )

    print(
        "EPG BUILD COMPLETE"
    )

    print(
        "=================================="
    )


    print(
        f"Sources OK: "
        f"{sources_ok}/"
        f"{len(URLS)}"
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
        f"{skipped_time}"
    )


    print(
        f"Skipped duplicates: "
        f"{skipped_duplicate}"
    )


    print(
        f"Skipped invalid: "
        f"{skipped_invalid}"
    )


    print("")
    print(
        "PROGRAMME ARTWORK"
    )

    print(
        f"Programmes with <icon>: "
        f"{programmes_with_icon}"
    )

    print(
        f"Programmes with <image>: "
        f"{programmes_with_image}"
    )

    print(
        f"Converted image -> icon: "
        f"{converted_image_to_icon}"
    )

    print(
        f"Programmes without artwork: "
        f"{programmes_without_artwork}"
    )


    print("")

    print(
        f"GZIP size: "
        f"{gz_size_mb:.2f} MB"
    )


    print(
        f"EPG output: "
        f"{OUTPUT_GZ}"
    )


if __name__ == "__main__":
    main()
