#!/usr/bin/env python3
"""
End-to-end pipeline (DML-only for now).

Current mode:
  - Reads local places JSON (ktb_res_*.json) and menu JSON (uploaded/*_menu.json).
  - Matches by normalized store name.
  - Emits per-restaurant INSERT blocks to a single DML file.

Future extensions:
  - Places API fetch, Naver menu crawl, cache usage.
"""
import argparse
import glob
import json
import os
import re
import time
import urllib.request
import warnings
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any

# Silence urllib3 OpenSSL/LibreSSL warning (environment issue)
warnings.filterwarnings("ignore", message=".*NotOpenSSLWarning.*")

def norm_name(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[()\[\]{}<>\\-_/.,'\"\u2019]+", "", s)
    return s


def safe_str(s: str) -> str:
    if s is None:
        return ""
    return s.replace("'", "''")


def is_ui_noise_text(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    ui_phrases = (
        "페이지 닫기",
        "이미지 갯수",
        "알림받기",
        "출발",
        "도착",
        "저장",
        "거리뷰",
        "공유",
        "동영상",
        "플레이스 플러스",
        "별점",
        "문의",
        "방문자 리뷰",
        "블로그 리뷰",
    )
    if any(p in s for p in ui_phrases):
        return True
    # review count patterns (e.g., "방문자 리뷰 19블로그 리뷰 2")
    if re.search(r"방문자\s*리뷰\s*\d+", s) and "리뷰" in s:
        return True
    return False


def is_store_name_with_category(raw_name: str, store_name: str) -> bool:
    if not raw_name or not store_name:
        return False
    n_raw = norm_name(raw_name)
    n_store = norm_name(store_name)
    if not n_raw.startswith(n_store):
        return False
    tail = raw_name[len(store_name) :].strip()
    if not tail:
        return True
    # common category suffix patterns
    if "," in tail:
        return True
    if re.search(r"(육류|고기요리|카페|일식|중식|양식|한식|분식|술집|해물|횟집|생선|치킨|피자)", tail):
        return True
    return False


def should_swap_section_item(section_name: str, items: List[dict]) -> bool:
    if not section_name or not items or len(items) != 1:
        return False
    item = items[0] or {}
    raw_name = (item.get("name") or "").strip()
    raw_desc = (item.get("description") or "").strip()
    if raw_desc:
        return False
    if len(raw_name) < 40:
        return False
    return True


@dataclass
class Place:
    place_id: Optional[str]
    name: str
    formatted_address: str
    lat: float
    lng: float
    opening_hours: Optional[dict]


def load_places(paths: List[str], override_path: Optional[str] = None) -> Dict[str, Place]:
    places = []
    for path in paths:
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for pl in data.get("places", []):
            name = (pl.get("displayName") or {}).get("text")
            if not name:
                continue
            places.append(pl)
    if override_path and os.path.exists(override_path):
        with open(override_path, "r", encoding="utf-8") as f:
            overrides = json.load(f)
        for key, pl in overrides.items():
            name = (pl.get("displayName") or {}).get("text") or key
            if not name:
                continue
            base = {
                "displayName": {"text": name},
                "formattedAddress": pl.get("formattedAddress"),
                "location": pl.get("location"),
                "openingHours": pl.get("openingHours"),
            }
            places.append(base)
            if key and key != name:
                places.append(
                    {
                        "displayName": {"text": key},
                        "formattedAddress": pl.get("formattedAddress"),
                        "location": pl.get("location"),
                        "openingHours": pl.get("openingHours"),
                    }
                )

    name_map: Dict[str, Place] = {}
    for pl in places:
        key = norm_name((pl.get("displayName") or {}).get("text"))
        if not key or key in name_map:
            continue
        loc = pl.get("location") or {}
        lat = loc.get("latitude")
        lng = loc.get("longitude")
        if lat is None or lng is None:
            continue
        place = Place(
            place_id=pl.get("id"),
            name=(pl.get("displayName") or {}).get("text") or "",
            formatted_address=pl.get("formattedAddress") or "",
            lat=lat,
            lng=lng,
            opening_hours=pl.get("openingHours"),
        )
        name_map[key] = place
    return name_map


def map_periods_to_weekly(periods: List[dict]) -> Dict[int, Tuple[Optional[str], Optional[str], bool]]:
    # Google: day 0=Sunday ... 6=Saturday
    # DML: day_of_week 1=Mon ... 7=Sun
    day_map = {0: 7, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6}
    by_day: Dict[int, Tuple[str, Optional[str]]] = {}
    for p in periods:
        o = p.get("open")
        c = p.get("close")
        if not o:
            continue
        day = o.get("day")
        if day is None or day in by_day:
            continue
        open_time = f"{o.get('hour', 0):02d}:{o.get('minute', 0):02d}"
        close_time = None
        if c:
            close_time = f"{c.get('hour', 0):02d}:{c.get('minute', 0):02d}"
        by_day[day] = (open_time, close_time)

    weekly: Dict[int, Tuple[Optional[str], Optional[str], bool]] = {}
    for g_day, d_day in day_map.items():
        if g_day in by_day:
            weekly[d_day] = (*by_day[g_day], False)
        else:
            weekly[d_day] = (None, None, True)
    return weekly


class DMLWriter:
    def __init__(self, out_path: str):
        self.out_path = out_path
        self.lines: List[str] = []

    def append_header(self, lat: float, lng: float, radius: Optional[float], rank: str) -> None:
        self.lines.append("-- auto-generated DML\n")
        meta = f"-- input lat={lat}, lng={lng}"
        if radius is not None:
            meta += f", radius={radius}"
        if rank:
            meta += f", rank={rank}"
        self.lines.append(meta + "\n\n")

    def append_restaurant_block(
        self,
        rid: int,
        addr_id: int,
        sched_rows: List[str],
        menu_category_rows: List[str],
        menu_rows: List[str],
        name: str,
        full_addr: str,
        lng: float,
        lat: float,
    ) -> None:
        self.lines.append(
            "INSERT INTO restaurant (\n"
            "  id, name, full_address, location, deleted_at, created_at, updated_at\n"
            ") VALUES\n"
            f"({rid}, '{name}', '{full_addr}', "
            f"ST_GeomFromText('POINT({lng} {lat})', 4326), "
            "NULL, now(), now());\n\n"
        )

        self.lines.append(
            "INSERT INTO restaurant_address (\n"
            "  id, restaurant_id, sido, sigungu, eupmyeondong, postal_code,\n"
            "  created_at, updated_at\n"
            ") VALUES\n"
            f"({addr_id}, {rid}, NULL, NULL, NULL, NULL, now(), now());\n\n"
        )

        if sched_rows:
            self.lines.append(
                "INSERT INTO restaurant_weekly_schedule (\n"
                "  id, restaurant_id, day_of_week,\n"
                "  open_time, close_time, is_closed,\n"
                "  effective_from, effective_to,\n"
                "  created_at, updated_at\n"
                ") VALUES\n"
                + ",\n".join(sched_rows)
                + ";\n\n"
            )

        if menu_category_rows:
            self.lines.append(
                "INSERT INTO menu_category (\n"
                "  id, restaurant_id, name, display_order,\n"
                "  created_at, updated_at\n"
                ") VALUES\n"
                + ",\n".join(menu_category_rows)
                + ";\n\n"
            )

        if menu_rows:
            self.lines.append(
                "INSERT INTO menu (\n"
                "  id, category_id, name, description,\n"
                "  price, image_url,\n"
                "  is_recommended, display_order,\n"
                "  created_at, updated_at\n"
                ") VALUES\n"
                + ",\n".join(menu_rows)
                + ";\n\n"
            )

    def write(self, dry_run: bool = False) -> None:
        if dry_run:
            return
        with open(self.out_path, "w", encoding="utf-8") as f:
            f.write("".join(self.lines))


def write_report(report_path: Optional[str], report: dict, dry_run: bool) -> None:
    if not report_path or dry_run:
        return
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


def build_dml_from_local(
    menus_dir: str,
    places_glob: str,
    out_path: str,
    override_json: Optional[str],
    start_id: int,
    lat: float,
    lng: float,
    dry_run: bool,
    report_path: Optional[str],
) -> dict:
    place_paths = sorted(glob.glob(places_glob))
    name_map = load_places(place_paths, override_json)

    menu_files = sorted(f for f in os.listdir(menus_dir) if f.endswith("_menu.json"))

    rid = start_id
    addr_id = rid + 100
    sched_id = rid + 200
    menu_cat_id = rid + 400
    menu_id = rid + 800

    writer = DMLWriter(out_path)
    writer.append_header(lat, lng, radius=None, rank="LOCAL")

    skipped = 0
    appended = 0
    skipped_items: List[Dict[str, Any]] = []
    total = len(menu_files)
    for idx, fname in enumerate(menu_files, 1):
        fpath = os.path.join(menus_dir, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            menu = json.load(f)
        store = menu.get("store_name") or ""
        if not store:
            skipped += 1
            skipped_items.append({"reason": "missing_store_name", "file": fpath})
            print(f"[{idx}/{total}] skipped (missing_store_name) file={fname}")
            continue
        key = norm_name(store)
        place = name_map.get(key)
        if not place:
            skipped += 1
            skipped_items.append({"reason": "place_not_found", "store": store, "file": fpath})
            print(f"[{idx}/{total}] skipped (place_not_found) store={store}")
            continue

        name = safe_str(store)
        full_addr = safe_str(place.formatted_address or "")
        if place.lat is None or place.lng is None:
            skipped += 1
            skipped_items.append({"reason": "missing_location", "store": store, "file": fpath})
            print(f"[{idx}/{total}] skipped (missing_location) store={store}")
            continue

        sched_rows: List[str] = []
        menu_category_rows: List[str] = []
        menu_rows: List[str] = []

        opening = menu.get("opening_hours") or {}
        regular = opening.get("regularOpeningHours") or {}
        periods = regular.get("periods") or []
        if periods:
            weekly = map_periods_to_weekly(periods)
            for day in range(1, 8):
                open_time, close_time, is_closed = weekly[day]
                o = f"'{open_time}'" if open_time else "NULL"
                c = f"'{close_time}'" if close_time else "NULL"
                sched_rows.append(
                    f"({sched_id}, {rid}, {day}, {o}, {c}, "
                    f"{'true' if is_closed else 'false'}, '2024-01-01', NULL, now(), now())"
                )
                sched_id += 1

        sections = menu.get("sections") or []
        if sections:
            for i, sec in enumerate(sections):
                raw_sec_name = sec.get("name") or "메뉴"
                items = sec.get("items") or []
                if is_ui_noise_text(raw_sec_name):
                    continue
                if should_swap_section_item(raw_sec_name, items):
                    raw_sec_name = "메뉴"
                sec_name = safe_str(raw_sec_name)
                menu_category_rows.append(
                    f"({menu_cat_id}, {rid}, '{sec_name}', {i}, now(), now())"
                )
                menu_cat_id += 1

            cat_base = menu_cat_id - len(menu_category_rows)
            for i, sec in enumerate(sections):
                raw_sec_name = sec.get("name") or "메뉴"
                items = sec.get("items") or []
                if is_ui_noise_text(raw_sec_name):
                    continue
                swap = should_swap_section_item(raw_sec_name, items)
                if swap:
                    raw_sec_name = "메뉴"
                cat_id = cat_base + i
                for j, item in enumerate(items):
                    raw_name = item.get("name") or ""
                    raw_desc = item.get("description") or ""
                    if swap and raw_name and not raw_desc:
                        raw_name, raw_desc = sec.get("name") or raw_name, raw_name
                    if not raw_name or raw_name == "(메뉴명 없음)":
                        continue
                    if is_ui_noise_text(raw_name):
                        continue
                    if raw_name == store or is_store_name_with_category(raw_name, store):
                        continue
                    if is_ui_noise_text(raw_desc):
                        continue
                    m_name = safe_str(raw_name)
                    m_desc = safe_str(raw_desc)
                    price_val = item.get("price_value")
                    price = price_val if isinstance(price_val, int) else "NULL"
                    menu_rows.append(
                        f"({menu_id}, {cat_id}, '{m_name}', "
                        f"'{m_desc}', {price}, NULL, false, {j}, now(), now())"
                    )
                    menu_id += 1

        writer.append_restaurant_block(
            rid=rid,
            addr_id=addr_id,
            sched_rows=sched_rows,
            menu_category_rows=menu_category_rows,
            menu_rows=menu_rows,
            name=name,
            full_addr=full_addr,
            lng=place.lng,
            lat=place.lat,
        )

        rid += 1
        addr_id += 1
        appended += 1
        print(f"[{idx}/{total}] appended store={store}")

    writer.write(dry_run=dry_run)
    report = {
        "mode": "local",
        "total_menu_files": len(menu_files),
        "appended_restaurants": appended,
        "skipped_restaurants": skipped,
        "skipped_items": skipped_items,
    }
    write_report(report_path, report, dry_run)
    return report


def _safe_filename(name: str) -> str:
    if not name or not name.strip():
        return "menu"
    s = name.strip()
    for c in r'\/:*?"<>|':
        s = s.replace(c, "")
    s = s.replace(" ", "_")
    return s or "menu"


def fetch_places_nearby(
    api_key: str,
    lat: float,
    lng: float,
    radius: float,
    rank: str,
    lang: str,
    place_type: str,
    max_count: int,
) -> dict:
    url = "https://places.googleapis.com/v1/places:searchNearby"
    payload = {
        "includedTypes": [place_type],
        "maxResultCount": max_count,
        "languageCode": lang,
        "rankPreference": rank,
        "locationRestriction": {
            "circle": {
                "center": {"latitude": lat, "longitude": lng},
                "radius": radius,
            }
        },
    }
    field_mask = (
        "places.displayName,places.id,places.formattedAddress,places.location,"
        "places.primaryType,places.types,places.googleMapsUri,places.businessStatus,"
        "places.shortFormattedAddress,places.plusCode,places.viewport,places.photos,"
        "places.currentOpeningHours,places.currentSecondaryOpeningHours,"
        "places.regularOpeningHours,places.regularSecondaryOpeningHours"
    )
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": field_mask,
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def places_from_response(data: dict) -> List[Place]:
    out: List[Place] = []
    for pl in data.get("places", []) or []:
        name = (pl.get("displayName") or {}).get("text") or ""
        loc = pl.get("location") or {}
        lat = loc.get("latitude")
        lng = loc.get("longitude")
        if not name or lat is None or lng is None:
            continue
        out.append(
            Place(
                place_id=pl.get("id"),
                name=name,
                formatted_address=pl.get("formattedAddress") or "",
                lat=lat,
                lng=lng,
                opening_hours=pl.get("openingHours"),
            )
        )
    return out


def get_menu_for_place(
    place: Place,
    cache_dir: str,
    use_naver: bool,
    sleep_sec: float,
) -> Optional[dict]:
    os.makedirs(os.path.join(cache_dir, "menu"), exist_ok=True)
    safe = _safe_filename(place.name)
    cache_path = os.path.join(cache_dir, "menu", f"{safe}.json")
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    if not use_naver:
        return None
    try:
        from menu_crawling import menu as naver_menu
        from menu_crawling import parse_menu_to_json
    except Exception as e:
        raise SystemExit(f"menu_crawling import 실패: {e}")
    raw = naver_menu(place.name)
    if raw == -1:
        return None
    data = parse_menu_to_json(raw)
    if not data.get("store_name"):
        data["store_name"] = place.name
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    if sleep_sec:
        time.sleep(sleep_sec)
    return data


def build_dml_from_api(
    api_key: str,
    lat: float,
    lng: float,
    radius: float,
    rank: str,
    lang: str,
    place_type: str,
    max_count: int,
    cache_dir: str,
    out_path: str,
    start_id: int,
    use_naver: bool,
    sleep_sec: float,
    dry_run: bool,
    report_path: Optional[str],
) -> dict:
    os.makedirs(cache_dir, exist_ok=True)
    cache_name = f"places_{lat}_{lng}_{radius}_{rank}_{place_type}_{lang}.json"
    cache_path = os.path.join(cache_dir, cache_name)

    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = fetch_places_nearby(api_key, lat, lng, radius, rank, lang, place_type, max_count)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    places = places_from_response(data)

    rid = start_id
    addr_id = rid + 100
    sched_id = rid + 200
    menu_cat_id = rid + 400
    menu_id = rid + 800

    writer = DMLWriter(out_path)
    writer.append_header(lat, lng, radius=radius, rank=rank)

    skipped = 0
    appended = 0
    skipped_items: List[Dict[str, Any]] = []
    total = len(places)
    for idx, place in enumerate(places, 1):
        menu = get_menu_for_place(place, cache_dir, use_naver, sleep_sec)
        if not menu:
            skipped += 1
            skipped_items.append(
                {
                    "reason": "menu_not_found",
                    "place_name": place.name,
                    "place_id": place.place_id,
                }
            )
            print(f"[{idx}/{total}] skipped (menu_not_found) place={place.name}")
            continue

        sched_rows: List[str] = []
        menu_category_rows: List[str] = []
        menu_rows: List[str] = []

        opening = place.opening_hours or {}
        regular = (opening or {}).get("regularOpeningHours") or {}
        periods = regular.get("periods") or []
        if periods:
            weekly = map_periods_to_weekly(periods)
            for day in range(1, 8):
                open_time, close_time, is_closed = weekly[day]
                o = f"'{open_time}'" if open_time else "NULL"
                c = f"'{close_time}'" if close_time else "NULL"
                sched_rows.append(
                    f"({sched_id}, {rid}, {day}, {o}, {c}, "
                    f"{'true' if is_closed else 'false'}, '2024-01-01', NULL, now(), now())"
                )
                sched_id += 1

        sections = menu.get("sections") or []
        if sections:
            for i, sec in enumerate(sections):
                raw_sec_name = sec.get("name") or "메뉴"
                items = sec.get("items") or []
                if is_ui_noise_text(raw_sec_name):
                    continue
                if should_swap_section_item(raw_sec_name, items):
                    raw_sec_name = "메뉴"
                sec_name = safe_str(raw_sec_name)
                menu_category_rows.append(
                    f"({menu_cat_id}, {rid}, '{sec_name}', {i}, now(), now())"
                )
                menu_cat_id += 1

            cat_base = menu_cat_id - len(menu_category_rows)
            for i, sec in enumerate(sections):
                raw_sec_name = sec.get("name") or "메뉴"
                items = sec.get("items") or []
                if is_ui_noise_text(raw_sec_name):
                    continue
                swap = should_swap_section_item(raw_sec_name, items)
                if swap:
                    raw_sec_name = "메뉴"
                cat_id = cat_base + i
                for j, item in enumerate(items):
                    raw_name = item.get("name") or ""
                    raw_desc = item.get("description") or ""
                    if swap and raw_name and not raw_desc:
                        raw_name, raw_desc = sec.get("name") or raw_name, raw_name
                    if not raw_name or raw_name == "(메뉴명 없음)":
                        continue
                    if is_ui_noise_text(raw_name):
                        continue
                    if raw_name == place.name or is_store_name_with_category(raw_name, place.name):
                        continue
                    if is_ui_noise_text(raw_desc):
                        continue
                    m_name = safe_str(raw_name)
                    m_desc = safe_str(raw_desc)
                    price_val = item.get("price_value")
                    price = price_val if isinstance(price_val, int) else "NULL"
                    menu_rows.append(
                        f"({menu_id}, {cat_id}, '{m_name}', "
                        f"'{m_desc}', {price}, NULL, false, {j}, now(), now())"
                    )
                    menu_id += 1

        writer.append_restaurant_block(
            rid=rid,
            addr_id=addr_id,
            sched_rows=sched_rows,
            menu_category_rows=menu_category_rows,
            menu_rows=menu_rows,
            name=safe_str(place.name),
            full_addr=safe_str(place.formatted_address or ""),
            lng=place.lng,
            lat=place.lat,
        )

        rid += 1
        addr_id += 1
        appended += 1
        print(f"[{idx}/{total}] appended place={place.name}")

    writer.write(dry_run=dry_run)
    report = {
        "mode": "api",
        "total_places": len(places),
        "appended_restaurants": appended,
        "skipped_restaurants": skipped,
        "skipped_items": skipped_items,
        "cache_path": cache_path,
    }
    write_report(report_path, report, dry_run)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end DML pipeline")
    parser.add_argument("--mode", default="api", choices=["local", "api"], help="data source mode (api requires GCP_API_KEY/API_KEY)")
    parser.add_argument("--lat", type=float, required=True, help="center latitude")
    parser.add_argument("--lng", type=float, required=True, help="center longitude")
    parser.add_argument("--menus-dir", default="uploaded", help="menu JSON directory")
    parser.add_argument("--places-glob", default="ktb_res_*.json", help="places JSON glob")
    parser.add_argument("--override-json", default=None, help="override JSON (optional)")
    parser.add_argument("--out", default="dml_output.sql", help="DML output file")
    parser.add_argument("--start-id", type=int, default=9000, help="restaurant starting ID")
    parser.add_argument("--cache-dir", default="cache", help="cache directory")
    parser.add_argument("--radius", type=float, default=500.0, help="radius (m)")
    parser.add_argument("--rank", default="DISTANCE", choices=["DISTANCE", "POPULARITY"], help="rank")
    parser.add_argument("--lang", default="ko", help="language")
    parser.add_argument("--type", dest="place_type", default="restaurant", help="place includedTypes")
    parser.add_argument("--max", dest="max_count", type=int, default=20, help="max places")
    parser.add_argument(
        "--use-naver",
        dest="use_naver",
        action="store_true",
        default=True,
        help="crawl menus via Naver Map (selenium required)",
    )
    parser.add_argument(
        "--no-naver",
        dest="use_naver",
        action="store_false",
        help="disable Naver Map crawling",
    )
    parser.add_argument("--sleep", type=float, default=0.2, help="sleep between requests")
    parser.add_argument("--report", default=None, help="write report JSON to this path")
    parser.add_argument("--dry-run", action="store_true", help="no file write")
    args = parser.parse_args()

    if args.mode == "local":
        report = build_dml_from_local(
            menus_dir=args.menus_dir,
            places_glob=args.places_glob,
            out_path=args.out,
            override_json=args.override_json,
            start_id=args.start_id,
            lat=args.lat,
            lng=args.lng,
            dry_run=args.dry_run,
            report_path=args.report,
        )
    else:
        api_key = os.environ.get("GCP_API_KEY") or os.environ.get("API_KEY")
        if not api_key:
            raise SystemExit("GCP_API_KEY 또는 API_KEY 환경 변수를 설정하세요.")
        report = build_dml_from_api(
            api_key=api_key,
            lat=args.lat,
            lng=args.lng,
            radius=args.radius,
            rank=args.rank,
            lang=args.lang,
            place_type=args.place_type,
            max_count=args.max_count,
            cache_dir=args.cache_dir,
            out_path=args.out,
            start_id=args.start_id,
            use_naver=args.use_naver,
            sleep_sec=args.sleep,
            dry_run=args.dry_run,
            report_path=args.report,
        )

    print(f"written: {args.out}")
    print(f"appended: {report.get('appended_restaurants')}")
    print(f"skipped: {report.get('skipped_restaurants')}")
    # detailed skipped items are only written to report file if requested


if __name__ == "__main__":
    main()
