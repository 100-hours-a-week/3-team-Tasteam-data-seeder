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
import hashlib
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


def stable_hash_int(*parts: object, bits: int = 48) -> int:
    if bits <= 0 or bits > 60:
        raise ValueError("bits must be between 1 and 60")
    key = "|".join("" if p is None else str(p) for p in parts)
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    val = int.from_bytes(digest[:8], "big") & ((1 << bits) - 1)
    return val or 1


def restaurant_id_for_place(place: "Place") -> int:
    key = place.place_id or f"{place.name}|{place.formatted_address}|{place.lat}|{place.lng}"
    return stable_hash_int("restaurant", key, bits=48)


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
        "더보기",
        "펼쳐서 더보기",
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
    regular_opening_hours: Optional[dict]
    current_opening_hours: Optional[dict]
    national_phone_number: Optional[str]


def address_id(rid: int) -> int:
    return rid * 10 + 1


def weekly_id(rid: int, day: int) -> int:
    return rid * 100 + day


def override_id(rid: int, idx: int) -> int:
    return rid * 1000 + idx


def menu_category_id(rid: int, idx: int) -> int:
    return rid * 100 + 20 + idx


def menu_id(rid: int, idx: int) -> int:
    return rid * 1000 + 200 + idx


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
            regular_opening_hours=pl.get("regularOpeningHours"),
            current_opening_hours=pl.get("currentOpeningHours"),
            national_phone_number=pl.get("nationalPhoneNumber"),
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


def expand_current_periods(periods: List[dict]) -> Dict[str, Tuple[str, Optional[str]]]:
    """Expand currentOpeningHours periods into date -> (open_time, close_time)."""
    from datetime import date, timedelta

    out: Dict[str, Tuple[str, Optional[str]]] = {}
    for p in periods:
        o = p.get("open") or {}
        c = p.get("close") or {}
        o_date = (o.get("date") or {})
        c_date = (c.get("date") or {})
        if not o_date or not c_date:
            continue
        try:
            start = date(o_date.get("year"), o_date.get("month"), o_date.get("day"))
            end = date(c_date.get("year"), c_date.get("month"), c_date.get("day"))
        except Exception:
            continue
        if end < start:
            continue

        o_time = f"{o.get('hour', 0):02d}:{o.get('minute', 0):02d}"
        c_time = f"{c.get('hour', 0):02d}:{c.get('minute', 0):02d}" if c else None

        cur = start
        while cur <= end:
            if start == end:
                out[cur.isoformat()] = (o_time, c_time)
                break
            if cur == start:
                out[cur.isoformat()] = (o_time, "23:59")
            elif cur == end:
                out[cur.isoformat()] = ("00:00", c_time)
            else:
                out[cur.isoformat()] = ("00:00", "23:59")
            cur += timedelta(days=1)
    return out


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
        override_rows: List[str],
        menu_cat_rows: List[str],
        menu_rows: List[str],
        name: str,
        full_addr: str,
        phone_number: Optional[str],
        lng: float,
        lat: float,
    ) -> None:
        phone_val = "NULL" if not phone_number else f"'{phone_number}'"
        self.lines.append(
            "INSERT INTO restaurant (\n"
            "  id, name, full_address, phone_number, location, deleted_at, created_at, updated_at\n"
            ") VALUES\n"
            f"({rid}, '{name}', '{full_addr}', "
            f"{phone_val}, "
            f"ST_GeomFromText('POINT({lng} {lat})', 4326), "
            "NULL, now(), now())\n"
            "ON CONFLICT DO NOTHING;\n\n"
        )

        self.lines.append(
            "INSERT INTO restaurant_address (\n"
            "  id, restaurant_id, sido, sigungu, eupmyeondong, postal_code,\n"
            "  created_at, updated_at\n"
            ") VALUES\n"
            f"({addr_id}, {rid}, NULL, NULL, NULL, NULL, now(), now())\n"
            "ON CONFLICT DO NOTHING;\n\n"
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
                + "\nON CONFLICT DO NOTHING;\n\n"
            )

        if override_rows:
            self.lines.append(
                "INSERT INTO restaurant_schedule_override (\n"
                "  id, restaurant_id, date,\n"
                "  open_time, close_time, is_closed, reason,\n"
                "  created_at, updated_at\n"
                ") VALUES\n"
                + ",\n".join(override_rows)
                + "\nON CONFLICT DO NOTHING;\n\n"
            )

        if menu_cat_rows:
            self.lines.append(
                "INSERT INTO menu_category (\n"
                "  id, restaurant_id, name, display_order,\n"
                "  created_at, updated_at\n"
                ") VALUES\n"
                + ",\n".join(menu_cat_rows)
                + "\nON CONFLICT DO NOTHING;\n\n"
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
                + "\nON CONFLICT DO NOTHING;\n\n"
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
    id_mode: str,
    lat: float,
    lng: float,
    dry_run: bool,
    report_path: Optional[str],
) -> dict:
    place_paths = sorted(glob.glob(places_glob))
    name_map = load_places(place_paths, override_json)

    menu_files = sorted(f for f in os.listdir(menus_dir) if f.endswith(".json"))

    rid = start_id

    writer = DMLWriter(out_path)
    writer.append_header(lat, lng, radius=None, rank="LOCAL")

    skipped = 0
    appended = 0
    skipped_items: List[Dict[str, Any]] = []
    seen_rids: set[int] = set()
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

        if id_mode == "hash":
            rid = restaurant_id_for_place(place)
        name = safe_str(store)
        full_addr = safe_str(place.formatted_address or "")
        if place.lat is None or place.lng is None:
            skipped += 1
            skipped_items.append({"reason": "missing_location", "store": store, "file": fpath})
            print(f"[{idx}/{total}] skipped (missing_location) store={store}")
            continue
        if rid in seen_rids:
            skipped += 1
            skipped_items.append({"reason": "duplicated_restaurant", "store": store, "file": fpath})
            print(f"[{idx}/{total}] skipped (duplicated_restaurant) store={store}")
            continue
        seen_rids.add(rid)

        sched_rows: List[str] = []
        override_rows: List[str] = []
        menu_cat_rows: List[str] = []
        menu_rows: List[str] = []

        regular = place.regular_opening_hours or {}
        periods = regular.get("periods") or []
        weekly: Dict[int, Tuple[Optional[str], Optional[str], bool]] = {}
        if periods:
            weekly = map_periods_to_weekly(periods)
            for day in range(1, 8):
                open_time, close_time, is_closed = weekly[day]
                o = f"'{open_time}'" if open_time else "NULL"
                c = f"'{close_time}'" if close_time else "NULL"
                sched_rows.append(
                    f"({weekly_id(rid, day)}, {rid}, {day}, {o}, {c}, "
                    f"{'true' if is_closed else 'false'}, NULL, NULL, now(), now())"
                )

            current = place.current_opening_hours or {}
            current_periods = current.get("periods") or []
            if current_periods:
                expanded = expand_current_periods(current_periods)
                override_idx = 1
                for date_str, (o_time, c_time) in expanded.items():
                    try:
                        from datetime import date as _date
                        dt = _date.fromisoformat(date_str)
                    except Exception:
                        continue
                    day = dt.weekday() + 1
                    w_open, w_close, w_closed = weekly.get(day, (None, None, True))
                    same_open = (w_open == o_time)
                    same_close = (w_close == c_time)
                    same_closed = (w_closed is False)
                    if same_open and same_close and same_closed:
                        continue
                    cval = f"'{c_time}'" if c_time else "NULL"
                    override_rows.append(
                        f"({override_id(rid, override_idx)}, {rid}, '{date_str}', "
                        f"'{o_time}', {cval}, false, NULL, now(), now())"
                    )
                    override_idx += 1

        sections = menu.get("sections") or []
        if sections:
            cat_order = 0
            cat_idx = 1
            menu_idx = 1
            for sec in sections:
                sec_name = safe_str(sec.get("name") or "메뉴")
                items = sec.get("items") or []
                swap = should_swap_section_item(sec.get("name") or "메뉴", items)
                cat_items: List[str] = []
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
                    image_url = safe_str(item.get("image_url") or "")
                    img_val = "NULL" if not image_url else f"'{image_url}'"
                    cat_items.append(
                        f"({menu_id(rid, menu_idx)}, {menu_category_id(rid, cat_idx)}, '{m_name}', "
                        f"'{m_desc}', {price}, {img_val}, false, {j}, now(), now())"
                    )
                    menu_idx += 1
                if cat_items:
                    menu_cat_rows.append(
                        f"({menu_category_id(rid, cat_idx)}, {rid}, '{sec_name}', {cat_order}, now(), now())"
                    )
                    cat_order += 1
                    menu_rows.extend(cat_items)
                    cat_idx += 1

        writer.append_restaurant_block(
            rid=rid,
            addr_id=address_id(rid),
            sched_rows=sched_rows,
            override_rows=override_rows,
            menu_cat_rows=menu_cat_rows,
            menu_rows=menu_rows,
            name=name,
            full_addr=full_addr,
            phone_number=safe_str(place.national_phone_number or ""),
            lng=place.lng,
            lat=place.lat,
        )

        if id_mode == "sequential":
            rid += 1
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
        "places.nationalPhoneNumber,"
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
                regular_opening_hours=pl.get("regularOpeningHours"),
                current_opening_hours=pl.get("currentOpeningHours"),
                national_phone_number=pl.get("nationalPhoneNumber"),
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
    id_mode: str,
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

    writer = DMLWriter(out_path)
    writer.append_header(lat, lng, radius=radius, rank=rank)

    skipped = 0
    appended = 0
    skipped_items: List[Dict[str, Any]] = []
    seen_rids: set[int] = set()
    total = len(places)
    for idx, place in enumerate(places, 1):
        if id_mode == "hash":
            rid = restaurant_id_for_place(place)
        if rid in seen_rids:
            skipped += 1
            skipped_items.append(
                {"reason": "duplicated_restaurant", "place_name": place.name, "place_id": place.place_id}
            )
            print(f"[{idx}/{total}] skipped (duplicated_restaurant) place={place.name}")
            continue
        seen_rids.add(rid)
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
        override_rows: List[str] = []
        menu_cat_rows: List[str] = []
        menu_rows: List[str] = []

        regular = place.regular_opening_hours or {}
        periods = regular.get("periods") or []
        weekly: Dict[int, Tuple[Optional[str], Optional[str], bool]] = {}
        if periods:
            weekly = map_periods_to_weekly(periods)
            for day in range(1, 8):
                open_time, close_time, is_closed = weekly[day]
                o = f"'{open_time}'" if open_time else "NULL"
                c = f"'{close_time}'" if close_time else "NULL"
                sched_rows.append(
                    f"({weekly_id(rid, day)}, {rid}, {day}, {o}, {c}, "
                    f"{'true' if is_closed else 'false'}, NULL, NULL, now(), now())"
                )

        current = place.current_opening_hours or {}
        current_periods = current.get("periods") or []
        if current_periods and weekly:
            expanded = expand_current_periods(current_periods)
            override_idx = 1
            for date_str, (o_time, c_time) in expanded.items():
                try:
                    from datetime import date as _date
                    dt = _date.fromisoformat(date_str)
                except Exception:
                    continue
                day = dt.weekday() + 1
                w_open, w_close, w_closed = weekly.get(day, (None, None, True))
                same_open = (w_open == o_time)
                same_close = (w_close == c_time)
                same_closed = (w_closed is False)
                if same_open and same_close and same_closed:
                    continue
                cval = f"'{c_time}'" if c_time else "NULL"
                override_rows.append(
                    f"({override_id(rid, override_idx)}, {rid}, '{date_str}', "
                    f"'{o_time}', {cval}, false, NULL, now(), now())"
                )
                override_idx += 1

        sections = menu.get("sections") or []
        if sections:
            cat_order = 0
            cat_idx = 1
            menu_idx = 1
            for sec in sections:
                sec_name = safe_str(sec.get("name") or "메뉴")
                items = sec.get("items") or []
                swap = should_swap_section_item(sec.get("name") or "메뉴", items)
                cat_items: List[str] = []
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
                    image_url = safe_str(item.get("image_url") or "")
                    img_val = "NULL" if not image_url else f"'{image_url}'"
                    cat_items.append(
                        f"({menu_id(rid, menu_idx)}, {menu_category_id(rid, cat_idx)}, '{m_name}', "
                        f"'{m_desc}', {price}, {img_val}, false, {j}, now(), now())"
                    )
                    menu_idx += 1
                if cat_items:
                    menu_cat_rows.append(
                        f"({menu_category_id(rid, cat_idx)}, {rid}, '{sec_name}', {cat_order}, now(), now())"
                    )
                    cat_order += 1
                    menu_rows.extend(cat_items)
                    cat_idx += 1

        writer.append_restaurant_block(
            rid=rid,
            addr_id=address_id(rid),
            sched_rows=sched_rows,
            override_rows=override_rows,
            menu_cat_rows=menu_cat_rows,
            menu_rows=menu_rows,
            name=safe_str(place.name),
            full_addr=safe_str(place.formatted_address or ""),
            phone_number=safe_str(place.national_phone_number or ""),
            lng=place.lng,
            lat=place.lat,
        )

        if id_mode == "sequential":
            rid += 1
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
    parser.add_argument(
        "--id-mode",
        choices=["hash", "sequential"],
        default="hash",
        help="restaurant ID 생성 방식 (hash 권장)",
    )
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
            id_mode=args.id_mode,
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
            id_mode=args.id_mode,
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
