from __future__ import annotations

import glob
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .menu_service import get_menu_for_place
from .places_service import (
    Place,
    expand_current_periods,
    fetch_places_nearby,
    load_places,
    map_periods_to_weekly,
    norm_name,
    places_from_response,
    restaurant_id_for_place,
)


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
        "더보기",
        "펼쳐서 더보기",
    )
    if any(p in s for p in ui_phrases):
        return True
    if re.search(r"방문자\s*리뷰\s*\d+", s) and "리뷰" in s:
        return True
    return False


def is_store_name_with_category(raw_name: str, store_name: str) -> bool:
    if not raw_name or not store_name:
        return False
    norm = lambda x: re.sub(r"[()\[\]{}<>\\-_/.,'\"\u2019\s]+", "", x.lower())
    n_raw = norm(raw_name)
    n_store = norm(store_name)
    if not n_raw.startswith(n_store):
        return False
    tail = raw_name[len(store_name) :].strip()
    if not tail:
        return True
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


@dataclass
class DMLWriter:
    out_path: str
    lines: List[str]

    def __init__(self, out_path: str):
        self.out_path = out_path
        self.lines = []

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
            f"({rid}, '{name}', '{full_addr}', {phone_val}, "
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
        out_dir = os.path.dirname(self.out_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(self.out_path, "w", encoding="utf-8") as f:
            f.write("".join(self.lines))


def write_report(report_path: Optional[str], report: dict, dry_run: bool) -> None:
    if not report_path or dry_run:
        return
    report_dir = os.path.dirname(report_path)
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


def _build_menu_rows_for_place(rid: int, place_name: str, menu: dict) -> Tuple[List[str], List[str]]:
    menu_cat_rows: List[str] = []
    menu_rows: List[str] = []

    sections = menu.get("sections") or []
    if not sections:
        return menu_cat_rows, menu_rows

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
            if raw_name == place_name or is_store_name_with_category(raw_name, place_name):
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

    return menu_cat_rows, menu_rows


def _build_schedule_rows(rid: int, place: Place) -> Tuple[List[str], List[str]]:
    sched_rows: List[str] = []
    override_rows: List[str] = []

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
            if (w_open == o_time) and (w_close == c_time) and (w_closed is False):
                continue
            cval = f"'{c_time}'" if c_time else "NULL"
            override_rows.append(
                f"({override_id(rid, override_idx)}, {rid}, '{date_str}', "
                f"'{o_time}', {cval}, false, NULL, now(), now())"
            )
            override_idx += 1

    return sched_rows, override_rows


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

    writer = DMLWriter(out_path)
    writer.append_header(lat, lng, radius=None, rank="LOCAL")

    rid = start_id
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

        place = name_map.get(norm_name(store))
        if not place:
            skipped += 1
            skipped_items.append({"reason": "place_not_found", "store": store, "file": fpath})
            print(f"[{idx}/{total}] skipped (place_not_found) store={store}")
            continue

        if id_mode == "hash":
            rid = restaurant_id_for_place(place)
        if rid in seen_rids:
            skipped += 1
            skipped_items.append({"reason": "duplicated_restaurant", "store": store, "file": fpath})
            print(f"[{idx}/{total}] skipped (duplicated_restaurant) store={store}")
            continue
        seen_rids.add(rid)

        sched_rows, override_rows = _build_schedule_rows(rid, place)
        menu_cat_rows, menu_rows = _build_menu_rows_for_place(rid, store, menu)

        writer.append_restaurant_block(
            rid=rid,
            addr_id=address_id(rid),
            sched_rows=sched_rows,
            override_rows=override_rows,
            menu_cat_rows=menu_cat_rows,
            menu_rows=menu_rows,
            name=safe_str(store),
            full_addr=safe_str(place.formatted_address or ""),
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

    writer = DMLWriter(out_path)
    writer.append_header(lat, lng, radius=radius, rank=rank)

    rid = start_id
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

        menu = get_menu_for_place(place, cache_dir)

        sched_rows, override_rows = _build_schedule_rows(rid, place)
        if menu:
            menu_cat_rows, menu_rows = _build_menu_rows_for_place(rid, place.name, menu)
        else:
            menu_cat_rows, menu_rows = [], []

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
