"""
uploaded/ 메뉴 JSON과 ktb_res_*.json을 매칭해서 DML을 생성한다.
주소 상세(sido/sigungu/eupmyeondong/postal_code)는 NULL로 저장한다.
매칭 실패(places 없음) 시 해당 매장은 스킵한다.
"""
import argparse
import glob
import hashlib
import json
import os
import re
from datetime import datetime


def norm_name(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[()\[\]{}<>\\-_/.,'\"\u2019]+", "", s)
    return s


def load_places(paths, override_path=None):
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
            # normalize override into place-like dict
            name = (pl.get("displayName") or {}).get("text") or key
            if not name:
                continue
            base = {
                "displayName": {"text": name},
                "formattedAddress": pl.get("formattedAddress"),
                "location": pl.get("location"),
            }
            places.append(base)
            # also add a synthetic entry keyed by override key
            if key and key != name:
                places.append(
                    {
                        "displayName": {"text": key},
                        "formattedAddress": pl.get("formattedAddress"),
                        "location": pl.get("location"),
                    }
                )
    # name -> place (first wins)
    name_map = {}
    for pl in places:
        key = norm_name((pl.get("displayName") or {}).get("text"))
        if key and key not in name_map:
            name_map[key] = pl
    return name_map


def map_periods_to_weekly(periods):
    # Google: day 0=Sunday ... 6=Saturday
    # DML: day_of_week 1=Mon ... 7=Sun
    day_map = {0: 7, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6}
    by_day = {}
    for p in periods:
        o = p.get("open")
        c = p.get("close")
        if not o:
            continue
        day = o.get("day")
        if day is None:
            continue
        # take first period per day (테스트용)
        if day in by_day:
            continue
        open_time = f"{o.get('hour', 0):02d}:{o.get('minute', 0):02d}"
        close_time = None
        if c:
            close_time = f"{c.get('hour', 0):02d}:{c.get('minute', 0):02d}"
        by_day[day] = (open_time, close_time)
    weekly = {}
    for g_day, d_day in day_map.items():
        if g_day in by_day:
            weekly[d_day] = (*by_day[g_day], False)
        else:
            weekly[d_day] = (None, None, True)
    return weekly


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


def restaurant_id_for_place(place: dict, name: str) -> int:
    place_id = place.get("id")
    if place_id:
        key = place_id
    else:
        addr = place.get("formattedAddress") or ""
        loc = place.get("location") or {}
        lat = loc.get("latitude")
        lng = loc.get("longitude")
        key = f"{name}|{addr}|{lat}|{lng}"
    return stable_hash_int("restaurant", key, bits=48)


def address_id(rid: int) -> int:
    return rid * 10 + 1


def weekly_id(rid: int, day: int) -> int:
    return rid * 100 + day


def menu_category_id(rid: int, idx: int) -> int:
    return rid * 100 + 20 + idx


def menu_id(rid: int, idx: int) -> int:
    return rid * 1000 + 200 + idx


def is_ui_noise_text(s: str) -> bool:
    if not s:
        return False
    ui_phrases = (
        "페이지 닫기",
        "이미지 갯수",
        "알림받기",
        "출발",
        "도착",
        "저장",
        "거리뷰",
        "공유",
    )
    return any(p in s for p in ui_phrases)


def main():
    parser = argparse.ArgumentParser(description="기존 데이터로 DML 생성")
    parser.add_argument("--menus-dir", default="output/seeder/menus", help="메뉴 JSON 폴더")
    parser.add_argument("--places-glob", default="output/seeder/ktb_res*.json", help="places JSON glob")
    parser.add_argument("--out", default="output/seeder/dml_output.sql", help="DML 출력 파일")
    parser.add_argument("--override-json", default=None, help="textSearch 결과 JSON")
    parser.add_argument("--start-id", type=int, default=9000, help="restaurant 시작 ID")
    parser.add_argument(
        "--id-mode",
        choices=["hash", "sequential"],
        default="hash",
        help="restaurant ID 생성 방식 (hash 권장)",
    )
    args = parser.parse_args()

    place_paths = sorted(glob.glob(args.places_glob))
    name_map = load_places(place_paths, args.override_json)

    menu_files = sorted(
        f for f in os.listdir(args.menus_dir) if f.endswith("_menu.json")
    )

    rid = args.start_id

    lines = []
    lines.append("-- auto-generated DML\n")

    skipped = 0
    seen_rids: set[int] = set()
    for fname in menu_files:
        fpath = os.path.join(args.menus_dir, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            menu = json.load(f)
        store = menu.get("store_name") or ""
        if not store:
            skipped += 1
            continue
        key = norm_name(store)
        place = name_map.get(key)
        if not place:
            skipped += 1
            continue

        if args.id_mode == "hash":
            rid = restaurant_id_for_place(place, store)
        name = safe_str(store)
        full_addr = safe_str(place.get("formattedAddress") or "")
        loc = place.get("location") or {}
        lat = loc.get("latitude")
        lng = loc.get("longitude")
        if lat is None or lng is None:
            skipped += 1
            continue
        if rid in seen_rids:
            skipped += 1
            continue
        seen_rids.add(rid)

        phone = safe_str(place.get("nationalPhoneNumber") or "")
        phone_val = "NULL" if not phone else f"'{phone}'"
        restaurant_values = [
            f"({rid}, '{name}', '{full_addr}', {phone_val}, "
            f"ST_GeomFromText('POINT({lng} {lat})', 4326), "
            "NULL, now(), now())"
        ]
        address_values = [
            f"({address_id(rid)}, {rid}, NULL, NULL, NULL, NULL, now(), now())"
        ]
        schedule_values = []
        rfc_values = []
        menu_category_values = []
        menu_values = []

        # schedule
        opening = menu.get("opening_hours") or {}
        regular = opening.get("regularOpeningHours") or {}
        periods = regular.get("periods") or []
        if periods:
            weekly = map_periods_to_weekly(periods)
            for day in range(1, 8):
                open_time, close_time, is_closed = weekly[day]
                o = f"'{open_time}'" if open_time else "NULL"
                c = f"'{close_time}'" if close_time else "NULL"
                schedule_values.append(
                    f"({weekly_id(rid, day)}, {rid}, {day}, {o}, {c}, "
                    f"{'true' if is_closed else 'false'}, NULL, NULL, now(), now())"
                )

        # menu_category + menu
        sections = menu.get("sections") or []
        if sections:
            cat_idx = 1
            menu_idx = 1
            for i, sec in enumerate(sections):
                sec_name = safe_str(sec.get("name") or "메뉴")
                menu_category_values.append(
                    f"({menu_category_id(rid, cat_idx)}, {rid}, '{sec_name}', {i}, now(), now())"
                )
                cat_idx += 1

            cat_base = menu_category_id(rid, 1)
            for i, sec in enumerate(sections):
                cat_id = cat_base + i
                items = sec.get("items") or []
                for j, item in enumerate(items):
                    raw_name = item.get("name") or ""
                    raw_desc = item.get("description") or ""
                    m_name = safe_str(raw_name)
                    m_desc = safe_str(raw_desc)
                    # drop UI/header noise that got parsed as a menu item
                    if raw_name == store or is_ui_noise_text(raw_desc):
                        continue
                    price_val = item.get("price_value")
                    price = price_val if isinstance(price_val, int) else "NULL"
                    menu_values.append(
                        f"({menu_id(rid, menu_idx)}, {cat_id}, '{m_name}', "
                        f"'{m_desc}', {price}, NULL, false, {j}, now(), now())"
                    )
                    menu_idx += 1

        # restaurant_food_category (스킵: 자동 분류 미적용)

        # Emit DML per restaurant (append style)
        if restaurant_values:
            lines.append(
                "INSERT INTO restaurant (\n"
                "  id, name, full_address, phone_number, location, deleted_at, created_at, updated_at\n"
                ") VALUES\n"
                + ",\n".join(restaurant_values)
                + "\nON CONFLICT DO NOTHING;\n\n"
            )
        if address_values:
            lines.append(
                "INSERT INTO restaurant_address (\n"
                "  id, restaurant_id, sido, sigungu, eupmyeondong, postal_code,\n"
                "  created_at, updated_at\n"
                ") VALUES\n"
                + ",\n".join(address_values)
                + "\nON CONFLICT DO NOTHING;\n\n"
            )
        if schedule_values:
            lines.append(
                "INSERT INTO restaurant_weekly_schedule (\n"
                "  id, restaurant_id, day_of_week,\n"
                "  open_time, close_time, is_closed,\n"
                "  effective_from, effective_to,\n"
                "  created_at, updated_at\n"
                ") VALUES\n"
                + ",\n".join(schedule_values)
                + "\nON CONFLICT DO NOTHING;\n\n"
            )
        if rfc_values:
            lines.append(
                "INSERT INTO restaurant_food_category (\n"
                "  id, restaurant_id, food_category_id,\n"
                "  created_at, updated_at\n"
                ") VALUES\n"
                + ",\n".join(rfc_values)
                + "\nON CONFLICT DO NOTHING;\n\n"
            )
        if menu_category_values:
            lines.append(
                "INSERT INTO menu_category (\n"
                "  id, restaurant_id, name, display_order,\n"
                "  created_at, updated_at\n"
                ") VALUES\n"
                + ",\n".join(menu_category_values)
                + "\nON CONFLICT DO NOTHING;\n\n"
            )
        if menu_values:
            lines.append(
                "INSERT INTO menu (\n"
                "  id, category_id, name, description,\n"
                "  price, image_url,\n"
                "  is_recommended, display_order,\n"
                "  created_at, updated_at\n"
                ") VALUES\n"
                + ",\n".join(menu_values)
                + "\nON CONFLICT DO NOTHING;\n\n"
            )

        if args.id_mode == "sequential":
            rid += 1

    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("".join(lines))

    print(f"written: {args.out}")
    print(f"skipped: {skipped}")


if __name__ == "__main__":
    main()
