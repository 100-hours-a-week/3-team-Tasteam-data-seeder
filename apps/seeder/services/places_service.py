from __future__ import annotations

import hashlib
import json
import os
import re
import urllib.request
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


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


def norm_name(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[()\[\]{}<>\\-_/.,'\"\u2019]+", "", s)
    return s


def stable_hash_int(*parts: object, bits: int = 48) -> int:
    if bits <= 0 or bits > 60:
        raise ValueError("bits must be between 1 and 60")
    key = "|".join("" if p is None else str(p) for p in parts)
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    val = int.from_bytes(digest[:8], "big") & ((1 << bits) - 1)
    return val or 1


def restaurant_id_for_place(place: Place) -> int:
    key = place.place_id or f"{place.name}|{place.formatted_address}|{place.lat}|{place.lng}"
    return stable_hash_int("restaurant", key, bits=48)


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
        name_map[key] = Place(
            place_id=pl.get("id"),
            name=(pl.get("displayName") or {}).get("text") or "",
            formatted_address=pl.get("formattedAddress") or "",
            lat=lat,
            lng=lng,
            regular_opening_hours=pl.get("regularOpeningHours"),
            current_opening_hours=pl.get("currentOpeningHours"),
            national_phone_number=pl.get("nationalPhoneNumber"),
        )
    return name_map


def map_periods_to_weekly(periods: List[dict]) -> Dict[int, Tuple[Optional[str], Optional[str], bool]]:
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
