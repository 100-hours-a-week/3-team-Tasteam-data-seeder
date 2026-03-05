#!/usr/bin/env python3
"""Generate context features from implicit feedback snapshots."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict


def parse_context(raw: str) -> dict:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        val = json.loads(text)
        return val if isinstance(val, dict) else {}
    except json.JSONDecodeError:
        return {}


def parse_dt(value: str) -> datetime:
    text = (value or "").strip().replace("Z", "+00:00")
    text = re.sub(r"([+-]\d{2})$", r"\1:00", text)
    text = re.sub(
        r"(\.\d+)(?=(?:[+-]\d{2}:\d{2})?$)",
        lambda m: "." + m.group(1)[1:].ljust(6, "0")[:6],
        text,
    )
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.fromisoformat(text.replace(" ", "T", 1))


def day_of_week(dt: datetime) -> str:
    names = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
    return names[dt.weekday()]


def time_slot(dt: datetime) -> str:
    h = dt.hour
    if 5 <= h <= 10:
        return "breakfast"
    if 11 <= h <= 14:
        return "lunch"
    if 15 <= h <= 16:
        return "afternoon"
    if 17 <= h <= 21:
        return "dinner"
    return "late_night"


def dining_type(context: dict, signal_type: str) -> str:
    raw = (context.get("dining_type") or context.get("diningType") or "").strip().upper()
    if raw in {"SOLO", "GROUP"}:
        return raw
    if signal_type == "SHARE":
        return "GROUP"
    return "SOLO"


def distance_bucket(context: dict) -> str:
    # Distance source is unavailable in current snapshots.
    # Keep empty unless upstream explicitly provides one.
    raw = (context.get("distance_bucket") or context.get("distanceBucket") or "").strip().upper()
    if raw in {"NEAR", "CLOSE", "MID", "FAR"}:
        return raw
    return ""


def weather_bucket(context: dict) -> str:
    raw = (context.get("weather_bucket") or context.get("weatherBucket") or "").strip().upper()
    if raw in {"CLEAR", "CLOUDY", "RAIN", "SNOW"}:
        return raw
    return ""


def build_event_key(row: Dict[str, str]) -> str:
    key = "|".join(
        [
            (row.get("user_id") or "").strip(),
            (row.get("anonymous_id") or "").strip(),
            (row.get("restaurant_id") or "").strip(),
            (row.get("signal_type") or "").strip(),
            (row.get("occurred_at") or "").strip(),
            (row.get("context") or "").strip(),
        ]
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def load_restaurant_feature(path: Path) -> Dict[int, Dict[str, str]]:
    out: Dict[int, Dict[str, str]] = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rid_raw = (row.get("restaurant_id") or "").strip()
            if not rid_raw:
                continue
            try:
                rid = int(rid_raw)
            except ValueError:
                continue
            out[rid] = {
                "region_dong": (row.get("region_dong") or "").strip(),
                "geohash": (row.get("geohash") or "").strip(),
            }
    return out


def generate_context_rows(feedback_path: Path, restaurant_feature_path: Path) -> list[Dict[str, str]]:
    restaurant_map = load_restaurant_feature(restaurant_feature_path)
    rows: list[Dict[str, str]] = []

    with feedback_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"restaurant_id", "signal_type", "occurred_at", "context"}
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"implicit_feedback missing columns: {', '.join(missing)}")

        for row in reader:
            event_key = build_event_key(row)
            occurred = parse_dt(row.get("occurred_at") or "")
            context = parse_context(row.get("context") or "")

            rid = None
            rid_raw = (row.get("restaurant_id") or "").strip()
            if rid_raw:
                try:
                    rid = int(rid_raw)
                except ValueError:
                    rid = None

            restaurant_ref = restaurant_map.get(rid or -1, {})
            admin_dong = (
                (context.get("admin_dong") or context.get("adminDong") or "").strip()
                or restaurant_ref.get("region_dong", "")
            )
            geoh = (
                (context.get("geohash") or "").strip()
                or restaurant_ref.get("geohash", "")
            )

            rows.append(
                {
                    "event_key": event_key,
                    "day_of_week": day_of_week(occurred),
                    "time_slot": time_slot(occurred),
                    "admin_dong": admin_dong,
                    "geohash": geoh,
                    "distance_bucket": distance_bucket(context),
                    "weather_bucket": weather_bucket(context),
                    "dining_type": dining_type(context, (row.get("signal_type") or "").strip().upper()),
                }
            )

    return rows


def write_rows(rows: list[Dict[str, str]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "event_key",
        "day_of_week",
        "time_slot",
        "admin_dong",
        "geohash",
        "distance_bucket",
        "weather_bucket",
        "dining_type",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate context_feature.csv")
    parser.add_argument("--feedback", default="output/synthetic/implicit_feedback_mixed.csv")
    parser.add_argument("--restaurant-feature", default="output/synthetic/restaurant_feature.csv")
    parser.add_argument("--output", default="output/synthetic/context_feature.csv")
    args = parser.parse_args()

    rows = generate_context_rows(Path(args.feedback), Path(args.restaurant_feature))
    write_rows(rows, Path(args.output))
    print(f"wrote {len(rows)} rows -> {args.output}")


if __name__ == "__main__":
    main()
