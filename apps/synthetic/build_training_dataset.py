#!/usr/bin/env python3
"""Build final training dataset by joining user/item/context features with implicit feedback."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple


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


def anon_cohort(anonymous_id: str) -> str:
    digest = hashlib.sha1(anonymous_id.encode("utf-8")).hexdigest()[:12]
    return f"anon_cohort_{digest}"


def parse_json(raw: str, default):
    text = (raw or "").strip()
    if not text:
        return default
    try:
        v = json.loads(text)
        return v
    except json.JSONDecodeError:
        return default


def top_pref_categories(preferred_categories_json: str) -> Tuple[List[str], List[str]]:
    arr = parse_json(preferred_categories_json, [])
    names = ["", "", ""]
    weights = ["", "", ""]
    if isinstance(arr, list):
        for i, item in enumerate(arr[:3]):
            if not isinstance(item, dict):
                continue
            names[i] = str(item.get("category") or "")
            w = item.get("weight")
            if w is not None:
                try:
                    weights[i] = str(round(float(w), 6))
                except Exception:
                    weights[i] = ""
    return names, weights


def first_tag(raw_json: str, key: str = "tag") -> str:
    arr = parse_json(raw_json, [])
    if not isinstance(arr, list) or not arr:
        return ""
    first = arr[0]
    if isinstance(first, dict):
        return str(first.get(key) or "")
    return str(first)


def load_user_features(path: Path) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    by_user_id: Dict[str, dict] = {}
    by_cohort: Dict[str, dict] = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            uid = (row.get("user_id") or "").strip()
            cohort = (row.get("anonymous_cohort_id") or "").strip()
            if uid:
                by_user_id[uid] = row
            if cohort:
                by_cohort[cohort] = row
    return by_user_id, by_cohort


def load_restaurant_features(path: Path) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rid = (row.get("restaurant_id") or "").strip()
            if rid:
                out[rid] = row
    return out


def load_context_features(path: Path) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ek = (row.get("event_key") or "").strip()
            if ek:
                out[ek] = row
    return out


def build_rows(
    feedback_path: Path,
    user_feature_path: Path,
    restaurant_feature_path: Path,
    context_feature_path: Path,
) -> List[dict]:
    user_by_id, user_by_cohort = load_user_features(user_feature_path)
    restaurant_map = load_restaurant_features(restaurant_feature_path)
    context_map = load_context_features(context_feature_path)

    out: List[dict] = []
    generated_at = datetime.now(timezone.utc).isoformat()

    with feedback_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            uid = (row.get("user_id") or "").strip()
            anon = (row.get("anonymous_id") or "").strip()
            rid = (row.get("restaurant_id") or "").strip()

            if not rid:
                continue

            u = None
            cohort = ""
            if uid:
                u = user_by_id.get(uid)
            elif anon:
                cohort = anon_cohort(anon)
                u = user_by_cohort.get(cohort)

            item = restaurant_map.get(rid, {})
            ek = build_event_key(row)
            ctx = context_map.get(ek, {})

            preferred_json = (u.get("preferred_categories") if u else "") or ""
            pref_names, pref_weights = top_pref_categories(preferred_json)

            categories = parse_json((item.get("categories") or "").strip(), [])
            primary_category = ""
            if isinstance(categories, list) and categories:
                primary_category = str(categories[0])

            out.append(
                {
                    "user_id": uid,
                    "anonymous_id": anon,
                    "restaurant_id": rid,
                    "taste_preferences": (u.get("taste_preferences") if u else "") or "{}",
                    "visit_time_distribution": (u.get("visit_time_distribution") if u else "") or "{}",
                    "is_anonymous": "1" if (not uid and anon) else "0",
                    "avg_price_tier": (u.get("avg_price_tier") if u else "") or "",
                    "primary_category": primary_category,
                    "pref_cat_1": pref_names[0],
                    "pref_cat_2": pref_names[1],
                    "pref_cat_3": pref_names[2],
                    "price_tier": (item.get("price_tier") or "").strip(),
                    "region_gu": (item.get("region_gu") or "").strip(),
                    "region_dong": (item.get("region_dong") or "").strip(),
                    "geohash": (item.get("geohash") or "").strip(),
                    "day_of_week": (ctx.get("day_of_week") or "").strip(),
                    "time_slot": (ctx.get("time_slot") or "").strip(),
                    "admin_dong": (ctx.get("admin_dong") or "").strip(),
                    "distance_bucket": (ctx.get("distance_bucket") or "").strip(),
                    "weather_bucket": (ctx.get("weather_bucket") or "").strip(),
                    "dining_type": (ctx.get("dining_type") or "").strip(),
                    "first_positive_segment": first_tag(item.get("positive_segments") or "[]"),
                    "first_comparison_tag": first_tag(item.get("comparison_tags") or "[]", key="tag"),
                    "pref_w_1": pref_weights[0],
                    "pref_w_2": pref_weights[1],
                    "pref_w_3": pref_weights[2],
                    "signal_type": (row.get("signal_type") or "").strip(),
                    "weight": (row.get("weight") or "").strip(),
                    "occurred_at": (row.get("occurred_at") or "").strip(),
                    "data_source": (row.get("data_source") or "").strip(),
                    "generated_at": generated_at,
                    "recommendation_id": "",
                }
            )

    return out


def write_rows(rows: List[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "user_id",
        "anonymous_id",
        "restaurant_id",
        "taste_preferences",
        "visit_time_distribution",
        "is_anonymous",
        "avg_price_tier",
        "primary_category",
        "pref_cat_1",
        "pref_cat_2",
        "pref_cat_3",
        "price_tier",
        "region_gu",
        "region_dong",
        "geohash",
        "day_of_week",
        "time_slot",
        "admin_dong",
        "distance_bucket",
        "weather_bucket",
        "dining_type",
        "first_positive_segment",
        "first_comparison_tag",
        "pref_w_1",
        "pref_w_2",
        "pref_w_3",
        "signal_type",
        "weight",
        "occurred_at",
        "data_source",
        "generated_at",
        "recommendation_id",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build training_dataset.csv")
    parser.add_argument("--feedback", default="output/synthetic/implicit_feedback_mixed.csv")
    parser.add_argument("--user-feature", default="output/synthetic/user_feature_mixed.csv")
    parser.add_argument("--restaurant-feature", default="output/synthetic/restaurant_feature.csv")
    parser.add_argument("--context-feature", default="output/synthetic/context_feature.csv")
    parser.add_argument("--output", default="output/synthetic/training_dataset.csv")
    args = parser.parse_args()

    rows = build_rows(
        feedback_path=Path(args.feedback),
        user_feature_path=Path(args.user_feature),
        restaurant_feature_path=Path(args.restaurant_feature),
        context_feature_path=Path(args.context_feature),
    )
    write_rows(rows, Path(args.output))
    print(f"wrote {len(rows)} rows -> {args.output}")


if __name__ == "__main__":
    main()
