#!/usr/bin/env python3
"""Generate restaurant_feature.csv from normalized restaurant/menu inputs."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

ALLOWED_PRICE_TIERS = {"LOW", "MID", "HIGH", "PREMIUM"}


@dataclass
class RestaurantFeatureRow:
    id: int
    restaurant_id: int
    categories: str
    price_tier: str
    region_gu: str
    region_dong: str
    geohash: str
    positive_segments: str
    comparison_tags: str
    tags_generated_at: str
    created_at: str
    updated_at: str


def _normalize_price_tier(raw: str) -> str:
    text = (raw or "").strip().upper()
    if text in ALLOWED_PRICE_TIERS:
        return text
    return ""


def _tier_from_price_mean(price_mean: Optional[float]) -> str:
    if price_mean is None:
        return ""
    if price_mean < 10000:
        return "LOW"
    if price_mean < 20000:
        return "MID"
    if price_mean < 35000:
        return "HIGH"
    return "PREMIUM"


def _parse_float_or_none(value: str) -> Optional[float]:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _pick_most_common_non_empty(values: List[str]) -> str:
    non_empty = [v.strip() for v in values if v and v.strip()]
    if not non_empty:
        return ""
    return Counter(non_empty).most_common(1)[0][0]


def load_restaurant_base(path: Path) -> Dict[int, dict]:
    grouped: Dict[int, dict] = defaultdict(
        lambda: {
            "categories": set(),
            "sigungu": [],
            "eupmyeondong": [],
            "geohash": [],
        }
    )

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {
            "restaurant_id",
            "food_category_name",
            "sigungu",
            "eupmyeondong",
            "geohash",
        }
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"restaurant.csv missing columns: {', '.join(missing)}")

        for row in reader:
            raw_rid = (row.get("restaurant_id") or "").strip()
            if not raw_rid:
                continue
            try:
                rid = int(raw_rid)
            except ValueError:
                continue

            category = (row.get("food_category_name") or "").strip()
            if category:
                grouped[rid]["categories"].add(category)

            grouped[rid]["sigungu"].append((row.get("sigungu") or "").strip())
            grouped[rid]["eupmyeondong"].append((row.get("eupmyeondong") or "").strip())
            grouped[rid]["geohash"].append((row.get("geohash") or "").strip())

    return grouped


def load_price_tiers(path: Path) -> Dict[int, str]:
    out: Dict[int, str] = {}

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"restaurant_id", "price_tier", "price_mean"}
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"restaurant_menu_agg.csv missing columns: {', '.join(missing)}")

        for row in reader:
            raw_rid = (row.get("restaurant_id") or "").strip()
            if not raw_rid:
                continue
            try:
                rid = int(raw_rid)
            except ValueError:
                continue

            tier = _normalize_price_tier(row.get("price_tier") or "")
            if not tier:
                price_mean = _parse_float_or_none(row.get("price_mean") or "")
                tier = _tier_from_price_mean(price_mean)
            out[rid] = tier

    return out


def build_rows(restaurant_base: Dict[int, dict], price_tiers: Dict[int, str]) -> List[RestaurantFeatureRow]:
    now = datetime.now(timezone.utc).isoformat()
    rows: List[RestaurantFeatureRow] = []

    for idx, rid in enumerate(sorted(restaurant_base.keys()), start=1):
        base = restaurant_base[rid]
        categories: Set[str] = base["categories"]

        rows.append(
            RestaurantFeatureRow(
                id=idx,
                restaurant_id=rid,
                categories=json.dumps(sorted(categories), ensure_ascii=False),
                price_tier=price_tiers.get(rid, ""),
                region_gu=_pick_most_common_non_empty(base["sigungu"]),
                region_dong=_pick_most_common_non_empty(base["eupmyeondong"]),
                geohash=_pick_most_common_non_empty(base["geohash"]),
                positive_segments="[]",
                comparison_tags="[]",
                tags_generated_at="",
                created_at=now,
                updated_at=now,
            )
        )

    return rows


def write_rows(rows: List[RestaurantFeatureRow], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "restaurant_id",
                "categories",
                "price_tier",
                "region_gu",
                "region_dong",
                "geohash",
                "positive_segments",
                "comparison_tags",
                "tags_generated_at",
                "created_at",
                "updated_at",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate restaurant_feature.csv")
    parser.add_argument("--restaurant", default="restaurant.csv")
    parser.add_argument("--menu-agg", default="restaurant_menu_agg.csv")
    parser.add_argument("--output", default="output/synthetic/restaurant_feature.csv")
    args = parser.parse_args()

    restaurant_base = load_restaurant_base(Path(args.restaurant))
    price_tiers = load_price_tiers(Path(args.menu_agg))
    rows = build_rows(restaurant_base, price_tiers)
    write_rows(rows, Path(args.output))

    print(f"wrote {len(rows)} rows -> {args.output}")


if __name__ == "__main__":
    main()
