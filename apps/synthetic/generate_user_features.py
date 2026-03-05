#!/usr/bin/env python3
"""Generate user_feature.csv from implicit feedback and restaurant features."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

PRICE_TIER_SCORE = {
    "LOW": 1.0,
    "MID": 2.0,
    "HIGH": 3.0,
    "PREMIUM": 4.0,
}

SCORE_TO_TIER = [
    (1.5, "LOW"),
    (2.5, "MID"),
    (3.5, "HIGH"),
    (999.0, "PREMIUM"),
]

# Simple proxy mapping from category to taste dimensions.
CATEGORY_TASTE_PROFILE: Dict[str, Dict[str, float]] = {
    "한식": {"spicy": 0.6, "sweet": 0.2, "savory": 0.9, "light": 0.4},
    "고깃집": {"spicy": 0.4, "sweet": 0.1, "savory": 1.0, "light": 0.1},
    "중식": {"spicy": 0.5, "sweet": 0.3, "savory": 0.8, "light": 0.3},
    "일식": {"spicy": 0.2, "sweet": 0.2, "savory": 0.8, "light": 0.7},
    "양식": {"spicy": 0.2, "sweet": 0.3, "savory": 0.7, "light": 0.4},
    "아시안": {"spicy": 0.5, "sweet": 0.3, "savory": 0.8, "light": 0.4},
    "분식": {"spicy": 0.6, "sweet": 0.4, "savory": 0.6, "light": 0.2},
    "패스트푸드": {"spicy": 0.2, "sweet": 0.2, "savory": 0.7, "light": 0.1},
    "카페": {"spicy": 0.0, "sweet": 0.8, "savory": 0.2, "light": 0.7},
    "디저트": {"spicy": 0.0, "sweet": 1.0, "savory": 0.1, "light": 0.6},
    "주점": {"spicy": 0.4, "sweet": 0.2, "savory": 0.8, "light": 0.2},
    "샌드위치": {"spicy": 0.1, "sweet": 0.2, "savory": 0.6, "light": 0.8},
    "베이커리": {"spicy": 0.0, "sweet": 0.7, "savory": 0.3, "light": 0.7},
    "샐러드": {"spicy": 0.1, "sweet": 0.2, "savory": 0.5, "light": 1.0},
    "구내식당": {"spicy": 0.3, "sweet": 0.2, "savory": 0.7, "light": 0.5},
    "치킨": {"spicy": 0.5, "sweet": 0.3, "savory": 0.9, "light": 0.2},
}


@dataclass
class UserAccumulator:
    user_id: Optional[int]
    anonymous_cohort_id: Optional[str]
    category_score: Dict[str, float]
    tier_score_sum: float
    tier_weight_sum: float
    taste_score_sum: Dict[str, float]
    taste_weight_sum: float
    time_bucket_score: Dict[str, float]


def _normalize_json_array(raw: str) -> List[str]:
    text = (raw or "").strip()
    if not text:
        return []
    try:
        v = json.loads(text)
        if isinstance(v, list):
            return [str(x) for x in v if str(x).strip()]
    except json.JSONDecodeError:
        pass
    return []


def _parse_context(raw: str) -> dict:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        v = json.loads(text)
        return v if isinstance(v, dict) else {}
    except json.JSONDecodeError:
        return {}


def _to_cohort_id(anonymous_id: str) -> str:
    digest = hashlib.sha1(anonymous_id.encode("utf-8")).hexdigest()[:12]
    return f"anon_cohort_{digest}"


def _user_key(user_id: Optional[int], anonymous_id: Optional[str]) -> Tuple[str, Optional[int], Optional[str]]:
    if user_id is not None:
        return f"u:{user_id}", user_id, None
    anon = anonymous_id or ""
    cohort = _to_cohort_id(anon)
    return f"a:{cohort}", None, cohort


def _time_bucket(occurred_at: str) -> str:
    text = (occurred_at or "").strip()
    if not text:
        return "other"
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            dt = datetime.fromisoformat(normalized.replace(" ", "T", 1))
        except ValueError:
            return "other"

    hour = dt.hour
    if 5 <= hour <= 10:
        return "breakfast"
    if 11 <= hour <= 14:
        return "lunch"
    if 17 <= hour <= 21:
        return "dinner"
    if hour >= 22 or hour <= 4:
        return "late_night"
    return "other"


def _tier_from_score(score: float) -> str:
    for threshold, tier in SCORE_TO_TIER:
        if score < threshold:
            return tier
    return ""


def load_restaurant_features(path: Path) -> Dict[int, dict]:
    out: Dict[int, dict] = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"restaurant_id", "categories", "price_tier"}
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"restaurant_feature missing columns: {', '.join(missing)}")

        for row in reader:
            rid_raw = (row.get("restaurant_id") or "").strip()
            if not rid_raw:
                continue
            try:
                rid = int(rid_raw)
            except ValueError:
                continue
            out[rid] = {
                "categories": _normalize_json_array(row.get("categories") or ""),
                "price_tier": (row.get("price_tier") or "").strip().upper(),
            }
    return out


def build_user_features(feedback_path: Path, restaurant_feature_path: Path) -> List[dict]:
    restaurant_map = load_restaurant_features(restaurant_feature_path)

    users: Dict[str, UserAccumulator] = {}

    with feedback_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"user_id", "anonymous_id", "restaurant_id", "weight", "context", "occurred_at"}
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"implicit_feedback missing columns: {', '.join(missing)}")

        for row in reader:
            user_id_raw = (row.get("user_id") or "").strip()
            anon_id = (row.get("anonymous_id") or "").strip()
            rid_raw = (row.get("restaurant_id") or "").strip()
            if not user_id_raw and not anon_id:
                continue
            if not rid_raw:
                continue

            user_id = None
            if user_id_raw:
                try:
                    user_id = int(user_id_raw)
                except ValueError:
                    continue

            try:
                rid = int(rid_raw)
            except ValueError:
                continue

            try:
                weight = float((row.get("weight") or "").strip() or "1.0")
            except ValueError:
                weight = 1.0

            ukey, uid, cohort = _user_key(user_id, anon_id or None)
            if ukey not in users:
                users[ukey] = UserAccumulator(
                    user_id=uid,
                    anonymous_cohort_id=cohort,
                    category_score=defaultdict(float),
                    tier_score_sum=0.0,
                    tier_weight_sum=0.0,
                    taste_score_sum=defaultdict(float),
                    taste_weight_sum=0.0,
                    time_bucket_score=defaultdict(float),
                )
            acc = users[ukey]

            context = _parse_context(row.get("context") or "")
            ctx_category = (context.get("restaurantCategory") or "").strip() if isinstance(context, dict) else ""

            rf = restaurant_map.get(rid, {})
            categories = []
            if ctx_category:
                categories.append(ctx_category)
            categories.extend(c for c in rf.get("categories", []) if c and c != ctx_category)

            if categories:
                per_cat_weight = weight / max(len(categories), 1)
                for cat in categories:
                    acc.category_score[cat] += per_cat_weight
                    profile = CATEGORY_TASTE_PROFILE.get(cat)
                    if profile:
                        for k, v in profile.items():
                            acc.taste_score_sum[k] += v * per_cat_weight
                acc.taste_weight_sum += weight

            tier = (rf.get("price_tier") or "").strip().upper()
            tier_score = PRICE_TIER_SCORE.get(tier)
            if tier_score is not None:
                acc.tier_score_sum += tier_score * weight
                acc.tier_weight_sum += weight

            bucket = _time_bucket(row.get("occurred_at") or "")
            acc.time_bucket_score[bucket] += weight

    now = datetime.now(timezone.utc).isoformat()
    out_rows: List[dict] = []

    for idx, acc in enumerate(users.values(), start=1):
        # preferred_categories as weighted sorted JSON list
        total_cat = sum(acc.category_score.values())
        preferred = []
        if total_cat > 0:
            for cat, score in sorted(acc.category_score.items(), key=lambda x: x[1], reverse=True):
                preferred.append({"category": cat, "weight": round(score / total_cat, 6)})

        # avg_price_tier
        avg_price_tier = ""
        if acc.tier_weight_sum > 0:
            avg_score = acc.tier_score_sum / acc.tier_weight_sum
            avg_price_tier = _tier_from_score(avg_score)

        # taste_preferences
        taste_preferences = {}
        if acc.taste_weight_sum > 0:
            for k, v in acc.taste_score_sum.items():
                taste_preferences[k] = round(v / acc.taste_weight_sum, 6)

        # visit_time_distribution
        total_time = sum(acc.time_bucket_score.values())
        time_dist = {}
        if total_time > 0:
            for bucket in ["breakfast", "lunch", "dinner", "late_night", "other"]:
                score = acc.time_bucket_score.get(bucket, 0.0)
                if score > 0:
                    time_dist[bucket] = round(score / total_time, 6)

        out_rows.append(
            {
                "id": idx,
                "user_id": acc.user_id if acc.user_id is not None else "",
                "anonymous_cohort_id": acc.anonymous_cohort_id or "",
                "preferred_categories": json.dumps(preferred, ensure_ascii=False),
                "avg_price_tier": avg_price_tier,
                "taste_preferences": json.dumps(taste_preferences, ensure_ascii=False),
                "visit_time_distribution": json.dumps(time_dist, ensure_ascii=False),
                "computed_at": now,
                "created_at": now,
                "updated_at": now,
            }
        )

    return out_rows


def write_rows(rows: List[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "id",
        "user_id",
        "anonymous_cohort_id",
        "preferred_categories",
        "avg_price_tier",
        "taste_preferences",
        "visit_time_distribution",
        "computed_at",
        "created_at",
        "updated_at",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate user_feature.csv")
    parser.add_argument("--feedback", default="output/synthetic/implicit_feedback.csv")
    parser.add_argument("--restaurant-feature", default="output/synthetic/restaurant_feature.csv")
    parser.add_argument("--output", default="output/synthetic/user_feature.csv")
    args = parser.parse_args()

    rows = build_user_features(Path(args.feedback), Path(args.restaurant_feature))
    write_rows(rows, Path(args.output))

    print(f"wrote {len(rows)} rows -> {args.output}")


if __name__ == "__main__":
    main()
