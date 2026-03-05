#!/usr/bin/env python3
"""Generate synthetic implicit-feedback interactions from synthetic users."""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple


SIGNAL_WEIGHTS: Dict[str, float] = {
    "CLICK": 1.0,
    "SAVE": 2.0,
    "CALL": 1.5,
    "ROUTE": 1.7,
    "SHARE": 1.8,
    "REVIEW": 3.0,
}

ACTION_DISTRIBUTION: Dict[str, float] = {
    "CLICK": 0.70,
    "SAVE": 0.10,
    "CALL": 0.05,
    "ROUTE": 0.05,
    "SHARE": 0.05,
    "REVIEW": 0.05,
}

PERSONA_CATEGORY_PREFS: Dict[str, List[str]] = {
    "korean_focus": ["한식"],
    "meat_focus": ["고깃집"],
    "chinese_focus": ["중식"],
    "japanese_focus": ["일식"],
    "western_focus": ["양식"],
    "asian_focus": ["아시안"],
    "snack_focus": ["분식"],
    "fastfood_focus": ["패스트푸드"],
    "cafe_focus": ["카페"],
    "dessert_focus": ["디저트"],
    "pub_focus": ["주점"],
    "sandwich_focus": ["샌드위치"],
    "bakery_focus": ["베이커리"],
    "salad_focus": ["샐러드"],
    "cafeteria_focus": ["구내식당"],
    "chicken_focus": ["치킨"],
    "office_lunch_user": ["구내식당", "한식", "샐러드"],
    "night_food_user": ["고깃집", "주점", "치킨"],
    "weekend_brunch_user": ["카페", "베이커리", "샌드위치", "디저트"],
    "healthy_user": ["샐러드", "샌드위치", "아시안"],
    "social_sharer": ["카페", "디저트", "양식", "주점"],
    "random_explorer": [],
}

ALL_CATEGORIES: List[str] = [
    "한식",
    "고깃집",
    "중식",
    "일식",
    "양식",
    "아시안",
    "분식",
    "패스트푸드",
    "카페",
    "디저트",
    "주점",
    "샌드위치",
    "베이커리",
    "샐러드",
    "구내식당",
    "치킨",
]

CATEGORY_ALIAS = {
    "korean": "한식",
    "meat": "고깃집",
    "chinese": "중식",
    "japanese": "일식",
    "western": "양식",
    "asian": "아시안",
    "snack": "분식",
    "fastfood": "패스트푸드",
    "cafe": "카페",
    "dessert": "디저트",
    "pub": "주점",
    "sandwich": "샌드위치",
    "bakery": "베이커리",
    "salad": "샐러드",
    "cafeteria": "구내식당",
    "chicken": "치킨",
}

PAGE_KEYS = ["home", "restaurant_list", "restaurant_detail", "search", "favorites"]


@dataclass
class User:
    user_key: str
    member_id: Optional[int]
    anonymous_id: Optional[str]
    persona: str
    activity_tier: str


@dataclass
class Restaurant:
    restaurant_id: int
    categories: Tuple[str, ...]


def parse_ts(ts: str) -> datetime:
    normalized = ts.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def load_users(path: Path) -> List[User]:
    users: List[User] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            anonymous_id = row.get("anonymous_id", "").strip() or None
            if not anonymous_id:
                continue
            users.append(
                User(
                    user_key=row["user_key"],
                    member_id=None,
                    anonymous_id=anonymous_id,
                    persona=row["persona"],
                    activity_tier=row["activity_tier"],
                )
            )
    return users


def pick_column(fieldnames: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    lowered = {f.lower(): f for f in fieldnames}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def normalize_category(raw: str) -> str:
    text = raw.strip()
    if text in ALL_CATEGORIES:
        return text
    key = text.lower()
    return CATEGORY_ALIAS.get(key, text if text else random.choice(ALL_CATEGORIES))


def load_restaurants_from_feature(path: Path) -> List[Restaurant]:
    restaurants: List[Restaurant] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        id_col = pick_column(fieldnames, ["restaurant_id", "id", "rest_id"])
        cat_col = pick_column(fieldnames, ["category", "cat", "primary_category"])
        if not id_col:
            raise ValueError("restaurant_feature.csv must contain restaurant_id/id column")

        for row in reader:
            raw_id = (row.get(id_col) or "").strip()
            if not raw_id:
                continue
            try:
                rid = int(raw_id)
            except ValueError:
                continue
            raw_cat = (row.get(cat_col) or "").strip() if cat_col else ""
            category = normalize_category(raw_cat) if raw_cat else random.choice(ALL_CATEGORIES)
            restaurants.append(Restaurant(restaurant_id=rid, categories=(category,)))

    if not restaurants:
        raise ValueError("No restaurants loaded from restaurant_feature.csv")
    return restaurants


def load_restaurants_from_result(path: Path, seed: int) -> List[Restaurant]:
    rng = random.Random(seed)
    ids = set()
    pattern = re.compile(r'"restaurantId"\s*:\s*(\d+)')

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if "restaurantId" not in line:
                continue
            for m in pattern.finditer(line):
                ids.add(int(m.group(1)))

    if not ids:
        raise ValueError("No restaurantId found in result.txt")

    restaurants = [
        Restaurant(restaurant_id=rid, categories=(rng.choice(ALL_CATEGORIES),))
        for rid in sorted(ids)
    ]
    return restaurants


def parse_active_restaurant_ids(path: Path) -> Set[int]:
    active_ids: Set[int] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if "|" not in line:
                continue
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("created_at") and "deleted_at" in stripped and "id" in stripped:
                continue
            if stripped.startswith("-"):
                continue

            parts = [p.strip() for p in line.rstrip("\n").split("|")]
            if len(parts) < 3:
                continue

            deleted_at = parts[1]
            raw_id = parts[2]
            if deleted_at or not raw_id:
                continue
            try:
                active_ids.add(int(raw_id))
            except ValueError:
                continue
    return active_ids


def load_restaurants_from_food_category(
    path: Path,
    active_ids: Optional[Set[int]] = None,
) -> List[Restaurant]:
    categories_by_restaurant: Dict[int, Set[str]] = {}

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if "|" not in line:
                continue
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("restaurant_id") and "food_category_name" in stripped:
                continue
            if stripped.startswith("-"):
                continue

            parts = [p.strip() for p in line.rstrip("\n").split("|")]
            if len(parts) < 4:
                continue

            raw_restaurant_id = parts[0]
            raw_category_name = parts[3]
            if not raw_restaurant_id or not raw_category_name:
                continue

            try:
                restaurant_id = int(raw_restaurant_id)
            except ValueError:
                continue

            if active_ids is not None and restaurant_id not in active_ids:
                continue

            category = normalize_category(raw_category_name)
            categories_by_restaurant.setdefault(restaurant_id, set()).add(category)

    restaurants = [
        Restaurant(restaurant_id=rid, categories=tuple(sorted(cats)))
        for rid, cats in sorted(categories_by_restaurant.items())
        if cats
    ]
    if not restaurants:
        raise ValueError("No restaurants loaded from restaurant_food_category.txt")
    return restaurants


def load_restaurants_from_table(path: Path, seed: int) -> List[Restaurant]:
    """Load restaurant IDs from psql-style table text, using only deleted_at IS NULL rows."""
    rng = random.Random(seed)
    restaurants: List[Restaurant] = []

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if "|" not in line:
                continue
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("created_at") and "deleted_at" in stripped and "id" in stripped:
                continue
            if stripped.startswith("-"):
                continue

            parts = [p.strip() for p in line.rstrip("\n").split("|")]
            if len(parts) < 3:
                continue

            # restaurant.txt columns: created_at | deleted_at | id | ...
            deleted_at = parts[1]
            raw_id = parts[2]
            if deleted_at:
                continue
            if not raw_id:
                continue

            try:
                rid = int(raw_id)
            except ValueError:
                continue

            restaurants.append(
                Restaurant(
                    restaurant_id=rid,
                    categories=(rng.choice(ALL_CATEGORIES),),
                )
            )

    if not restaurants:
        raise ValueError("No active restaurants found in restaurant.txt (deleted_at IS NULL)")
    return restaurants


def events_count_for_tier(rng: random.Random, tier: str) -> int:
    if tier == "heavy":
        return rng.randint(30, 50)
    if tier == "normal":
        return rng.randint(10, 20)
    return rng.randint(3, 8)


def activity_hour_for_persona(rng: random.Random, persona: str) -> int:
    if persona == "office_lunch_user":
        pool = [11, 12, 13, 14] + list(range(9, 18))
        weights = [4, 5, 4, 3] + [1] * 9
        return rng.choices(pool, weights=weights, k=1)[0]
    if persona == "night_food_user":
        pool = [18, 19, 20, 21, 22, 23, 0, 1] + list(range(10, 18))
        weights = [3, 4, 5, 5, 4, 3, 2, 2] + [1] * 8
        return rng.choices(pool, weights=weights, k=1)[0]
    if persona == "weekend_brunch_user":
        return rng.choices([9, 10, 11, 12, 13, 14, 15], weights=[1, 2, 4, 5, 4, 3, 2], k=1)[0]
    return rng.randint(8, 23)


def action_distribution_for_persona(persona: str) -> Dict[str, float]:
    dist = dict(ACTION_DISTRIBUTION)
    if persona == "social_sharer":
        dist["CLICK"] -= 0.12
        dist["SHARE"] += 0.05
        dist["SAVE"] += 0.04
        dist["REVIEW"] += 0.03
    elif persona == "random_explorer":
        dist["CLICK"] += 0.08
        dist["REVIEW"] -= 0.03
        dist["SAVE"] -= 0.02
        dist["ROUTE"] -= 0.01
        dist["CALL"] -= 0.01
        dist["SHARE"] -= 0.01

    total = sum(max(v, 0.001) for v in dist.values())
    return {k: max(v, 0.001) / total for k, v in dist.items()}


def choose_restaurant(
    rng: random.Random,
    restaurants: List[Restaurant],
    persona: str,
    visited: Dict[int, int],
) -> Restaurant:
    preferred = set(PERSONA_CATEGORY_PREFS.get(persona, []))

    weights = []
    for r in restaurants:
        w = 1.0
        if preferred and any(category in preferred for category in r.categories):
            w *= 3.0

        repeat_penalty = 0.75 ** visited.get(r.restaurant_id, 0)
        if persona == "random_explorer":
            repeat_penalty = 0.9 ** visited.get(r.restaurant_id, 0)
        w *= repeat_penalty

        weights.append(max(w, 0.01))

    idx = rng.choices(range(len(restaurants)), weights=weights, k=1)[0]
    chosen = restaurants[idx]
    visited[chosen.restaurant_id] = visited.get(chosen.restaurant_id, 0) + 1
    return chosen


def pick_category_for_event(rng: random.Random, restaurant: Restaurant, persona: str) -> str:
    preferred = set(PERSONA_CATEGORY_PREFS.get(persona, []))
    matched = [c for c in restaurant.categories if c in preferred]
    if matched:
        return rng.choice(matched)
    return rng.choice(list(restaurant.categories))


def generate_interactions(
    users: List[User],
    restaurants: List[Restaurant],
    start_dt: datetime,
    end_dt: datetime,
    seed: int,
) -> List[Dict[str, str]]:
    rng = random.Random(seed)
    if start_dt >= end_dt:
        raise ValueError("start datetime must be earlier than end datetime")

    total_sec = int((end_dt - start_dt).total_seconds())
    rows: List[Dict[str, str]] = []

    for user in users:
        n_events = events_count_for_tier(rng, user.activity_tier)
        action_dist = action_distribution_for_persona(user.persona)
        visited: Dict[int, int] = {}
        session_id = f"sess_{rng.getrandbits(64):016x}"

        for _ in range(n_events):
            base = start_dt + timedelta(seconds=rng.randint(0, total_sec))
            hour = activity_hour_for_persona(rng, user.persona)
            minute = rng.randint(0, 59)
            second = rng.randint(0, 59)
            occurred_at = base.replace(hour=hour, minute=minute, second=second, microsecond=0)
            if occurred_at < start_dt:
                occurred_at = start_dt
            if occurred_at > end_dt:
                occurred_at = end_dt

            restaurant = choose_restaurant(rng, restaurants, user.persona, visited)
            chosen_category = pick_category_for_event(rng, restaurant, user.persona)
            signal = rng.choices(
                population=list(action_dist.keys()),
                weights=list(action_dist.values()),
                k=1,
            )[0]

            context = {
                "source": "CLIENT",
                "platform": "WEB",
                "fromPageKey": rng.choice(PAGE_KEYS),
                "sessionId": session_id,
                "persona": user.persona,
                "restaurantCategory": chosen_category,
            }

            rows.append(
                {
                    "user_key": user.user_key,
                    "member_id": str(user.member_id) if user.member_id is not None else "",
                    "anonymous_id": user.anonymous_id or "",
                    "restaurant_id": str(restaurant.restaurant_id),
                    "restaurant_category": chosen_category,
                    "signal_type": signal,
                    "weight": f"{SIGNAL_WEIGHTS[signal]:.2f}",
                    "context": json.dumps(context, ensure_ascii=False),
                    "occurred_at": occurred_at.isoformat(),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )

    rows.sort(key=lambda r: r["occurred_at"])
    return rows


def write_interactions(rows: List[Dict[str, str]], out_path: Path) -> None:
    fieldnames = [
        "user_key",
        "member_id",
        "anonymous_id",
        "restaurant_id",
        "restaurant_category",
        "signal_type",
        "weight",
        "context",
        "occurred_at",
        "created_at",
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def cap_rows(rows: List[Dict[str, str]], max_events: Optional[int], seed: int) -> List[Dict[str, str]]:
    if max_events is None or max_events <= 0 or len(rows) <= max_events:
        return rows
    rng = random.Random(seed)
    sampled = rng.sample(rows, k=max_events)
    sampled.sort(key=lambda r: r["occurred_at"])
    return sampled


def summarize(rows: List[Dict[str, str]]) -> None:
    signal_counter = Counter(r["signal_type"] for r in rows)
    print("signal distribution:", dict(signal_counter))


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic implicit-feedback interactions")
    parser.add_argument("--users", default="output/synthetic/synthetic_users.csv")
    parser.add_argument("--restaurants", default="restaurant_feature.csv")
    parser.add_argument("--restaurant-table", default="restaurant.txt")
    parser.add_argument("--restaurant-category-table", default="restaurant_food_category.txt")
    parser.add_argument("--fallback-result", default="result.txt")
    parser.add_argument("--start", default="2026-02-01T00:00:00+00:00")
    parser.add_argument("--end", default="2026-03-01T23:59:59+00:00")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-events", type=int, default=None)
    parser.add_argument("--output", default="output/synthetic/synthetic_interactions.csv")

    args = parser.parse_args()

    users = load_users(Path(args.users))
    if not users:
        raise ValueError("No users loaded")

    restaurant_path = Path(args.restaurants)
    restaurant_table_path = Path(args.restaurant_table)
    restaurant_category_table_path = Path(args.restaurant_category_table)
    if restaurant_category_table_path.exists():
        active_ids: Optional[Set[int]] = None
        if restaurant_table_path.exists():
            active_ids = parse_active_restaurant_ids(restaurant_table_path)
        restaurants = load_restaurants_from_food_category(restaurant_category_table_path, active_ids)
        source = str(restaurant_category_table_path)
        if active_ids is not None:
            source += " + active filter from restaurant.txt"
    elif restaurant_path.exists():
        restaurants = load_restaurants_from_feature(restaurant_path)
        source = str(restaurant_path)
    elif restaurant_table_path.exists():
        restaurants = load_restaurants_from_table(restaurant_table_path, args.seed)
        source = f"{restaurant_table_path} (deleted_at IS NULL)"
    else:
        restaurants = load_restaurants_from_result(Path(args.fallback_result), args.seed)
        source = str(args.fallback_result)

    rows = generate_interactions(
        users=users,
        restaurants=restaurants,
        start_dt=parse_ts(args.start),
        end_dt=parse_ts(args.end),
        seed=args.seed,
    )
    rows = cap_rows(rows, args.max_events, args.seed)

    out = Path(args.output)
    write_interactions(rows, out)

    print(f"restaurant source: {source} ({len(restaurants)} rows)")
    print(f"wrote {len(rows)} interactions -> {out}")
    summarize(rows)


if __name__ == "__main__":
    main()
