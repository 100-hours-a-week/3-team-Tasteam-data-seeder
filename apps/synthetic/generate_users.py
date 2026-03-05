#!/usr/bin/env python3
"""Generate synthetic users for recommendation simulation."""

from __future__ import annotations

import argparse
import csv
import json
import random
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


CATEGORY_PERSONAS: List[str] = [
    "korean_focus",
    "meat_focus",
    "chinese_focus",
    "japanese_focus",
    "western_focus",
    "asian_focus",
    "snack_focus",
    "fastfood_focus",
    "cafe_focus",
    "dessert_focus",
    "pub_focus",
    "sandwich_focus",
    "bakery_focus",
    "salad_focus",
    "cafeteria_focus",
    "chicken_focus",
]

LIFESTYLE_PERSONAS: List[str] = [
    "office_lunch_user",
    "night_food_user",
    "weekend_brunch_user",
    "healthy_user",
    "social_sharer",
    "random_explorer",
]

# 70% category-focused, 30% lifestyle-focused.
PERSONA_WEIGHTS: Dict[str, float] = {
    "korean_focus": 0.06,
    "meat_focus": 0.05,
    "chinese_focus": 0.04,
    "japanese_focus": 0.04,
    "western_focus": 0.05,
    "asian_focus": 0.04,
    "snack_focus": 0.03,
    "fastfood_focus": 0.03,
    "cafe_focus": 0.08,
    "dessert_focus": 0.06,
    "pub_focus": 0.04,
    "sandwich_focus": 0.03,
    "bakery_focus": 0.05,
    "salad_focus": 0.03,
    "cafeteria_focus": 0.03,
    "chicken_focus": 0.04,
    "office_lunch_user": 0.08,
    "night_food_user": 0.07,
    "weekend_brunch_user": 0.05,
    "healthy_user": 0.04,
    "social_sharer": 0.03,
    "random_explorer": 0.03,
}

ACTIVITY_TIER_WEIGHTS: Dict[str, float] = {
    "heavy": 0.10,
    "normal": 0.60,
    "light": 0.30,
}

PREFERRED_CATEGORIES: Dict[str, List[str]] = {
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


@dataclass
class UserRow:
    user_key: str
    member_id: str
    anonymous_id: str
    is_anonymous: int
    persona: str
    persona_group: str
    activity_tier: str
    preferred_categories: str


def weighted_choice(rng: random.Random, weights: Dict[str, float]) -> str:
    keys = list(weights.keys())
    probs = list(weights.values())
    return rng.choices(keys, weights=probs, k=1)[0]


def normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    total = sum(weights.values())
    if total <= 0:
        raise ValueError("sum of weights must be > 0")
    return {k: v / total for k, v in weights.items()}


def build_users(num_users: int, anonymous_ratio: float, seed: int) -> List[UserRow]:
    rng = random.Random(seed)
    persona_weights = normalize_weights(PERSONA_WEIGHTS)
    tier_weights = normalize_weights(ACTIVITY_TIER_WEIGHTS)

    anon_count = round(num_users * anonymous_ratio)
    member_count = num_users - anon_count

    rows: List[UserRow] = []

    for idx in range(1, member_count + 1):
        persona = weighted_choice(rng, persona_weights)
        tier = weighted_choice(rng, tier_weights)
        persona_group = "category_focus" if persona in CATEGORY_PERSONAS else "lifestyle"
        preferred = json.dumps(PREFERRED_CATEGORIES[persona], ensure_ascii=False)

        rows.append(
            UserRow(
                user_key=f"m_{idx:04d}",
                member_id=str(idx),
                anonymous_id="",
                is_anonymous=0,
                persona=persona,
                persona_group=persona_group,
                activity_tier=tier,
                preferred_categories=preferred,
            )
        )

    for idx in range(1, anon_count + 1):
        persona = weighted_choice(rng, persona_weights)
        tier = weighted_choice(rng, tier_weights)
        persona_group = "category_focus" if persona in CATEGORY_PERSONAS else "lifestyle"
        preferred = json.dumps(PREFERRED_CATEGORIES[persona], ensure_ascii=False)

        rows.append(
            UserRow(
                user_key=f"a_{idx:04d}",
                member_id="",
                anonymous_id=str(uuid.uuid4()),
                is_anonymous=1,
                persona=persona,
                persona_group=persona_group,
                activity_tier=tier,
                preferred_categories=preferred,
            )
        )

    rng.shuffle(rows)
    return rows


def write_users(rows: List[UserRow], out_path: Path) -> None:
    fieldnames = [
        "user_key",
        "member_id",
        "anonymous_id",
        "is_anonymous",
        "persona",
        "persona_group",
        "activity_tier",
        "preferred_categories",
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def summarize(rows: List[UserRow]) -> Tuple[Counter, Counter, Counter]:
    persona_counter = Counter(r.persona for r in rows)
    tier_counter = Counter(r.activity_tier for r in rows)
    anon_counter = Counter(r.is_anonymous for r in rows)
    return persona_counter, tier_counter, anon_counter


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic users")
    parser.add_argument("--num-users", type=int, default=2000)
    parser.add_argument("--anonymous-ratio", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="output/synthetic/synthetic_users.csv")

    args = parser.parse_args()

    rows = build_users(
        num_users=args.num_users,
        anonymous_ratio=args.anonymous_ratio,
        seed=args.seed,
    )
    out = Path(args.output)
    write_users(rows, out)

    persona_counter, tier_counter, anon_counter = summarize(rows)
    print(f"wrote {len(rows)} users -> {out}")
    print(f"anonymous={anon_counter.get(1,0)}, member={anon_counter.get(0,0)}")
    print("activity_tier:", dict(tier_counter))
    print("top personas:", dict(persona_counter.most_common(10)))


if __name__ == "__main__":
    main()
