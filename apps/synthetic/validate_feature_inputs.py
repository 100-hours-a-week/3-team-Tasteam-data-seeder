#!/usr/bin/env python3
"""Validate input CSV schemas for feature generation."""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple


Validator = Callable[[str], bool]


def _is_int(value: str) -> bool:
    try:
        int(value)
        return True
    except Exception:
        return False


def _is_numeric(value: str) -> bool:
    try:
        float(value)
        return True
    except Exception:
        return False


def _is_bool(value: str) -> bool:
    return value.lower() in {"true", "false", "t", "f", "1", "0"}


def _is_timestamp(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    # Accept offsets like '+00' by normalizing to '+00:00'
    text = re.sub(r"([+-]\d{2})$", r"\1:00", text)
    # Normalize fractional seconds to exactly 6 digits for Python 3.9 compatibility.
    text = re.sub(
        r"(\.\d+)(?=(?:[+-]\d{2}:\d{2})?$)",
        lambda m: "." + m.group(1)[1:].ljust(6, "0")[:6],
        text,
    )
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00").replace(" ", "T", 1))
        return True
    except Exception:
        return False


def _is_json_array(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    try:
        parsed = json.loads(text)
        return isinstance(parsed, list)
    except Exception:
        return False


@dataclass(frozen=True)
class ColumnRule:
    required: bool
    validator: Optional[Validator] = None


SCHEMAS: Dict[str, Dict[str, ColumnRule]] = {
    "restaurant": {
        "restaurant_id": ColumnRule(required=True, validator=_is_int),
        "restaurant_name": ColumnRule(required=True),
        "sido": ColumnRule(required=False),
        "sigungu": ColumnRule(required=False),
        "eupmyeondong": ColumnRule(required=False),
        "food_category_id": ColumnRule(required=False, validator=_is_int),
        "food_category_name": ColumnRule(required=False),
    },
    "restaurant_menu_raw": {
        "restaurant_id": ColumnRule(required=True, validator=_is_int),
        "menu_id": ColumnRule(required=True),
        "menu_name": ColumnRule(required=True),
        "menu_price_raw": ColumnRule(required=False),
        "menu_price_num": ColumnRule(required=False, validator=_is_numeric),
        "currency": ColumnRule(required=True),
        "is_representative": ColumnRule(required=True, validator=_is_bool),
        "updated_at": ColumnRule(required=True, validator=_is_timestamp),
    },
    "restaurant_menu_agg": {
        "restaurant_id": ColumnRule(required=True, validator=_is_int),
        "menu_count": ColumnRule(required=True, validator=_is_int),
        "price_min": ColumnRule(required=False, validator=_is_numeric),
        "price_max": ColumnRule(required=False, validator=_is_numeric),
        "price_mean": ColumnRule(required=False, validator=_is_numeric),
        "price_median": ColumnRule(required=False, validator=_is_numeric),
        "representative_menu_name": ColumnRule(required=False),
        "top_menus": ColumnRule(required=True, validator=_is_json_array),
        "price_tier": ColumnRule(required=False),
    },
}


def validate_file(path: Path, schema_name: str, sample_rows: int = 2000) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    rules = SCHEMAS[schema_name]

    if not path.exists():
        return False, [f"file not found: {path}"]

    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        header_set = set(headers)

        missing = [col for col in rules if col not in header_set]
        if missing:
            errors.append(f"missing required columns in header: {', '.join(missing)}")
            return False, errors

        checked = 0
        for row_idx, row in enumerate(reader, start=2):
            for col, rule in rules.items():
                value = (row.get(col) or "").strip()

                if rule.required and value == "":
                    errors.append(f"row {row_idx}: required column '{col}' is empty")
                    if len(errors) >= 30:
                        return False, errors
                    continue

                if value == "":
                    continue

                if rule.validator and not rule.validator(value):
                    errors.append(f"row {row_idx}: invalid value for '{col}' -> {value}")
                    if len(errors) >= 30:
                        return False, errors

            checked += 1
            if checked >= sample_rows:
                break

    return len(errors) == 0, errors


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate feature input CSV schemas")
    parser.add_argument("--restaurant", default="restaurant.csv")
    parser.add_argument("--menu-raw", default="restaurant_menu_raw.csv")
    parser.add_argument("--menu-agg", default="restaurant_menu_agg.csv")
    parser.add_argument("--sample-rows", type=int, default=2000)
    args = parser.parse_args()

    targets: Iterable[Tuple[str, Path]] = [
        ("restaurant", Path(args.restaurant)),
        ("restaurant_menu_raw", Path(args.menu_raw)),
        ("restaurant_menu_agg", Path(args.menu_agg)),
    ]

    has_error = False
    for schema_name, path in targets:
        ok, errors = validate_file(path, schema_name, sample_rows=args.sample_rows)
        if ok:
            print(f"[PASS] {schema_name}: {path}")
        else:
            has_error = True
            print(f"[FAIL] {schema_name}: {path}")
            for err in errors:
                print(f"  - {err}")

    if has_error:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
