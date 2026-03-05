#!/usr/bin/env python3
"""Build implicit_feedback dataset from either raw event table or synthetic interactions CSV."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional


DEFAULT_SIGNAL_WEIGHTS: Dict[str, float] = {
    "CLICK": 1.0,
    "SAVE": 2.0,
    "CALL": 1.5,
    "ROUTE": 1.7,
    "SHARE": 1.8,
    "REVIEW": 3.0,
}

EVENT_TO_SIGNAL: Dict[str, str] = {
    "ui.restaurant.clicked": "CLICK",
    "ui.favorite.updated": "SAVE",
    "ui.review.write_started": "REVIEW",
    "ui.review.submitted": "REVIEW",
}

CONTEXT_KEYS = {
    "fromPageKey",
    "sessionId",
    "platform",
    "source",
    "pageKey",
    "pathTemplate",
    "referrerPathTemplate",
    "position",
    "groupId",
    "subgroupId",
    "toTab",
    "fromTab",
}


@dataclass
class RawEvent:
    event_name: str
    occurred_at: str
    member_id: Optional[int]
    anonymous_id: Optional[str]
    properties: Dict[str, Any]


@dataclass
class ImplicitFeedbackRow:
    user_id: Optional[int]
    anonymous_id: Optional[str]
    restaurant_id: int
    signal_type: str
    weight: float
    context: Dict[str, Any]
    occurred_at: str
    created_at: str


def parse_int_or_none(value: str) -> Optional[int]:
    text = (value or "").strip()
    if not text:
        return None
    return int(text)


def normalize_text(value: str) -> Optional[str]:
    text = (value or "").strip()
    return text if text else None


def parse_psql_table(path: Path) -> Iterator[RawEvent]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if "|" not in line:
                continue

            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("id") and "event_name" in stripped:
                continue
            if stripped.startswith("-"):
                continue

            parts = [p.strip() for p in line.rstrip("\n").split("|")]
            if len(parts) < 17:
                continue

            event_name = parts[2]
            occurred_at = parts[4]
            member_id = parse_int_or_none(parts[5])
            anonymous_id = normalize_text(parts[6])
            raw_properties = parts[15]

            try:
                properties = json.loads(raw_properties) if raw_properties else {}
            except json.JSONDecodeError:
                continue

            yield RawEvent(
                event_name=event_name,
                occurred_at=occurred_at,
                member_id=member_id,
                anonymous_id=anonymous_id,
                properties=properties,
            )


def build_context_from_event(event_name: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    context = {k: properties[k] for k in CONTEXT_KEYS if k in properties}
    context["eventName"] = event_name
    return context


def to_feedback_from_event(
    raw: RawEvent,
    signal_weights: Dict[str, float],
    include_review_started: bool,
) -> Optional[ImplicitFeedbackRow]:
    signal = EVENT_TO_SIGNAL.get(raw.event_name)
    if signal is None:
        return None

    if raw.event_name == "ui.review.write_started" and not include_review_started:
        return None

    restaurant_id = raw.properties.get("restaurantId")
    if restaurant_id is None:
        return None

    if raw.event_name == "ui.favorite.updated":
        selected = raw.properties.get("selectedTargetCount")
        if isinstance(selected, int) and selected <= 0:
            return None

    try:
        restaurant_id_int = int(restaurant_id)
    except (TypeError, ValueError):
        return None

    weight = signal_weights.get(signal)
    if weight is None:
        return None

    created_at = datetime.now(timezone.utc).isoformat()

    return ImplicitFeedbackRow(
        user_id=raw.member_id,
        anonymous_id=raw.anonymous_id,
        restaurant_id=restaurant_id_int,
        signal_type=signal,
        weight=float(weight),
        context=build_context_from_event(raw.event_name, raw.properties),
        occurred_at=raw.occurred_at,
        created_at=created_at,
    )


def parse_weight_overrides(raw: Optional[str]) -> Dict[str, float]:
    if not raw:
        return {}
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("--weight-overrides must be a JSON object")

    overrides: Dict[str, float] = {}
    for k, v in data.items():
        key = str(k).upper()
        if key not in DEFAULT_SIGNAL_WEIGHTS:
            raise ValueError(f"Unknown signal in override: {key}")
        overrides[key] = float(v)
    return overrides


def write_csv(rows: Iterable[ImplicitFeedbackRow], out_path: Path) -> None:
    fieldnames = [
        "user_id",
        "anonymous_id",
        "restaurant_id",
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
        for row in rows:
            writer.writerow(
                {
                    "user_id": row.user_id,
                    "anonymous_id": row.anonymous_id,
                    "restaurant_id": row.restaurant_id,
                    "signal_type": row.signal_type,
                    "weight": row.weight,
                    "context": json.dumps(row.context, ensure_ascii=False),
                    "occurred_at": row.occurred_at,
                    "created_at": row.created_at,
                }
            )


def build_rows_from_result(
    in_path: Path,
    signal_weights: Dict[str, float],
    include_review_started: bool,
) -> list[ImplicitFeedbackRow]:
    rows: list[ImplicitFeedbackRow] = []
    for event in parse_psql_table(in_path):
        row = to_feedback_from_event(event, signal_weights, include_review_started)
        if row:
            rows.append(row)
    return rows


def parse_context(raw_context: str) -> Dict[str, Any]:
    text = (raw_context or "").strip()
    if not text:
        return {}
    try:
        val = json.loads(text)
        return val if isinstance(val, dict) else {}
    except json.JSONDecodeError:
        return {}


def build_rows_from_synthetic_csv(
    in_path: Path,
    signal_weights: Dict[str, float],
) -> list[ImplicitFeedbackRow]:
    rows: list[ImplicitFeedbackRow] = []

    with in_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            restaurant_raw = (r.get("restaurant_id") or "").strip()
            signal_type = (r.get("signal_type") or "").strip().upper()
            occurred_at = (r.get("occurred_at") or "").strip()
            if not restaurant_raw or not signal_type or not occurred_at:
                continue

            try:
                restaurant_id = int(restaurant_raw)
            except ValueError:
                continue

            if signal_type not in DEFAULT_SIGNAL_WEIGHTS:
                continue

            weight_raw = (r.get("weight") or "").strip()
            if weight_raw:
                try:
                    weight = float(weight_raw)
                except ValueError:
                    weight = signal_weights[signal_type]
            else:
                weight = signal_weights[signal_type]

            user_id = parse_int_or_none(r.get("member_id") or r.get("user_id") or "")
            anonymous_id = normalize_text(r.get("anonymous_id") or "")
            context = parse_context(r.get("context") or "")
            created_at = (r.get("created_at") or "").strip() or datetime.now(timezone.utc).isoformat()

            rows.append(
                ImplicitFeedbackRow(
                    user_id=user_id,
                    anonymous_id=anonymous_id,
                    restaurant_id=restaurant_id,
                    signal_type=signal_type,
                    weight=weight,
                    context=context,
                    occurred_at=occurred_at,
                    created_at=created_at,
                )
            )

    return rows


def detect_input_type(input_path: Path) -> str:
    if input_path.suffix.lower() == ".csv":
        return "synthetic"
    return "result"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build implicit_feedback rows")
    parser.add_argument("--input", default="result.txt", help="Input file path")
    parser.add_argument("--input-type", choices=["auto", "result", "synthetic"], default="auto")
    parser.add_argument(
        "--output",
        default="output/synthetic/implicit_feedback.csv",
        help="Output file path",
    )
    parser.add_argument(
        "--weight-overrides",
        default=None,
        help='JSON object. Example: {"CLICK": 1.0, "REVIEW": 4.0}',
    )
    parser.add_argument(
        "--exclude-review-started",
        action="store_true",
        help="Exclude ui.review.write_started from REVIEW signals (result input only)",
    )

    args = parser.parse_args()

    in_path = Path(args.input)
    input_type = detect_input_type(in_path) if args.input_type == "auto" else args.input_type

    weight_overrides = parse_weight_overrides(args.weight_overrides)
    signal_weights = {**DEFAULT_SIGNAL_WEIGHTS, **weight_overrides}

    if input_type == "result":
        rows = build_rows_from_result(
            in_path,
            signal_weights,
            include_review_started=not args.exclude_review_started,
        )
    else:
        rows = build_rows_from_synthetic_csv(in_path, signal_weights)

    out_path = Path(args.output)
    write_csv(rows, out_path)

    print(f"input_type={input_type}, wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    main()
