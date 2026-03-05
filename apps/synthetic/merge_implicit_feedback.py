#!/usr/bin/env python3
"""Merge real + synthetic implicit feedback into a single training dataset."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

REQUIRED_COLS = [
    "user_id",
    "anonymous_id",
    "restaurant_id",
    "signal_type",
    "weight",
    "context",
    "occurred_at",
    "created_at",
]


def _parse_time(value: str) -> datetime:
    text = (value or "").strip()
    if not text:
        return datetime.min
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        try:
            return datetime.fromisoformat(normalized.replace(" ", "T", 1))
        except ValueError:
            return datetime.min


def _sort_epoch(value: str) -> float:
    dt = _parse_time(value)
    if dt == datetime.min:
        return float("-inf")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).timestamp()
    return dt.timestamp()


def _read_rows(path: Path, source: str, add_source_col: bool) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        missing = [c for c in REQUIRED_COLS if c not in headers]
        if missing:
            raise ValueError(f"{path} missing columns: {', '.join(missing)}")

        out: List[Dict[str, str]] = []
        for row in reader:
            normalized = {col: (row.get(col) or "").strip() for col in REQUIRED_COLS}
            if add_source_col:
                normalized["data_source"] = source
            out.append(normalized)
        return out


def _row_key(row: Dict[str, str]) -> Tuple[str, ...]:
    return (
        row.get("user_id", ""),
        row.get("anonymous_id", ""),
        row.get("restaurant_id", ""),
        row.get("signal_type", ""),
        row.get("occurred_at", ""),
        row.get("context", ""),
    )


def merge_rows(
    real_rows: List[Dict[str, str]],
    synthetic_rows: List[Dict[str, str]],
    deduplicate: bool,
) -> List[Dict[str, str]]:
    merged = real_rows + synthetic_rows

    if deduplicate:
        seen = set()
        unique_rows = []
        for row in merged:
            key = _row_key(row)
            if key in seen:
                continue
            seen.add(key)
            unique_rows.append(row)
        merged = unique_rows

    merged.sort(key=lambda r: _sort_epoch(r.get("occurred_at", "")))
    return merged


def write_rows(rows: List[Dict[str, str]], out_path: Path, add_source_col: bool) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(REQUIRED_COLS)
    if add_source_col:
        fieldnames.append("data_source")

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge implicit feedback (real + synthetic)")
    parser.add_argument("--real", default="output/synthetic/implicit_feedback_real.csv")
    parser.add_argument("--synthetic", default="output/synthetic/implicit_feedback_synthetic.csv")
    parser.add_argument("--output", default="output/synthetic/implicit_feedback_mixed.csv")
    parser.add_argument("--no-deduplicate", action="store_true")
    parser.add_argument("--no-source-col", action="store_true")
    args = parser.parse_args()

    add_source_col = not args.no_source_col
    real_rows = _read_rows(Path(args.real), "real", add_source_col=add_source_col)
    synthetic_rows = _read_rows(Path(args.synthetic), "synthetic", add_source_col=add_source_col)

    merged = merge_rows(
        real_rows=real_rows,
        synthetic_rows=synthetic_rows,
        deduplicate=not args.no_deduplicate,
    )
    write_rows(merged, Path(args.output), add_source_col=add_source_col)

    print(f"real={len(real_rows)}, synthetic={len(synthetic_rows)}, merged={len(merged)} -> {args.output}")


if __name__ == "__main__":
    main()
