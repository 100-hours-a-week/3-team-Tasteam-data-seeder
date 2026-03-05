#!/usr/bin/env python3
"""Seeder orchestration entrypoint.

This script only parses CLI options and delegates work to service modules.
"""

from __future__ import annotations

import argparse
import os
import sys
import warnings
from pathlib import Path

try:
    from apps.seeder import config
    from apps.seeder.services.dml_service import build_dml_from_api, build_dml_from_local
except ModuleNotFoundError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from apps.seeder import config
    from apps.seeder.services.dml_service import build_dml_from_api, build_dml_from_local

warnings.filterwarnings("ignore", message=".*NotOpenSSLWarning.*")


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end DML pipeline")
    parser.add_argument(
        "--mode",
        default="api",
        choices=["local", "api"],
        help="data source mode (api requires GCP_API_KEY/API_KEY)",
    )
    parser.add_argument("--lat", type=float, required=True, help="center latitude")
    parser.add_argument("--lng", type=float, required=True, help="center longitude")
    parser.add_argument("--menus-dir", default=config.DEFAULT_MENUS_DIR, help="menu JSON directory")
    parser.add_argument("--places-glob", default=config.DEFAULT_PLACES_GLOB, help="places JSON glob")
    parser.add_argument("--override-json", default=None, help="override JSON (optional)")
    parser.add_argument("--out", default=config.DEFAULT_DML_OUT, help="DML output file")
    parser.add_argument("--start-id", type=int, default=9000, help="restaurant starting ID")
    parser.add_argument(
        "--id-mode",
        choices=["hash", "sequential"],
        default="hash",
        help="restaurant ID 생성 방식 (hash 권장)",
    )
    parser.add_argument("--cache-dir", default=config.DEFAULT_CACHE_DIR, help="cache directory")
    parser.add_argument("--radius", type=float, default=500.0, help="radius (m)")
    parser.add_argument("--rank", default="DISTANCE", choices=["DISTANCE", "POPULARITY"], help="rank")
    parser.add_argument("--lang", default="ko", help="language")
    parser.add_argument("--type", dest="place_type", default="restaurant", help="place includedTypes")
    parser.add_argument("--max", dest="max_count", type=int, default=20, help="max places")
    parser.add_argument("--report", default=config.DEFAULT_REPORT_OUT, help="write report JSON to this path")
    parser.add_argument("--dry-run", action="store_true", help="no file write")
    args = parser.parse_args()

    if args.mode == "local":
        report = build_dml_from_local(
            menus_dir=args.menus_dir,
            places_glob=args.places_glob,
            out_path=args.out,
            override_json=args.override_json,
            start_id=args.start_id,
            id_mode=args.id_mode,
            lat=args.lat,
            lng=args.lng,
            dry_run=args.dry_run,
            report_path=args.report,
        )
    else:
        api_key = os.environ.get("GCP_API_KEY") or os.environ.get("API_KEY")
        if not api_key:
            raise SystemExit("GCP_API_KEY 또는 API_KEY 환경 변수를 설정하세요.")
        report = build_dml_from_api(
            api_key=api_key,
            lat=args.lat,
            lng=args.lng,
            radius=args.radius,
            rank=args.rank,
            lang=args.lang,
            place_type=args.place_type,
            max_count=args.max_count,
            cache_dir=args.cache_dir,
            out_path=args.out,
            start_id=args.start_id,
            id_mode=args.id_mode,
            dry_run=args.dry_run,
            report_path=args.report,
        )

    print(f"written: {args.out}")
    print(f"appended: {report.get('appended_restaurants')}")
    print(f"skipped: {report.get('skipped_restaurants')}")


if __name__ == "__main__":
    main()
