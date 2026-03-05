"""Shared defaults for seeder apps."""

from __future__ import annotations

OUTPUT_ROOT = "output/seeder"

DEFAULT_DML_OUT = f"{OUTPUT_ROOT}/dml_output.sql"
DEFAULT_CACHE_DIR = f"{OUTPUT_ROOT}/cache"
DEFAULT_REPORT_OUT = f"{OUTPUT_ROOT}/report.json"

DEFAULT_PLACES_JSON = f"{OUTPUT_ROOT}/ktb_res.json"
DEFAULT_PLACES_GLOB = f"{OUTPUT_ROOT}/ktb_res*.json"
DEFAULT_MENU_TARGETS = f"{OUTPUT_ROOT}/menu_targets.json"
DEFAULT_MENUS_DIR = f"{OUTPUT_ROOT}/menus"

DEFAULT_LAT = 37.402052
DEFAULT_LNG = 127.107058
