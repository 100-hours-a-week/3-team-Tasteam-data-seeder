from __future__ import annotations

import json
import os
from typing import Optional

from .places_service import Place


def safe_filename(name: str) -> str:
    if not name or not name.strip():
        return "menu"
    s = name.strip()
    for c in r'\/:*?"<>|':
        s = s.replace(c, "")
    s = s.replace(" ", "_")
    return s or "menu"


def get_menu_for_place(
    place: Place,
    cache_dir: str,
) -> Optional[dict]:
    os.makedirs(os.path.join(cache_dir, "menu"), exist_ok=True)
    cache_path = os.path.join(cache_dir, "menu", f"{safe_filename(place.name)}.json")

    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None
