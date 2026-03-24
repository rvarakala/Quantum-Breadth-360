"""
In-memory + disk cache for breadth engine.
All other modules import get_cache / set_cache from here.
"""
import json
import pathlib
import logging
from datetime import datetime, timezone
from typing import Dict

logger = logging.getLogger(__name__)

# ── In-memory breadth cache ───────────────────────────────────────────────────
_cache: Dict[str, dict] = {}
CACHE_TTL = 28800   # 8 hours — covers full trading day (recompute after market close)

# ── Disk cache — survives backend restarts ────────────────────────────────────
_DISK_CACHE_FILE = pathlib.Path(__file__).parent / "breadth_cache.json"

def _save_disk_cache():
    """Persist current cache to disk so next startup loads instantly."""
    try:
        serializable = {}
        for key, val in _cache.items():
            try:
                serializable[key] = {
                    "data": val["data"],
                    "ts":   val["ts"].isoformat()
                }
            except Exception:
                pass
        with open(_DISK_CACHE_FILE, "w") as f:
            json.dump(serializable, f)
        logger.info(f"Disk cache saved: {len(serializable)} entries")
    except Exception as e:
        logger.warning(f"Could not save disk cache: {e}")

def _load_disk_cache():
    """Load cache from disk on startup — avoids full recompute."""
    if not _DISK_CACHE_FILE.exists():
        return
    try:
        with open(_DISK_CACHE_FILE) as f:
            data = json.load(f)
        loaded = 0
        skipped = 0
        for key, val in data.items():
            ts = datetime.fromisoformat(val["ts"])
            age = (datetime.now(timezone.utc) - ts).total_seconds()
            if age >= CACHE_TTL:
                continue  # expired
            # Sanity check: skip breadth entries with suspiciously low universe or PANIC scores
            # This prevents corrupt/stale cache (e.g. COVID crash data) from being loaded
            entry_data = val["data"]
            if key.startswith("breadth_") and isinstance(entry_data, dict):
                universe = entry_data.get("universe_size", entry_data.get("valid", 999))
                score = entry_data.get("score", 50)
                regime = entry_data.get("regime", "")
                if universe < 100:
                    logger.warning(f"Disk cache SKIP '{key}': universe_size={universe} too small (likely partial data)")
                    skipped += 1
                    continue
                if score < 10 and regime == "PANIC":
                    logger.warning(f"Disk cache SKIP '{key}': score={score} regime={regime} (likely stale crash data)")
                    skipped += 1
                    continue
            _cache[key] = {"data": entry_data, "ts": ts}
            loaded += 1
        logger.info(f"Disk cache loaded: {loaded}/{len(data)} entries (skipped {skipped} suspicious)")
    except Exception as e:
        logger.warning(f"Could not load disk cache: {e}")

def get_cache(key):
    if key in _cache:
        age = (datetime.now(timezone.utc) - _cache[key]["ts"]).total_seconds()
        if age < CACHE_TTL:
            return _cache[key]["data"]
    return None

def set_cache(key, data):
    _cache[key] = {"data": data, "ts": datetime.now(timezone.utc)}
    _save_disk_cache()   # persist immediately so restart loads from disk
