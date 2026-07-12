#!/usr/bin/env python3
"""
CS2 Skin Price Tracker – BULK LISTINGS VERSION

Features:
- Uses CSFloat /listings pagination (100 items per page)
- Respects rate limit headers to avoid 429s
- Schema for fast normal/StatTrak prices
- Bulk listings for Souvenirs + missing skins
- Early exit when all wanted prices are found
- Batch writes + history tracking
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from supabase import Client, create_client
from urllib3.util.retry import Retry

# ═══════════════════════════════════════════════════════════════
# LOAD ENVIRONMENT
# ═══════════════════════════════════════════════════════════════
load_dotenv()

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
CSFLOAT_API_KEY = os.getenv("CSFLOAT_API_KEY", "")

CSFLOAT_LISTINGS_URL = "https://csfloat.com/api/v1/listings"
CSFLOAT_SCHEMA_URL = "https://csfloat.com/api/v1/schema"
FX_URL = "https://open.er-api.com/v6/latest/USD"

FX_FALLBACK = float(os.getenv("FX_FALLBACK", "0.92"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "1000"))
SCHEMA_SAVE_BATCH_SIZE = int(os.getenv("SCHEMA_SAVE_BATCH_SIZE", "250"))
LISTINGS_SAVE_BATCH_SIZE = int(os.getenv("LISTINGS_SAVE_BATCH_SIZE", "50"))
HISTORY_INSERT_CHUNK_SIZE = int(os.getenv("HISTORY_INSERT_CHUNK_SIZE", "500"))

# Bulk listings settings
LISTINGS_PAGE_LIMIT = int(os.getenv("LISTINGS_PAGE_LIMIT", "100"))
LISTINGS_INTER_REQUEST_SECONDS = float(os.getenv("LISTINGS_INTER_REQUEST_SECONDS", "3.0"))
LISTINGS_MAX_PAGES = int(os.getenv("LISTINGS_MAX_PAGES", "4000"))
LISTINGS_TIMEOUT_SECONDS = float(os.getenv("LISTINGS_TIMEOUT_SECONDS", "30"))

PRICE_EPSILON = float(os.getenv("PRICE_EPSILON", "0.005"))
TRY_SKINS_BATCH_UPSERT = os.getenv("TRY_SKINS_BATCH_UPSERT", "1").strip().lower() not in {"0", "false", "no"}

WEAR_NAMES = [
    "Factory New",
    "Minimal Wear",
    "Field-Tested",
    "Well-Worn",
    "Battle-Scarred",
]

WEAR_TO_CANONICAL = {
    "Factory New": "Factory New",
    "Minimal Wear": "Minimal Wear",
    "Field Tested": "Field-Tested",
    "Field-Tested": "Field-Tested",
    "Well Worn": "Well-Worn",
    "Well-Worn": "Well-Worn",
    "Battle Scarred": "Battle-Scarred",
    "Battle-Scarred": "Battle-Scarred",
}

CANONICAL_TO_SPACE = {
    "Field-Tested": "Field Tested",
    "Well-Worn": "Well Worn",
    "Battle-Scarred": "Battle Scarred",
}

WEAR_PATTERN = re.compile(
    r"\((Factory New|Minimal Wear|Field Tested|Field-Tested|Well Worn|Well-Worn|Battle Scarred|Battle-Scarred)\)$"
)

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

for noisy_logger in (
    "httpx", "httpcore", "urllib3", "postgrest", "supabase",
    "gotrue", "storage3", "realtime",
):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════
@dataclass
class RunStats:
    started_at: float = field(default_factory=time.time)
    total_skins: int = 0
    normal_count: int = 0
    souvenir_count: int = 0
    stattrak_count: int = 0
    schema_found: int = 0
    schema_not_found: int = 0
    listings_found: int = 0
    listings_not_found: int = 0
    listings_429: int = 0
    listings_timeouts: int = 0
    listings_request_errors: int = 0
    listings_pages_fetched: int = 0
    skipped_unchanged: int = 0
    queued_updates: int = 0
    successful_skin_writes: int = 0
    successful_history_writes: int = 0
    failed_skin_writes: int = 0
    failed_history_writes: int = 0

    def elapsed(self) -> float:
        return time.time() - self.started_at

    def total_found(self) -> int:
        return self.schema_found + self.listings_found

    def coverage_pct(self) -> float:
        if self.total_skins <= 0:
            return 0.0
        return 100.0 * self.total_found() / self.total_skins


STATS = RunStats()

# ═══════════════════════════════════════════════════════════════
# HTTP SESSION
# ═══════════════════════════════════════════════════════════════
def make_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


HTTP = make_http_session()


def csfloat_headers() -> Dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "CS2-Skin-Tracker/3.0",
    }
    if CSFLOAT_API_KEY:
        headers["Authorization"] = CSFLOAT_API_KEY
    return headers

# ═══════════════════════════════════════════════════════════════
# VALIDATION / SUPABASE CLIENT
# ═══════════════════════════════════════════════════════════════
def validate_config() -> None:
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_KEY")
    if not CSFLOAT_API_KEY:
        missing.append("CSFLOAT_API_KEY")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def make_supabase_client() -> Client:
    validate_config()
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ═══════════════════════════════════════════════════════════════
# NAME NORMALIZATION
# ═══════════════════════════════════════════════════════════════
def normalize_wear_text(wear: str, canonical: bool = True) -> str:
    wear = (wear or "").strip()
    canonical_wear = WEAR_TO_CANONICAL.get(wear, wear)
    if canonical:
        return canonical_wear
    return CANONICAL_TO_SPACE.get(canonical_wear, canonical_wear)


def normalize_market_hash_name(name: str, canonical: bool = True) -> str:
    if not name:
        return name
    text = str(name).strip()
    match = WEAR_PATTERN.search(text)
    if not match:
        return text
    old_wear = match.group(1)
    new_wear = normalize_wear_text(old_wear, canonical=canonical)
    return text[: match.start(1)] + new_wear + text[match.end(1):]


def strip_star_prefix(name: str) -> str:
    """Remove ★ prefix from knife/glove names for fuzzy matching."""
    if not name:
        return name
    return re.sub(r"^★\s*", "", name).strip()


def market_hash_variants(name: str) -> List[str]:
    if not name:
        return []
    original = str(name).strip()
    canonical = normalize_market_hash_name(original, canonical=True)
    spaced = normalize_market_hash_name(original, canonical=False)
    no_star = strip_star_prefix(canonical)
    no_star_spaced = strip_star_prefix(spaced)

    with_star_canonical = f"★ {canonical}" if not canonical.startswith("★") else canonical
    with_star_spaced = f"★ {spaced}" if not spaced.startswith("★") else spaced

    variants: List[str] = []
    for candidate in (canonical, original, spaced, with_star_canonical, with_star_spaced, no_star, no_star_spaced):
        candidate = candidate.strip() if candidate else candidate
        if candidate and candidate not in variants:
            variants.append(candidate)
    return variants


def add_lookup_variant(lookup: Dict[str, float], name: str, price_cents: float) -> None:
    for variant in market_hash_variants(name):
        lookup[variant] = float(price_cents)


def skin_group(name: str) -> str:
    if name.startswith("StatTrak™"):
        return "StatTrak"
    if name.startswith("Souvenir"):
        return "Souvenir"
    return "Normal"


def is_same_price(old_price: float, new_price: float) -> bool:
    return abs(float(old_price or 0.0) - float(new_price or 0.0)) < PRICE_EPSILON

# ═══════════════════════════════════════════════════════════════
# FX RATE
# ═══════════════════════════════════════════════════════════════
def get_usd_eur_rate() -> float:
    try:
        resp = HTTP.get(FX_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        rate = float(data["rates"]["EUR"])
        logging.info("USD -> EUR rate: %.4f", rate)
        return rate
    except Exception as e:
        logging.warning("FX rate failed, using fallback %.2f | %s", FX_FALLBACK, str(e)[:150])
        return FX_FALLBACK

# ═══════════════════════════════════════════════════════════════
# FETCH ALL SKINS
# ═══════════════════════════════════════════════════════════════
def fetch_all_skins(supabase: Client, page_size: int = PAGE_SIZE) -> List[dict]:
    all_skins: List[dict] = []
    start = 0
    while True:
        end = start + page_size - 1
        try:
            resp = (
                supabase.table("skins")
                .select("market_hash_name, price")
                .order("market_hash_name")
                .range(start, end)
                .execute()
            )
            rows = resp.data or []
            all_skins.extend(rows)
            logging.info("📦 Loaded page %d-%d | %d rows | total %d", start, end, len(rows), len(all_skins))
            if len(rows) < page_size:
                break
            start += page_size
        except Exception as e:
            logging.critical("Error fetching skins page %d-%d: %s", start, end, str(e)[:300])
            break
    return all_skins

# ═══════════════════════════════════════════════════════════════
# SUPABASE WRITE HELPERS
# ═══════════════════════════════════════════════════════════════
def chunked(items: Sequence[dict], size: int) -> Iterable[List[dict]]:
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


def batch_upsert_skins(supabase: Client, updates: List[dict]) -> Tuple[int, bool]:
    if not updates:
        return 0, True
    try:
        payload = [
            {"market_hash_name": row["market_hash_name"], "price": row["price"]}
            for row in updates
        ]
        resp = supabase.table("skins").upsert(payload, on_conflict="market_hash_name").execute()
        return len(payload), True
    except Exception as e:
        logging.warning("Batch upsert failed, falling back to individual updates: %s", str(e)[:250])
        return 0, False


def update_skins_one_by_one(supabase: Client, updates: List[dict]) -> int:
    if not updates:
        return 0
    success = 0
    for record in updates:
        try:
            supabase.table("skins").update(
                {"price": record["price"]}
            ).eq("market_hash_name", record["market_hash_name"]).execute()
            success += 1
        except Exception as e:
            STATS.failed_skin_writes += 1
            logging.error("Update failed for %s: %s", record.get("market_hash_name", "?"), str(e)[:250])
    return success


def save_skin_updates(supabase: Client, updates: List[dict], allow_batch_upsert: bool) -> Tuple[int, bool]:
    if not updates:
        return 0, allow_batch_upsert
    if TRY_SKINS_BATCH_UPSERT and allow_batch_upsert:
        count, supported = batch_upsert_skins(supabase, updates)
        if supported:
            STATS.successful_skin_writes += count
            return count, True
        allow_batch_upsert = False
    count = update_skins_one_by_one(supabase, updates)
    STATS.successful_skin_writes += count
    return count, allow_batch_upsert


def insert_history_records(supabase: Client, records: List[dict]) -> int:
    if not records:
        return 0
    inserted_total = 0
    for part in chunked(records, HISTORY_INSERT_CHUNK_SIZE):
        try:
            resp = supabase.table("price_history").insert(part).execute()
            inserted = len(resp.data or []) if resp.data is not None else len(part)
            inserted_total += inserted
        except Exception as e:
            STATS.failed_history_writes += len(part)
            logging.error("Insert history failed (%d): %s", len(part), str(e)[:250])
    STATS.successful_history_writes += inserted_total
    return inserted_total


def flush_writes(
    supabase: Client,
    skin_updates: List[dict],
    history_records: List[dict],
    label: str,
    allow_batch_upsert: bool,
) -> Tuple[List[dict], List[dict], bool]:
    if not skin_updates and not history_records:
        return [], [], allow_batch_upsert

    updated, allow_batch_upsert = save_skin_updates(supabase, skin_updates, allow_batch_upsert)
    inserted = insert_history_records(supabase, history_records)
    logging.info("💾 %s saved: skins %d/%d, history %d/%d", label, updated, len(skin_updates), inserted, len(history_records))
    return [], [], allow_batch_upsert

# ═══════════════════════════════════════════════════════════════
# CSFLOAT SCHEMA LOOKUP
# ═══════════════════════════════════════════════════════════════
def build_schema_lookup(schema: dict) -> Dict[str, float]:
    lookup: Dict[str, float] = {}
    knife_names = {
        "Bayonet", "Bowie Knife", "Butterfly Knife", "Classic Knife",
        "Falchion Knife", "Flip Knife", "Gut Knife", "Huntsman Knife",
        "Kukri Knife", "Navaja Knife", "Nomad Knife", "Paracord Knife",
        "Skeleton Knife", "Stiletto Knife", "Survival Knife", "Talon Knife",
        "Ursus Knife", "Shadow Daggers", "M9 Bayonet", "Karambit",
    }
    glove_names = {
        "Bloodhound Gloves", "Broken Fang Gloves", "Driver Gloves",
        "Hydra Gloves", "Moto Gloves", "Specialist Gloves", "Sport Gloves",
        "Hand Wraps",
    }

    weapons = schema.get("weapons", {}) or {}
    for weapon_id, weapon_data in weapons.items():
        if not isinstance(weapon_data, dict):
            continue
        weapon_name = weapon_data.get("name", "") or ""
        if not weapon_name:
            continue
        weapon_type = weapon_data.get("type", "") or ""
        if weapon_type not in ("Weapons", "Knives", "Gloves", ""):
            continue

        is_knife_or_glove = weapon_name in knife_names or weapon_name in glove_names
        paints = weapon_data.get("paints", {}) or {}

        for paint_id, paint_data in paints.items():
            if not isinstance(paint_data, dict):
                continue
            paint_name = paint_data.get("name", "") or ""
            if not paint_name:
                continue

            # Normal prices
            normal_prices = paint_data.get("normal_prices", []) or []
            for wear_index, price_cents in enumerate(normal_prices):
                if wear_index >= len(WEAR_NAMES):
                    continue
                if isinstance(price_cents, (int, float)) and price_cents > 0:
                    base = f"{weapon_name} | {paint_name} ({WEAR_NAMES[wear_index]})"
                    add_lookup_variant(lookup, base, float(price_cents))
                    if is_knife_or_glove:
                        add_lookup_variant(lookup, f"★ {base}", float(price_cents))

            # StatTrak prices
            stattrak_prices = paint_data.get("stattrak_prices", []) or []
            for wear_index, price_cents in enumerate(stattrak_prices):
                if wear_index >= len(WEAR_NAMES):
                    continue
                if isinstance(price_cents, (int, float)) and price_cents > 0:
                    name = f"StatTrak™ {weapon_name} | {paint_name} ({WEAR_NAMES[wear_index]})"
                    add_lookup_variant(lookup, name, float(price_cents))
                    if is_knife_or_glove:
                        add_lookup_variant(lookup, f"★ {name}", float(price_cents))

            # Note: souvenir_prices is empty in schema → handled via bulk listings
    return lookup


def fetch_schema_lookup() -> Dict[str, float]:
    logging.info("📡 Phase 1: Fetching CSFloat schema...")
    resp = HTTP.get(CSFLOAT_SCHEMA_URL, headers=csfloat_headers(), timeout=40)
    resp.raise_for_status()
    schema = resp.json()
    lookup = build_schema_lookup(schema)
    logging.info("✅ Schema loaded – %d lookup variants", len(lookup))
    return lookup


def lookup_schema_price(schema_lookup: Dict[str, float], market_hash_name: str) -> Optional[float]:
    for variant in market_hash_variants(market_hash_name):
        price = schema_lookup.get(variant)
        if price is not None:
            return price
    return None

# ═══════════════════════════════════════════════════════════════
# BULK CSFLOAT LISTINGS
# ═══════════════════════════════════════════════════════════════
# Rate limit state
rate_state: Dict[str, float] = {
    "remaining": 200.0,
    "reset_at": 0.0,
    "last_request": 0.0,
}


def wait_for_rate_limit() -> None:
    """Respect rate limit headers and inter-request delay."""
    global rate_state
    now = time.time()

    # Inter-request delay
    since_last = now - rate_state.get("last_request", 0.0)
    if since_last < LISTINGS_INTER_REQUEST_SECONDS:
        time.sleep(LISTINGS_INTER_REQUEST_SECONDS - since_last)
        now = time.time()

    # If we are low on remaining requests, wait for reset
    remaining = rate_state.get("remaining")
    if remaining is not None and remaining < 10:
        reset_at = rate_state.get("reset_at", 0)
        if reset_at > now:
            wait_s = reset_at - now + 5
            logging.info("🛑 Rate limit almost exhausted (%d left), waiting %.0fs for reset...",
                         int(remaining), wait_s)
            time.sleep(wait_s)
            now = time.time()
            rate_state["remaining"] = 200.0

    rate_state["last_request"] = time.time()


def update_rate_state_from_headers(headers: Any) -> None:
    global rate_state
    try:
        rem = headers.get("X-Ratelimit-Remaining")
        reset = headers.get("X-Ratelimit-Reset")
        if rem is not None:
            rate_state["remaining"] = float(rem)
        if reset is not None:
            rate_state["reset_at"] = float(reset)
        logging.debug("Rate state: remaining=%.0f reset_in=%.0fs",
                      rate_state.get("remaining", 0),
                      max(0, rate_state.get("reset_at", 0) - time.time()))
    except Exception:
        pass


def extract_listings_from_response(data: Any) -> List[dict]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "listings", "results"):
            val = data.get(key)
            if isinstance(val, list):
                return val
    return []


def fetch_listings_page(page: int) -> Tuple[List[dict], bool]:
    """Fetch one listings page. Returns (listings, should_stop)."""
    params = {
        "sort_by": "lowest_price",
        "limit": LISTINGS_PAGE_LIMIT,
        "page": page,
    }
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        wait_for_rate_limit()
        try:
            resp = HTTP.get(
                CSFLOAT_LISTINGS_URL,
                params=params,
                headers=csfloat_headers(),
                timeout=LISTINGS_TIMEOUT_SECONDS,
            )
            update_rate_state_from_headers(resp.headers)

            if resp.status_code == 429:
                STATS.listings_429 += 1
                reset_at = rate_state.get("reset_at", 0)
                wait_s = max(30, reset_at - time.time() + 5) if reset_at > time.time() else 60
                logging.warning("🚫 429 on page %d, waiting %.0fs...", page, wait_s)
                time.sleep(min(wait_s, 1800))
                continue

            if resp.status_code in (500, 502, 503, 504):
                wait_s = min(30 * attempt, 120)
                logging.warning("⚠️ HTTP %s on page %d, retrying in %ds (attempt %d/%d)",
                                resp.status_code, page, wait_s, attempt, max_attempts)
                time.sleep(wait_s)
                continue

            if resp.status_code != 200:
                logging.warning("HTTP %s on page %d: %s", resp.status_code, page, resp.text[:200])
                if resp.status_code == 400:
                    return [], True  # past the end
                return [], False

            listings = extract_listings_from_response(resp.json())
            return listings, (len(listings) == 0)

        except requests.Timeout:
            STATS.listings_timeouts += 1
            wait_s = min(20 * attempt, 90)
            logging.warning("Timeout on page %d (attempt %d/%d), waiting %ds", page, attempt, max_attempts, wait_s)
            time.sleep(wait_s)
        except requests.RequestException as e:
            STATS.listings_request_errors += 1
            wait_s = min(20 * attempt, 90)
            logging.warning("Request error on page %d: %s, waiting %ds", page, str(e)[:150], wait_s)
            time.sleep(wait_s)
        except Exception as e:
            STATS.listings_request_errors += 1
            logging.warning("Other error on page %d: %s", page, str(e)[:200])
            time.sleep(30)

    return [], False


def bulk_fetch_listings_prices(
    wanted_names: List[str],
    fx_rate: float,
) -> Dict[str, float]:
    """Fetch prices using paginated listings. Returns name -> EUR price."""
    result: Dict[str, float] = {}
    variant_to_wanted: Dict[str, str] = {}

    for name in wanted_names:
        for v in market_hash_variants(name):
            variant_to_wanted[v] = name

    total_wanted = len(set(wanted_names))
    if total_wanted == 0:
        return result

    logging.info("📡 BULK listings: searching prices for %d skins (page size=%d)",
                 total_wanted, LISTINGS_PAGE_LIMIT)

    consecutive_empty = 0

    for page in range(LISTINGS_MAX_PAGES):
        listings, stop = fetch_listings_page(page)
        STATS.listings_pages_fetched += 1

        if stop:
            logging.info("🏁 Reached end of listings (empty page) at page=%d", page)
            break

        if not listings:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                logging.warning("3 consecutive empty pages, stopping")
                break
            continue

        consecutive_empty = 0

        page_new = 0
        for listing in listings:
            if not isinstance(listing, dict):
                continue
            item = listing.get("item", {}) or {}
            mhn = item.get("market_hash_name")
            price_cents = listing.get("price")

            if not mhn or price_cents is None:
                continue

            target = None
            if mhn in variant_to_wanted:
                target = variant_to_wanted[mhn]
            else:
                for v in market_hash_variants(mhn):
                    if v in variant_to_wanted:
                        target = variant_to_wanted[v]
                        break

            if target is None:
                continue

            price_eur = round(float(price_cents) / 100.0 * fx_rate, 2)

            if target not in result or price_eur < result[target]:
                if target not in result:
                    page_new += 1
                result[target] = price_eur

        # Progress logging
        if page % 5 == 0 or page_new > 0 or len(result) >= total_wanted:
            found = len(result)
            logging.info(
                "📄 page %d | new: %d | found: %d/%d (%.1f%%) | elapsed: %.0fs",
                page, page_new, found, total_wanted,
                100.0 * found / total_wanted, STATS.elapsed(),
            )

        if len(result) >= total_wanted:
            logging.info("✅ All wanted skins have prices! Stopping listings early.")
            break

    return result

# ═══════════════════════════════════════════════════════════════
# PROCESSING HELPERS
# ═══════════════════════════════════════════════════════════════
def order_skins(skins: List[dict]) -> List[dict]:
    normal_skins: List[dict] = []
    souvenir_skins: List[dict] = []
    stattrak_skins: List[dict] = []

    for skin in skins:
        name = skin.get("market_hash_name", "") or ""
        group = skin_group(name)
        if group == "StatTrak":
            stattrak_skins.append(skin)
        elif group == "Souvenir":
            souvenir_skins.append(skin)
        else:
            normal_skins.append(skin)

    STATS.normal_count = len(normal_skins)
    STATS.souvenir_count = len(souvenir_skins)
    STATS.stattrak_count = len(stattrak_skins)

    logging.info("📊 Skin groups:")
    logging.info("   🟢 Normal:    %d", len(normal_skins))
    logging.info("   🟡 Souvenir:  %d", len(souvenir_skins))
    logging.info("   🔴 StatTrak™: %d", len(stattrak_skins))

    return normal_skins + souvenir_skins + stattrak_skins


def make_history_record(market_hash_name: str, price: float, source: str) -> dict:
    return {
        "market_hash_name": market_hash_name,
        "price": price,
        "source": source,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }


def queue_price_change(
    market_hash_name: str,
    eur_price: float,
    old_prices: Dict[str, float],
    source: str,
    skin_updates: List[dict],
    history_records: List[dict],
) -> bool:
    old_price = float(old_prices.get(market_hash_name, 0.0) or 0.0)
    if is_same_price(old_price, eur_price):
        STATS.skipped_unchanged += 1
        return False

    skin_updates.append({"market_hash_name": market_hash_name, "price": eur_price})
    history_records.append(make_history_record(market_hash_name, eur_price, source))
    STATS.queued_updates += 1
    return True


def log_price_result(
    index: int,
    total: int,
    market_hash_name: str,
    eur_price: float,
    old_price: float,
    source: str,
    extra: str = "",
) -> None:
    change_pct = ((eur_price - old_price) / old_price * 100.0) if old_price > 0 else 0.0
    icon = "🟢" if change_pct >= 0 else "🔴"
    group = skin_group(market_hash_name)
    logging.info(
        "[%d/%d] (%s) %s -> €%.2f %s %+.1f%% %s%s",
        index, total, group, market_hash_name, eur_price, icon, change_pct, source,
        f" | {extra}" if extra else "",
    )

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main() -> None:
    print("=" * 72)
    print("CS2 SKIN PRICE TRACKER – BULK LISTINGS VERSION")
    print("=" * 72)

    try:
        supabase = make_supabase_client()
    except Exception as e:
        logging.critical("Configuration failed: %s", e)
        sys.exit(1)

    skins = fetch_all_skins(supabase)
    if not skins:
        logging.warning("❌ No skins found in database.")
        return

    ordered_skins = order_skins(skins)
    STATS.total_skins = len(ordered_skins)
    total = STATS.total_skins

    logging.info("📦 Loaded %d skins from database", total)

    fx_rate = get_usd_eur_rate()
    if fx_rate <= 0:
        logging.error("❌ Invalid FX rate.")
        return

    try:
        schema_lookup = fetch_schema_lookup()
    except Exception as e:
        logging.critical("Schema fetch failed: %s", str(e)[:300])
        return

    old_prices = {
        skin.get("market_hash_name"): float(skin.get("price", 0) or 0)
        for skin in ordered_skins
        if skin.get("market_hash_name")
    }

    not_found_list: List[str] = []
    skin_updates: List[dict] = []
    history_records: List[dict] = []
    allow_batch_upsert = TRY_SKINS_BATCH_UPSERT

    # ─────────────────────────────────────────────────────────────
    # PHASE 1: SCHEMA
    # ─────────────────────────────────────────────────────────────
    for index, skin in enumerate(ordered_skins, start=1):
        market_hash_name = skin.get("market_hash_name")
        if not market_hash_name:
            continue

        price_cents = lookup_schema_price(schema_lookup, market_hash_name)
        if price_cents is None:
            not_found_list.append(market_hash_name)
            STATS.schema_not_found += 1
            continue

        eur_price = round((float(price_cents) / 100.0) * fx_rate, 2)
        STATS.schema_found += 1

        queued = queue_price_change(
            market_hash_name=market_hash_name,
            eur_price=eur_price,
            old_prices=old_prices,
            source="csfloat_schema",
            skin_updates=skin_updates,
            history_records=history_records,
        )

        if index <= 5 or index % 500 == 0 or index == total:
            old_price = old_prices.get(market_hash_name, 0.0)
            extra = "unchanged, skipped" if not queued else ""
            log_price_result(index, total, market_hash_name, eur_price, old_price, "schema", extra)

        if len(skin_updates) >= SCHEMA_SAVE_BATCH_SIZE:
            skin_updates, history_records, allow_batch_upsert = flush_writes(
                supabase, skin_updates, history_records, "Schema batch", allow_batch_upsert
            )

    if skin_updates or history_records:
        skin_updates, history_records, allow_batch_upsert = flush_writes(
            supabase, skin_updates, history_records, "Final schema batch", allow_batch_upsert
        )

    elapsed_schema = STATS.elapsed()
    print()
    print(f"✅ Phase 1 (schema) finished in {elapsed_schema:.0f}s")
    print(f"   Found:     {STATS.schema_found}/{total} ({100.0 * STATS.schema_found / total:.1f}%)")
    print(f"   Missing:   {len(not_found_list)}")
    print(f"   Skipped:   {STATS.skipped_unchanged}")

    # ─────────────────────────────────────────────────────────────
    # PHASE 2: BULK LISTINGS
    # ─────────────────────────────────────────────────────────────
    if not not_found_list:
        logging.info("🎯 All skins already have schema prices – no listings needed.")
    else:
        print()
        print(f"📡 Phase 2: BULK listings for {len(not_found_list)} missing skins")
        print(f"   Page limit: {LISTINGS_PAGE_LIMIT}, max pages: {LISTINGS_MAX_PAGES}")
        print(f"   Inter-request delay: {LISTINGS_INTER_REQUEST_SECONDS}s")
        print()

        # Optimistic initial rate state
        rate_state["remaining"] = 180.0
        rate_state["reset_at"] = time.time() + 3600
        rate_state["last_request"] = 0.0

        listings_prices = bulk_fetch_listings_prices(not_found_list, fx_rate)

        for market_hash_name in not_found_list:
            eur_price = listings_prices.get(market_hash_name)
            if eur_price is None:
                STATS.listings_not_found += 1
                continue

            STATS.listings_found += 1
            queue_price_change(
                market_hash_name=market_hash_name,
                eur_price=eur_price,
                old_prices=old_prices,
                source="csfloat_listings",
                skin_updates=skin_updates,
                history_records=history_records,
            )

            if len(skin_updates) >= LISTINGS_SAVE_BATCH_SIZE:
                skin_updates, history_records, allow_batch_upsert = flush_writes(
                    supabase, skin_updates, history_records, "Listings batch", allow_batch_upsert
                )

        if skin_updates or history_records:
            skin_updates, history_records, allow_batch_upsert = flush_writes(
                supabase, skin_updates, history_records, "Final listings batch", allow_batch_upsert
            )

    # ─────────────────────────────────────────────────────────────
    # SUMMARY
    # ─────────────────────────────────────────────────────────────
    duration = STATS.elapsed()
    print()
    print("=" * 72)
    print(f"✅ DONE in {duration:.1f}s")
    print(f"   Skins in DB: {total}")
    print(f"   Normal: {STATS.normal_count}  |  Souvenir: {STATS.souvenir_count}  |  StatTrak™: {STATS.stattrak_count}")
    print("-" * 72)
    print(f"   Schema found:         {STATS.schema_found}")
    print(f"   Listings BULK found:  {STATS.listings_found}")
    print(f"   Not found:            {STATS.listings_not_found}")
    print(f"   Total covered:        {STATS.total_found()}/{total} ({STATS.coverage_pct():.1f}%)")
    print("-" * 72)
    print(f"   Queued price changes: {STATS.queued_updates}")
    print(f"   Skipped (unchanged):  {STATS.skipped_unchanged}")
    print(f"   Skin writes:          {STATS.successful_skin_writes}")
    print(f"   History writes:       {STATS.successful_history_writes}")
    print(f"   Failed writes:        {STATS.failed_skin_writes + STATS.failed_history_writes}")
    print("-" * 72)
    print(f"   Listings pages fetched: {STATS.listings_pages_fetched}")
    print(f"   429 responses:          {STATS.listings_429}")
    print(f"   Timeouts / errors:      {STATS.listings_timeouts + STATS.listings_request_errors}")
    print("=" * 72)


if __name__ == "__main__":
    main()
