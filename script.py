import os
import time
import random
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

import requests
from supabase import create_client, Client

# Načítanie premenných prostredia z .env súboru
load_dotenv()

# =========================================================
# CONFIG - NAČÍTANIE Z PREMENNÝCH PROSTREDIA
# =========================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") 
CSFLOAT_API_KEY = os.getenv("CSFLOAT_API_KEY")

# Kontrola, či sú všetky potrebné premenné nastavené
if not all([SUPABASE_URL, SUPABASE_KEY, CSFLOAT_API_KEY]):
    raise ValueError("Chýbajúce premenné prostredia. Skontrolujte .env súbor.")

CSFLOAT_LISTINGS_URL = "https://csfloat.com/api/v1/listings"

REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.2"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
FX_FALLBACK = float(os.getenv("FX_FALLBACK", "0.92"))

# =========================================================
# LOGGING
# =========================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# =========================================================
# CLIENTS
# =========================================================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

session = requests.Session()
session.headers.update({
    "Authorization": CSFLOAT_API_KEY,
    "Accept": "application/json",
    "User-Agent": "CS2-Skin-Tracker/1.0"
})

# =========================================================
# FX RATE USD -> EUR
# =========================================================
def get_usd_eur_rate() -> float:
    try:
        url = os.getenv("FX_API_URL", "https://open.er-api.com/v6/latest/USD")
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        rate = float(data["rates"]["EUR"])
        logging.info("USD -> EUR kurz: %.4f", rate)
        return rate
    except Exception as e:
        logging.warning("Nepodarilo sa získať FX kurz, používam fallback %.2f | %s", FX_FALLBACK, e)
        return FX_FALLBACK

# =========================================================
# FETCH SKINS
# =========================================================
def fetch_all_skins():
    try:
        resp = supabase.table("skins").select("market_hash_name, price").execute()
        return resp.data or []
    except Exception as e:
        logging.critical("Chyba pri načítaní skins: %s", e)
        return []

# =========================================================
# GET CURRENT PRICE FROM CSFLOAT (USD)
# =========================================================
def get_lowest_csfloat_price_usd(market_hash_name: str):
    params = {
        "market_hash_name": market_hash_name,
        "sort_by": "lowest_price",
        "limit": 1,
        "type": "buy_now"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(CSFLOAT_LISTINGS_URL, params=params, timeout=15)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                wait = retry_after + random.uniform(0, 0.5)
                logging.warning("Rate limit pre %s, čakám %.1fs", market_hash_name, wait)
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                logging.error("CSFloat HTTP %d pre %s | %s", resp.status_code, market_hash_name, resp.text[:200])
                return None

            data = resp.json()

            if isinstance(data, dict):
                listings = data.get("data") or data.get("listings") or data.get("results")
            else:
                listings = data

            if not listings or not isinstance(listings, list):
                return None

            first = listings[0]
            if not isinstance(first, dict):
                return None

            price_cents = first.get("price")
            if price_cents is None:
                return None

            return float(price_cents) / 100.0

        except requests.exceptions.Timeout:
            logging.warning("Timeout pre %s (attempt %d/%d)", market_hash_name, attempt, MAX_RETRIES)
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logging.warning("Request error pre %s (attempt %d/%d): %s", market_hash_name, attempt, MAX_RETRIES, e)
            time.sleep(1)

        except Exception as e:
            logging.error("Neočakávaná chyba pre %s: %s", market_hash_name, e)
            time.sleep(1)

    return None

# =========================================================
# UPDATE CURRENT PRICE IN SKINS
# =========================================================
def update_skin_price(market_hash_name: str, price_eur: float) -> bool:
    try:
        supabase.table("skins").update({
            "price": price_eur
        }).eq("market_hash_name", market_hash_name).execute()
        return True
    except Exception as e:
        logging.error("Update skins error pre %s: %s", market_hash_name, e)
        return False

# =========================================================
# INSERT HISTORY POINT
# =========================================================
def insert_price_history(market_hash_name: str, price_eur: float, source: str = "csfloat") -> bool:
    try:
        now_iso = datetime.now(timezone.utc).isoformat()

        supabase.table("price_history").insert({
            "market_hash_name": market_hash_name,
            "price": price_eur,
            "source": source,
            "recorded_at": now_iso
        }).execute()

        return True
    except Exception as e:
        logging.error("Insert history error pre %s: %s", market_hash_name, e)
        return False

# =========================================================
# PROCESS ONE SKIN
# =========================================================
def process_skin(market_hash_name: str, old_price, fx_rate: float, index: int, total: int):
    old_price = float(old_price or 0)

    usd_price = get_lowest_csfloat_price_usd(market_hash_name)

    if usd_price is None:
        logging.info("[%d/%d] %s -> cena nenájdená", index, total, market_hash_name)
        return "not_found"

    eur_price = round(usd_price * fx_rate, 2)

    updated = update_skin_price(market_hash_name, eur_price)
    if not updated:
        logging.info("[%d/%d] %s -> update skins zlyhal", index, total, market_hash_name)
        return "failed"

    history_ok = insert_price_history(market_hash_name, eur_price, source="csfloat")
    if not history_ok:
        logging.info("[%d/%d] %s -> insert history zlyhal", index, total, market_hash_name)
        return "failed"

    change_pct = 0
    if old_price > 0:
        change_pct = ((eur_price - old_price) / old_price) * 100

    icon = "🟢" if change_pct >= 0 else "🔴"
    logging.info(
        "[%d/%d] %s -> €%.2f %s (predtým €%.2f | %.2f%%)",
        index, total, market_hash_name, eur_price, icon, old_price, change_pct
    )

    return "updated"

# =========================================================
# MAIN
# =========================================================
def main():
    print("=" * 70)
    print("CS2 SKIN PRICE TRACKER - CSFLOAT SNAPSHOT HISTORY")
    print("=" * 70)

    skins = fetch_all_skins()
    if not skins:
        logging.warning("Žiadne skiny v databáze.")
        return

    logging.info("Načítaných %d skinov", len(skins))

    fx_rate = get_usd_eur_rate()
    if fx_rate <= 0:
        logging.error("Neplatný FX kurz.")
        return

    stats = {
        "updated": 0,
        "not_found": 0,
        "failed": 0
    }

    started = time.time()

    for i, skin in enumerate(skins, start=1):
        market_hash_name = skin.get("market_hash_name")
        old_price = skin.get("price", 0)

        if not market_hash_name:
            stats["failed"] += 1
            continue

        result = process_skin(market_hash_name, old_price, fx_rate, i, len(skins))

        if result == "updated":
            stats["updated"] += 1
        elif result == "not_found":
            stats["not_found"] += 1
        else:
            stats["failed"] += 1

        if i < len(skins):
            sleep_time = REQUEST_DELAY + random.uniform(0, 0.4)
            time.sleep(sleep_time)

    duration = time.time() - started

    print("\n" + "=" * 70)
    print(f"HOTOVO za {duration:.1f}s")
    print(f"Updated:   {stats['updated']}")
    print(f"Not found: {stats['not_found']}")
    print(f"Failed:    {stats['failed']}")
    print("=" * 70)

if __name__ == "__main__":
    main()
