import time
import random
import logging
import requests
import os
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --------------------------
# CONFIG (ENV VARIABLES)
# --------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
CSFLOAT_API_KEY = os.getenv("CSFLOAT_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not CSFLOAT_API_KEY:
    raise Exception("Missing environment variables")

CSFLOAT_LISTINGS_URL = "https://csfloat.com/api/v1/listings"

REQUEST_DELAY = 1.0
MAX_RETRIES = 5

# --------------------------
# SUPABASE
# --------------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --------------------------
# HTTP SESSION
# --------------------------
session = requests.Session()
session.headers.update({
    "Authorization": CSFLOAT_API_KEY,
    "Accept": "application/json",
})

# --------------------------
# FX RATE (USD → EUR)
# --------------------------
def get_usd_eur_rate():
    try:
        url = "https://open.er-api.com/v6/latest/USD"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        return data["rates"]["EUR"]
    except Exception as e:
        logging.error("FX error: %s", e)
        return None


# --------------------------
# CSFLOAT PRICE (USD)
# --------------------------
def get_lowest_csfloat_price(market_hash_name: str):
    params = {
        "market_hash_name": market_hash_name,
        "sort_by": "lowest_price",
        "limit": 1,
        "type": "buy_now"
    }

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(CSFLOAT_LISTINGS_URL, params=params, timeout=15)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                wait = retry_after + random.uniform(0, 0.5)
                logging.warning("Rate limit %s → čakám %.1fs", market_hash_name, wait)
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                logging.error("CSFloat error %d: %s", resp.status_code, resp.text[:200])
                return None

            data = resp.json()
            listings = data.get("data") or data.get("listings") or data.get("results") if isinstance(data, dict) else data

            if not listings:
                return None

            first = listings[0]
            price_cents = first.get("price")

            if price_cents is None:
                return None

            return price_cents / 100  # USD

        except requests.exceptions.Timeout:
            logging.warning("Timeout %s", market_hash_name)
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logging.error("Request error %s: %s", market_hash_name, e)
            time.sleep(1)

    return None


# --------------------------
# SUPABASE FETCH
# --------------------------
def fetch_all_skins():
    try:
        resp = supabase.table("skins").select("market_hash_name, price").execute()
        return {s["market_hash_name"]: s["price"] for s in resp.data}
    except Exception as e:
        logging.critical("Supabase error: %s", e)
        raise SystemExit(1)


# --------------------------
# UPDATE
# --------------------------
def update_skin_price(market_hash_name: str, price: float):
    try:
        supabase.table("skins").update({
            "price": price
        }).eq("market_hash_name", market_hash_name).execute()
        return True
    except Exception as e:
        logging.error("Update error %s: %s", market_hash_name, e)
        return False


# --------------------------
# MAIN
# --------------------------
def main():
    logging.info("Spúšťam scraper (USD → EUR MODE)")

    skins = fetch_all_skins()

    if not skins:
        logging.warning("Žiadne skiny v DB")
        return

    rate = get_usd_eur_rate()

    if rate is None:
        logging.error("Nepodarilo sa získať USD→EUR kurz")
        return

    logging.info("USD→EUR kurz: %.6f", rate)
    logging.info("Načítaných %d skinov", len(skins))

    stats = {"updated": 0, "not_found": 0, "failed": 0}

    for i, (name, old_price) in enumerate(skins.items(), start=1):

        usd_price = get_lowest_csfloat_price(name)

        if usd_price is None:
            logging.info("[%d/%d] %s: nenájdené", i, len(skins), name)
            stats["not_found"] += 1
        else:
            eur_price = round(usd_price * rate, 2)

            ok = update_skin_price(name, eur_price)

            if ok:
                logging.info("[%d/%d] %s: $%.2f → €%.2f",
                             i, len(skins), name, old_price, eur_price)
                stats["updated"] += 1
            else:
                stats["failed"] += 1

        time.sleep(REQUEST_DELAY)

    logging.info("=" * 60)
    logging.info("HOTOVO → updated=%d not_found=%d failed=%d",
                 stats["updated"], stats["not_found"], stats["failed"])


if __name__ == "__main__":
    main()
