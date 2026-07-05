import os
import time
import random
import logging

import requests
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# --------------------------
# CONFIG (GitHub Secrets)
# --------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
CSFLOAT_API_KEY = os.environ["CSFLOAT_API_KEY"]

CSFLOAT_LISTINGS_URL = "https://csfloat.com/api/v1/listings"

REQUEST_DELAY = 1.0
MAX_RETRIES = 5

# USD -> EUR
EUR_RATE = 0.92

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
# CSFLOAT
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
            response = session.get(
                CSFLOAT_LISTINGS_URL,
                params=params,
                timeout=15
            )

            if response.status_code == 429:
                retry_after = int(
                    response.headers.get("Retry-After", 2 ** attempt)
                )

                wait = retry_after + random.uniform(0, 0.5)

                logging.warning(
                    "Rate limit (%s). Waiting %.1fs...",
                    market_hash_name,
                    wait,
                )

                time.sleep(wait)
                continue

            if response.status_code != 200:
                logging.error(
                    "CSFloat %d: %s",
                    response.status_code,
                    response.text[:200],
                )
                return None

            data = response.json()

            if isinstance(data, dict):
                listings = (
                    data.get("data")
                    or data.get("listings")
                    or data.get("results")
                )
            else:
                listings = data

            if not listings:
                return None

            listing = listings[0]

            if not isinstance(listing, dict):
                return None

            price_cents = listing.get("price")

            if price_cents is None:
                return None

            usd_price = price_cents / 100
            eur_price = usd_price * EUR_RATE

            return round(eur_price, 2)

        except requests.exceptions.Timeout:
            logging.warning("Timeout: %s", market_hash_name)
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logging.error("%s: %s", market_hash_name, e)
            time.sleep(1)

    return None


# --------------------------
# SUPABASE FUNCTIONS
# --------------------------
def fetch_all_skins():
    try:
        response = (
            supabase.table("skins")
            .select("market_hash_name, price")
            .execute()
        )

        return {
            skin["market_hash_name"]: skin["price"]
            for skin in response.data
        }

    except Exception as e:
        logging.critical("Supabase Error: %s", e)
        raise SystemExit(1)


def update_skin_price(name: str, price: float):
    try:
        (
            supabase.table("skins")
            .update({"price": price})
            .eq("market_hash_name", name)
            .execute()
        )

        return True

    except Exception as e:
        logging.error("Update failed (%s): %s", name, e)
        return False


# --------------------------
# MAIN
# --------------------------
def main():
    logging.info("Starting CSFloat scraper...")
    logging.info("Mode: BUY NOW")
    logging.info("Currency: EUR")

    skins = fetch_all_skins()

    if not skins:
        logging.warning("No skins found.")
        return

    total = len(skins)

    logging.info("Loaded %d skins.", total)

    stats = {
        "updated": 0,
        "not_found": 0,
        "failed": 0,
    }

    for index, (name, old_price) in enumerate(skins.items(), start=1):

        new_price = get_lowest_csfloat_price(name)

        if new_price is None:

            logging.info(
                "[%d/%d] %s -> NOT FOUND",
                index,
                total,
                name,
            )

            stats["not_found"] += 1

        else:

            if update_skin_price(name, new_price):

                logging.info(
                    "[%d/%d] %s | %.2f€ -> %.2f€",
                    index,
                    total,
                    name,
                    old_price,
                    new_price,
                )

                stats["updated"] += 1

            else:
                stats["failed"] += 1

        time.sleep(REQUEST_DELAY)

    logging.info("-" * 60)
    logging.info("FINISHED")
    logging.info("Updated   : %d", stats["updated"])
    logging.info("Not Found : %d", stats["not_found"])
    logging.info("Failed    : %d", stats["failed"])


if __name__ == "__main__":
    main()
