import requests
from supabase import create_client, Client
import urllib.parse
import time
import re
import os

# --------------------------
# Supabase nastavenie (SAFE verzia)
# --------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --------------------------
# Funkcia na bezpečnú konverziu ceny
# --------------------------
def parse_price(price_str):
    if not price_str:
        return None

    price_str = price_str.replace(",", ".").replace(" ", "")
    match = re.search(r"\d+(\.\d+)?", price_str)

    if match:
        return float(match.group())

    return None

# --------------------------
# Steam price fetch
# --------------------------
def get_steam_price(market_hash_name):
    name_enc = urllib.parse.quote(market_hash_name)
    url = f"https://steamcommunity.com/market/priceoverview/?currency=3&appid=730&market_hash_name={name_enc}"

    try:
        resp = requests.get(url, timeout=10).json()

        if resp.get("success"):
            price_str = resp.get("lowest_price") or resp.get("median_price")
            return parse_price(price_str)

    except Exception as e:
        print(f"Chyba pri {market_hash_name}: {e}")

    return None

# --------------------------
# Načítanie skinov zo Supabase
# --------------------------
try:
    skins = supabase.table("skins").select("market_hash_name").execute().data
except Exception as e:
    print(f"Chyba Supabase fetch: {e}")
    skins = []

# --------------------------
# Update cien
# --------------------------
for skin in skins:
    market_hash_name = skin["market_hash_name"]

    price = get_steam_price(market_hash_name)

    if price is not None:
        try:
            supabase.table("skins") \
                .update({"price": price}) \
                .eq("market_hash_name", market_hash_name) \
                .execute()

            print(f"{market_hash_name} -> {price} €")

        except Exception as e:
            print(f"Update error {market_hash_name}: {e}")
    else:
        print(f"Nezískaná cena: {market_hash_name}")

    time.sleep(1)
