import requests
from supabase import create_client, Client
import os

# --------------------------
# SUPABASE
# --------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
API_KEY = os.environ["CS2_API_KEY"]

supabase: Client = create_client(
    SUPABASE_URL,
    SUPABASE_KEY
)

# --------------------------
# SESSION (FASTER)
# --------------------------
session = requests.Session()

session.headers.update({
    "Authorization": f"Bearer {API_KEY}",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json",
})

# --------------------------
# EUR RATE (ONLY ONCE)
# --------------------------
try:
    r = session.get(
        "https://api.exchangerate.host/latest?base=USD&symbols=EUR",
        timeout=10
    )

    EUR_RATE = r.json()["rates"]["EUR"]

except Exception:
    EUR_RATE = 0.92

print(f"USD → EUR: {EUR_RATE}")

# --------------------------
# LOAD SKINS
# --------------------------
try:
    skins = supabase.table("skins") \
        .select("market_hash_name") \
        .execute() \
        .data

except Exception as e:
    print("Supabase fetch error:", e)
    skins = []

print(f"Načítaných skinov: {len(skins)}")

if not skins:
    print("❌ No skins")
    exit()

# --------------------------
# GET ALL SKIN NAMES
# --------------------------
skin_names = []

for skin in skins:
    name = skin.get("market_hash_name")

    if name:
        skin_names.append(name)

# --------------------------
# FETCH ALL PRICES AT ONCE
# --------------------------
try:
    r = session.post(
        "https://api.cs2.sh/v1/prices/latest",
        json={"items": skin_names},
        timeout=30
    )

    data = r.json()
    items = data.get("items", {})

except Exception as e:
    print("API error:", e)
    exit()

# --------------------------
# UPDATE PRICES
# --------------------------
updated = 0

for skin in skins:

    market_hash_name = skin["market_hash_name"]

    item = None

    # exact match
    if market_hash_name in items:
        item = items[market_hash_name]

    else:
        # fallback fix (AXIA etc.)
        base_name = market_hash_name.split(" (")[0]

        for k, v in items.items():
            if base_name.lower() in k.lower():
                item = v
                break

    if not item:
        print(f"❌ NOT FOUND: {market_hash_name}")
        continue

    # price priority
    price_usd = (
        item.get("buff", {}).get("ask")
        or item.get("csfloat", {}).get("ask")
        or item.get("steam", {}).get("ask")
    )

    if not price_usd:
        print(f"❌ No price: {market_hash_name}")
        continue

    # USD -> EUR
    price_eur = round(price_usd * EUR_RATE, 2)

    try:
        supabase.table("skins") \
            .update({
                "price": price_eur
            }) \
            .eq(
                "market_hash_name",
                market_hash_name
            ) \
            .execute()

        print(f"{market_hash_name} -> {price_eur} €")
        updated += 1

    except Exception as e:
        print(f"Update error {market_hash_name}: {e}")

print(f"\n✅ UPDATED: {updated} skins")
