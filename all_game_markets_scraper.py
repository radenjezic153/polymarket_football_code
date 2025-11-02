import asyncio
import json
import time
import websockets
import os
import csv
import re
from datetime import datetime, timezone
from pathlib import Path

CLOB_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Persist only top N levels (None = full book)

TOP_N = None

# Directory where CSVs will be stored

DATA_DIR = Path("data/orderbooks")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# All markets & their tokens

TOKENS = {
"epl-tot-che-2025-11-01-tot": {
"Yes": "11020630288858196454442019628788722756448439383339324474431323043850376775556",
"No": "92506370473682962551026634276018078107080528692450216602357020848620695793558"
},
"epl-tot-che-2025-11-01-draw": {
"Yes": "35142638605244559944796574627259648429088640204093180662488137544944969802261",
"No": "46608967978596512830815731967270187718839844459521315845887046299629810369894"
},
"epl-tot-che-2025-11-01-che": {
"Yes": "71303597829162568661397974813628126423734571515484489377713757261206741893093",
"No": "84466533242560516985410217154519049332782613882122226593367293317293331720658"
},
"epl-tot-che-2025-11-01-total-2pt5": {
"Over": "99426790324376527155129462317787633865820490133645919706439205378117442957555",
"Under": "1310705080981277872969132264116031147891579665125834281205258945476593552767"
},
"epl-tot-che-2025-11-01-spread-away-0pt5": {
"Chelsea": "92827594558092568151032635528379463770205504988098237266855738008721040408792",
"Tottenham": "26841339186693956137513272690691397195498236733445410902599680263772060957350"
},
"epl-tot-che-2025-11-01-btts": {
"Yes": "39750594147065867653921602789051427960737012499461145175400083250141823489361",
"No": "97326792212377569528757619983849165992719161732234197701693283231064177607686"
}
}

# Flatten tokens for WebSocket subscription

TOKEN_LABELS = {}
for market_name, sides in TOKENS.items():
    for side_name, token_id in sides.items():
        TOKEN_LABELS[token_id] = (market_name, side_name)

ASSET_IDS = [tid for market in TOKENS.values() for tid in market.values()]

# Single CSV for all markets

CSV_FILE = DATA_DIR / "all_orderbooks.csv"

def sanitize_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(s)) if s else "unknown"

TOKEN_LABELS = {}
for market_name, sides in TOKENS.items():
    for side_name, token_id in sides.items():
        TOKEN_LABELS[token_id] = (market_name, side_name)

        
def save_order_book(token_id, market_id, market_name, side_name, bids, asks, best_bid, best_ask):
    """
    Save one row to CSV including token, market, side, label, and human-readable name.
    """
    now_utc = datetime.now(timezone.utc).isoformat()
    now_local = datetime.now().astimezone().isoformat()
    label = f"{market_name}-{side_name}"
    human_readable = f"Market {market_name} ({side_name})"

    row = {
        "ts_utc": now_utc,
        "ts_local": now_local,
        "market_id": market_id,
        "token_id": token_id,
        "market_name": market_name,
        "side": side_name,
        "label": label,
        "human_readable": human_readable,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bids_json": json.dumps(bids, separators=(",", ":"), ensure_ascii=False),
        "asks_json": json.dumps(asks, separators=(",", ":"), ensure_ascii=False)
    }

    write_header = not CSV_FILE.exists()
    try:
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(row.keys()))
            if write_header:
                writer.writeheader()
                print(f"Created CSV file: {CSV_FILE.resolve()}")
            writer.writerow(row)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
    except Exception as e:
        print(f"Error writing to CSV: {e}")



def extract_payload(obj):
    """
    Recursively extract payload dict from dict or list objects.
    Returns a dict with 'bids'/'asks'/'market' if found, else {}.
    """
    if isinstance(obj, dict):
        # If the dict itself has bids/asks/market, return it
        if "bids" in obj or "asks" in obj or "market" in obj:
            return obj
        # Otherwise try to get 'payload' key
        payload = obj.get("payload")
        if payload:
            return extract_payload(payload)
        return obj  # fallback

    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                p = extract_payload(item)
                if p and ("bids" in p or "asks" in p or "market" in p):
                    return p

    # fallback: nothing found
    return {}

def get_market_id(payload):
    """
    Safely extract market id from dict or list payload.
    """
    if isinstance(payload, list):
        for item in payload:
            mid = get_market_id(item)
            if mid:
                return mid
        return None

    if not isinstance(payload, dict):
        return None

    for key in ("market", "asset_id", "asset_ids", "assets_ids", "assetId", "market_id"):
        val = payload.get(key)
        if val:
            return val

    # fallback: payload keyed by market id
    for k, v in payload.items():
        if isinstance(v, dict) and ("bids" in v or "asks" in v):
            return k

    return None


async def process_payload(payload, order_books):
    """
    Process a single token update from Polymarket CLOB feed.
    """
    if not isinstance(payload, dict):
        return

    token_id = payload.get("asset_id") or payload.get("id")
    if not token_id:
        return

    market_id = payload.get("market") or "unknown_market"
    bids = payload.get("bids") or []
    asks = payload.get("asks") or []

    if not (bids or asks):
        return

    # Compute best bid / ask
    try:
        best_bid = max(bids, key=lambda x: float(x.get("price", 0)))["price"] if bids else None
        best_ask = min(asks, key=lambda x: float(x.get("price", 1e9)))["price"] if asks else None
    except Exception as e:
        print("Price parse error:", e)
        best_bid = None
        best_ask = None

    # Map token ID → market name + side
    market_name, side_name = TOKEN_LABELS.get(token_id, ("unknown_market", "unknown"))

    # Save row to CSV
    save_order_book(token_id, market_id, market_name, side_name, bids, asks, best_bid, best_ask)

    # Update in-memory order book
    order_books[token_id] = {"bids": bids, "asks": asks}

    # Console log
    print(f"{time.strftime('%H:%M:%S')} {market_name} ({side_name}) | token_id={token_id} | market_id={market_id} | best_bid={best_bid} | best_ask={best_ask}")


async def run_clob_market(asset_ids):
    """
    Main websocket loop: subscribe to all asset_ids, handle messages,
    and reconnect automatically if disconnected.
    """
    order_books = {}

    while True:
        try:
            async with websockets.connect(CLOB_MARKET_WS) as ws:
                await ws.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))
                print("Connected, waiting for messages...")
                last_ping = time.time()

                while True:
                    # Send ping every second to keep connection alive
                    if time.time() - last_ping > 0.2:
                        await ws.send("PING")
                        last_ping = time.time()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=8)
                    except asyncio.TimeoutError:
                        continue

                    if isinstance(raw, str) and raw.strip().upper() == "PONG":
                        continue

                    try:
                        parsed = json.loads(raw)
                    except Exception:
                        continue

                    # Extract payload and process
                    payload = extract_payload(parsed)
                    if isinstance(payload, list):
                        for p in payload:
                            if isinstance(p, dict):
                                p_extracted = extract_payload(p)
                                if p_extracted:
                                    await process_payload(p_extracted, order_books)
                    elif payload:
                        await process_payload(payload, order_books)

        except websockets.ConnectionClosed as cc:
            print("Connection closed, reconnecting in 3s:", cc)
            await asyncio.sleep(3)
        except Exception as e:
            print("Error, reconnecting in 3s:", e)
            await asyncio.sleep(3)


if __name__ == "__main__":
    print("All orderbook updates will be saved in:", CSV_FILE.resolve())
    asyncio.run(run_clob_market(ASSET_IDS))


