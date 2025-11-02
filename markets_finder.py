import requests

endpoint = "https://clob.polymarket.com/markets"
next_cursor = ""
found = []

print("Starting search.")
while True:
    try:
        resp = requests.get(endpoint, params={"next_cursor": next_cursor}).json()
        for m in resp["data"]:
            if "INSERT_GAME_URL_ENDING" for example "sea-cre-juv-2025-11-01" in m.get("market_slug"):
                print( m.get("market_slug"))
            #print(m)
            
            if "INSERT_GAME_URL_ENDING" for example "sea-cre-juv-2025-11-01" in (m.get("market_slug") or "").lower():  # adjust criteria
                found.append(m)
        next_cursor = resp.get("next_cursor")

    except Exception as e:
        print(f"Error occurred: {e}")
        break  # <---- this is where to put the break

print(found)

