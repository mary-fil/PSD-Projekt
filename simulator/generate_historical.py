import json
import random
from datetime import datetime, timedelta

TOTAL_CARDS = 10000
TOTAL_USERS = 7000
TOTAL_RECORDS = 80000

print(f"[*] Rozpoczynanie generowania {TOTAL_RECORDS} transakcji bazy historycznej...")

users = [f"user_{i}" for i in range(1, TOTAL_USERS + 1)]
cards_pool = []

for i in range(1, TOTAL_CARDS + 1):
    assigned_user = random.choice(users)
    home_lat = random.uniform(49.0, 54.8)
    home_lon = random.uniform(14.1, 24.1)
    
    profile_type = random.choice(["SMALL", "MEDIUM", "LARGE"])
    if profile_type == "SMALL":
        base_amount_min, base_amount_max = 10.0, 80.0
    elif profile_type == "MEDIUM":
        base_amount_min, base_amount_max = 80.0, 250.0
    else:
        base_amount_min, base_amount_max = 250.0, 600.0

    cards_pool.append({
        "card_id": f"card_{i}",
        "user_id": assigned_user,
        "home_gps": (home_lat, home_lon),
        "limit": round(random.uniform(1000, 15000), 2),
        "amount_range": (base_amount_min, base_amount_max)
    })

historical_transactions = []
base_start_time = datetime.utcnow() - timedelta(days=15)
card_timers = {card["card_id"]: base_start_time for card in cards_pool}

print("[*] Budowanie stabilnych ciągów transakcji (ochrona przed FREQUENCY_ANOMALY)...")
for i in range(TOTAL_RECORDS):
    card = random.choice(cards_pool)
    card_id = card["card_id"]
    lat, lon = card["home_gps"]
    
    lat += random.uniform(-0.01, 0.01)
    lon += random.uniform(-0.01, 0.01)
    
    min_amt, max_amt = card["amount_range"]
    amount = round(random.uniform(min_amt, max_amt), 2)
    
    card_timers[card_id] += timedelta(minutes=random.randint(30, 180))
    tx_time = card_timers[card_id]

    payload = {
        "card_id": card_id,
        "user_id": card["user_id"],
        "gps": [round(lat, 6), round(lon, 6)],
        "amount": amount,
        "card_limit": card["limit"],
        "timestamp": tx_time.isoformat()
    }
    historical_transactions.append(payload)

print("[*] Sortowanie chronologiczne zbioru danych...")
historical_transactions.sort(key=lambda x: x["timestamp"])

output_file = "historical_data.json"
with open(output_file, "w") as f:
    json.dump(historical_transactions, f, indent=2)

print(f"[*] Wygenerowano plik {output_file}. Brak jakichkolwiek anomalii częstotliwości.")