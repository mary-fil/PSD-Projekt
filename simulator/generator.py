import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker('pl_PL')

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'transactions'
TOTAL_CARDS = 10000
TOTAL_USERS = 7000 

print("Inicjalizacja bazy 10 000 kart przy użyciu Faker...")

users = [f"user_{i}" for i in range(1, TOTAL_USERS + 1)]
cards_pool = []

for i in range(1, TOTAL_CARDS + 1):
    assigned_user = random.choice(users)
    home_lat = random.uniform(49.0, 54.8)
    home_lon = random.uniform(14.1, 24.1)

    cards_pool.append({
        "card_id": f"card_{i}",
        "user_id": assigned_user,
        "home_gps": (home_lat, home_lon),
        "limit": round(random.uniform(1000, 15000), 2)
    })

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Połączono z Kafką pomyślnie!")
except Exception as e:
    print(f"Błąd połączenia z Kafką: {e}")
    exit(1)


def generate_transaction(anomaly_type="NORMAL"):
    card = random.choice(cards_pool)
    lat, lon = card["home_gps"]
    
    lat += random.uniform(-0.02, 0.02)
    lon += random.uniform(-0.02, 0.02)
    amount = round(random.uniform(10.0, 300.0), 2)
    
    if anomaly_type == "AMOUNT":
        amount = round(random.uniform(5000.0, 12000.0), 2)
        print(f"[ANOMALIA - KWOTA] Karta {card['card_id']} -> {amount} PLN")
        
    elif anomaly_type == "LOCATION":
        lat = float(fake.latitude())
        lon = float(fake.longitude())
        print(f"[ANOMALIA - LOKALIZACJA] Karta {card['card_id']} nagle pojawiła się w: [{lat}, {lon}]")

    payload = {
        "card_id": card["card_id"],
        "user_id": card["user_id"],
        "gps": [round(lat, 6), round(lon, 6)],
        "amount": amount,
        "card_limit": card["limit"],
        "timestamp": datetime.utcnow().isoformat()
    }
    return payload, card["card_id"]


print("Uruchamianie generowania transakcji...")
try:
    while True:
        rand_val = random.random()
        
        if rand_val < 0.01:
            data, _ = generate_transaction(anomaly_type="AMOUNT")
            producer.send(KAFKA_TOPIC, value=data)
        elif rand_val < 0.02:
            data, _ = generate_transaction(anomaly_type="LOCATION")
            producer.send(KAFKA_TOPIC, value=data)
        elif rand_val < 0.03:
            data, card_id = generate_transaction()
            print(f"[ANOMALIA - CZĘSTOTLIWOŚĆ] Seria transakcji dla karty {card_id}")
            for _ in range(4):
                payload = {
                    "card_id": card_id,
                    "user_id": data["user_id"],
                    "gps": data["gps"],
                    "amount": round(random.uniform(5.0, 40.0), 2),
                    "card_limit": data["card_limit"],
                    "timestamp": datetime.utcnow().isoformat()
                }
                producer.send(KAFKA_TOPIC, value=payload)
                time.sleep(0.1)
        else:
            data, _ = generate_transaction()
            producer.send(KAFKA_TOPIC, value=data)
            
        time.sleep(0.4)

except KeyboardInterrupt:
    print("\nZatrzymano symulator.")
finally:
    producer.flush()
    producer.close()