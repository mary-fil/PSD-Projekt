import os
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from faker import Faker

fake = Faker('pl_PL')

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'transactions'
HISTORICAL_FILE = "historical_data.json"

try:
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id='gen_admin')
    existing_topics = admin_client.list_topics()
    
    if KAFKA_TOPIC in existing_topics:
        print(f"[*] Topik '{KAFKA_TOPIC}' już istnieje. Usuwanie w celu uniknięcia duplikacji historii...")
        admin_client.delete_topics(topics=[KAFKA_TOPIC])
        time.sleep(2)  # Krótka pauza na synchronizację klastra Kafki
        
    print(f"[*] Tworzenie świeżego, czystego topiku '{KAFKA_TOPIC}'...")
    topic_list = [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    admin_client.close()
except Exception as e:
    print(f"[-] Ostrzeżenie podczas zarządzania topikami: {e}")

# Inicjalizacja właściwego producenta danych
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("[+] Połączono z Kafką pomyślnie!")
except Exception as e:
    print(f"[-] Błąd połączenia z Kafką: {e}")
    exit(1)

if os.path.exists(HISTORICAL_FILE):
    print(f"[*] Znaleziono plik historyczny '{HISTORICAL_FILE}'. Rozpoczynanie wstrzykiwania bazy zachowań...")
    with open(HISTORICAL_FILE, "r") as f:
        hist_data = json.load(f)
    
    total_to_send = len(hist_data)
    print(f"[*] Wstrzykiwanie {total_to_send} rekordów uczących do topiku '{KAFKA_TOPIC}'...")
    
    start_bulk = time.time()
    for idx, tx in enumerate(hist_data):
        producer.send(KAFKA_TOPIC, value=tx)
        if idx % 20000 == 0 and idx > 0:
            producer.flush()
            print(f" -> Wysłano {idx}/{total_to_send}...")
            
    producer.flush()
    print(f"[+] Faza historyczna zakończona w {round(time.time() - start_bulk, 2)} sek. Flink ma pełny profil zachowań.")
    print("[*] Aktywacja pauzy buforowej (5 sekund)... Pozwalam Flinkowi wczytać historię do pamięci RAM.")
    time.sleep(5)
    print("[+] Pauza zakończona. Profile użytkowników są gotowe we Flinku.")
    
else:
    print(f"[-] Brak pliku '{HISTORICAL_FILE}'. Uruchom najpierw generate_historical.py!")

# Pula kart potrzebna do generowania anomalii w locie na żywo
print("[*] Inicjalizacja bazy kart na potrzeby strumienia na żywo...")
TOTAL_CARDS = 10000
TOTAL_USERS = 7000
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
        "limit": round(random.uniform(1000, 15000), 2),
        "last_normal_amount": round(random.uniform(20.0, 150.0), 2)
    })

def generate_transaction(anomaly_type="NORMAL"):
    card = random.choice(cards_pool)
    lat, lon = card["home_gps"]
    
    lat += random.uniform(-0.01, 0.01)
    lon += random.uniform(-0.01, 0.01)
    amount = card["last_normal_amount"]
    
    if anomaly_type == "AMOUNT":
        amount = round(amount * random.uniform(10.0, 25.0), 2)
        if amount < 600: amount += 1000
        print(f"🚨 [PRODUCENT - ANOMALIA KWOTY] Karta {card['card_id']} -> Wstrzyknięto nagły skok: {amount} PLN")
        
    elif anomaly_type == "LOCATION":
        lat = float(fake.latitude())
        lon = float(fake.longitude())
        print(f"🚨 [PRODUCENT - ANOMALIA LOKALIZACJI] Karta {card['card_id']} -> Nagłe przesunięcie do: [{lat}, {lon}]")

    payload = {
        "card_id": card["card_id"],
        "user_id": card["user_id"],
        "gps": [round(lat, 6), round(lon, 6)],
        "amount": amount,
        "card_limit": card["limit"],
        "timestamp": datetime.utcnow().isoformat()
    }
    return payload, card["card_id"]

print("[*] Uruchamianie fazy produkcyjnej: Strumień na żywo z anomaliami...")
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
            print(f"🚨 [PRODUCENT - ANOMALIA CZĘSTOTLIWOŚCI] Wstrzykiwanie serii dla karty {card_id}")
            for _ in range(4):
                payload = {
                    "card_id": card_id,
                    "user_id": data["user_id"],
                    "gps": data["gps"],
                    "amount": round(random.uniform(5.0, 30.0), 2),
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
    print("\n[-] Zatrzymano symulator.")
finally:
    producer.flush()
    producer.close()