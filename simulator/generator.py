import os
import json
import time
import random
from datetime import datetime, timedelta
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
        time.sleep(2)
        
    print(f"[*] Tworzenie świeżego, czystego topiku '{KAFKA_TOPIC}'...")
    topic_list = [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    admin_client.close()
except Exception as e:
    print(f"[-] Ostrzeżenie podczas zarządzania topikami: {e}")

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("[+] Połączono z Kafką pomyślnie!")
except Exception as e:
    print(f"[-] Błąd połączenia z Kafką: {e}")
    exit(1)

# Słownik, w którym zapiszemy REALNE profile kart wyciągnięte z historii
cards_live_profiles = {}
# Zmienna do wyznaczenia punktu ciągłości osi czasu
latest_historical_timestamp = None

if os.path.exists(HISTORICAL_FILE):
    print(f"[*] Znaleziono plik historyczny '{HISTORICAL_FILE}'. Mapowanie profili i wstrzykiwanie...")
    with open(HISTORICAL_FILE, "r") as f:
        hist_data = json.load(f)
    
    total_to_send = len(hist_data)
    print(f"[*] Wstrzykiwanie {total_to_send} rekordów uczących do topiku '{KAFKA_TOPIC}'...")
    
    start_bulk = time.time()
    for idx, tx in enumerate(hist_data):
        producer.send(KAFKA_TOPIC, value=tx)
        
        # Ekstrakcja danych profilowych karty na podstawie historii
        cid = tx["card_id"]
        tx_time = datetime.fromisoformat(tx["timestamp"])
        
        if cid not in cards_live_profiles:
            cards_live_profiles[cid] = {
                "card_id": cid,
                "user_id": tx["user_id"],
                "home_gps": tx["gps"],
                "limit": tx["card_limit"],
                "sum_amount": tx["amount"],
                "tx_count": 1,
                "last_tx_time": tx_time
            }
        else:
            profile = cards_live_profiles[cid]
            profile["sum_amount"] += tx["amount"]
            profile["tx_count"] += 1
            if tx_time > profile["last_tx_time"]:
                profile["last_tx_time"] = tx_time
                profile["home_gps"] = tx["gps"] # aktualna ostatnia lokalizacja

        # Śledzimy, kiedy dokładnie skończyła się historia globalnie
        if latest_historical_timestamp is None or tx_time > latest_historical_timestamp:
            latest_historical_timestamp = tx_time

        if idx % 20000 == 0 and idx > 0:
            producer.flush()
            print(f" -> Wysłano {idx}/{total_to_send}...")
            
    producer.flush()
    print(f"[+] Faza historyczna zakończona w {round(time.time() - start_bulk, 2)} sek.")
    
    print("[*] Aktywacja pauzy buforowej (5 sekund) dla silnika Flink...")
    time.sleep(5)
else:
    print(f"[-] BŁĄD CRITICAL: Brak pliku '{HISTORICAL_FILE}'. Wygeneruj go najpierw!")
    exit(1)


# Przekształcamy profil w listę do szybkiego losowania
cards_pool = list(cards_live_profiles.values())
# Zegar systemowy strumienia na żywo rusza DOKŁADNIE tam, gdzie skończyła się historia
current_simulation_time = latest_historical_timestamp

print(f"[+] Czas startowy symulacji na żywo: {current_simulation_time.isoformat()}")

def generate_live_transaction(anomaly_type="NORMAL"):
    global current_simulation_time
    
    card = random.choice(cards_pool)
    lat, lon = card["home_gps"]
    
    # Wyliczamy prawdziwą średnią historyczną tej konkretnej karty
    historical_mean = card["sum_amount"] / card["tx_count"]
    
    # Ruch na żywo posuwa zegar globalny o losowe ułamki sekund do przodu
    current_simulation_time += timedelta(seconds=random.randint(1, 3))
    
    # Domyślne wartości (normalne)
    amount = round(random.uniform(historical_mean * 0.7, historical_mean * 1.3), 2)
    lat += random.uniform(-0.005, 0.005)
    lon += random.uniform(-0.005, 0.005)
    
    if anomaly_type == "AMOUNT":
        # Mnożymy PRAWDZIWĄ średnią historyczną x6, co gwarantuje przebicie progu x4 we Flinku
        amount = round(historical_mean * random.uniform(6.0, 10.0), 2)
        if amount < 550: amount += 600 # Upewniamy się, że przekracza też próg twardy 500 PLN
        print(f"🚨 [PRODUCENT] Karta {card['card_id']} (Śr: {round(historical_mean, 2)}) -> Wstrzyknięto Skok Kwoty: {amount} PLN")
        
    elif anomaly_type == "LOCATION":
        # Ponieważ symulacja czasu leci natychmiast po ostatniej transakcji historycznej danej karty,
        # odległość kilku tysięcy kilometrów wykonana w kilka sekund wygeneruje prędkość rzędu milionów km/h!
        lat = float(fake.latitude())
        lon = float(fake.longitude())
        print(f"🚨 [PRODUCENT] Karta {card['card_id']} -> Wstrzyknięto Nagłą Lokalizację: [{lat}, {lon}]")

    payload = {
        "card_id": card["card_id"],
        "user_id": card["user_id"],
        "gps": [round(lat, 6), round(lon, 6)],
        "amount": amount,
        "card_limit": card["limit"],
        "timestamp": current_simulation_time.isoformat()
    }
    
    # Jeśli transakcja była normalna, aktualizujemy wiedzę generatora, żeby dopasowywał się w locie
    if anomaly_type == "NORMAL":
        card["sum_amount"] += amount
        card["tx_count"] += 1
        card["home_gps"] = [lat, lon]
        
    return payload, card["card_id"]


print("[*] Uruchamianie fazy produkcyjnej: Strumień na żywo z precyzyjnymi anomaliami...")
try:
    while True:
        rand_val = random.random()
        
        if rand_val < 0.01:
            data, _ = generate_live_transaction(anomaly_type="AMOUNT")
            producer.send(KAFKA_TOPIC, value=data)
        elif rand_val < 0.02:
            data, _ = generate_live_transaction(anomaly_type="LOCATION")
            producer.send(KAFKA_TOPIC, value=data)
        elif rand_val < 0.03:
            # Częstotliwość generujemy wysyłając 4 paczki w ułamku sekundy symulacji
            data, card_id = generate_live_transaction(anomaly_type="NORMAL")
            print(f"🚨 [PRODUCENT] Wstrzykiwanie serii błyskawicznej dla karty {card_id}")
            producer.send(KAFKA_TOPIC, value=data)
            
            for _ in range(3):
                # Generujemy transakcje o tym samym czasie lub przesunięte o ułamek sekundy
                payload, _ = generate_live_transaction(anomaly_type="NORMAL")
                payload["card_id"] = card_id
                producer.send(KAFKA_TOPIC, value=payload)
        else:
            data, _ = generate_live_transaction(anomaly_type="NORMAL")
            producer.send(KAFKA_TOPIC, value=data)
            
        time.sleep(0.4)

except KeyboardInterrupt:
    print("\n[-] Zatrzymano symulator.")
finally:
    producer.flush()
    producer.close()