import os
import sys
import json
import time
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'transactions'

print("[*] Uruchamianie Komponentu: Testowy Konsument")

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print(f"[+] Polaczono z Kafka. Nasluchiwanie na topiku: {KAFKA_TOPIC}")
except Exception as e:
    print(f"[-] Blad polaczenia z brokerem Kafka: {e}")
    sys.exit(1)

metrics = {
    "total_messages": 0,
    "valid_messages": 0,
    "corrupted_messages": 0,
    "total_amount": 0.0,
    "start_time": time.time()
}

last_tx_preview = None
last_ui_update = time.time()

def draw_statistics_dashboard():
    os.system('cls' if os.name == 'nt' else 'clear')
    
    elapsed_time = time.time() - metrics["start_time"]
    tps = metrics["total_messages"] / elapsed_time if elapsed_time > 0 else 0
    avg_amount = metrics["total_amount"] / metrics["valid_messages"] if metrics["valid_messages"] > 0 else 0

    print("===========================================================================")
    print("                TESTOWY KONSUMENT KAFKA I WALIDATOR STRUMIENIA             ")
    print("===========================================================================")
    print(f" Status: AKTYWNY | Zrodlo: {KAFKA_TOPIC} | Serwer: {KAFKA_BOOTSTRAP_SERVERS}")
    print("---------------------------------------------------------------------------")
    print(f" Przetworzonych transakcji ogolem:         {metrics['total_messages']}")
    print(f" Poprawne struktury JSON (Valid):         {metrics['valid_messages']}")
    print(f" Uszkodzone/Bledne komunikaty (Invalid):   {metrics['corrupted_messages']}")
    print("---------------------------------------------------------------------------")
    print(f" Biezaca przepustowosc (TPS):              {round(tps, 2)} transakcji/sek")
    print(f" Srednia wartosc transakcji:              {round(avg_amount, 2)} PLN")
    print("===========================================================================")
    
    if last_tx_preview:
        print("\n[Wizualizacja struktury ostatniego poprawnego zdarzenia]:")
        print(json.dumps(last_tx_preview, indent=2))

try:
    for message in consumer:
        tx = message.value
        metrics["total_messages"] += 1
        
        required_fields = ["card_id", "user_id", "gps", "amount", "card_limit", "timestamp"]
        is_structure_valid = all(field in tx for field in required_fields)
        
        if is_structure_valid and isinstance(tx["gps"], list) and len(tx["gps"]) == 2:
            metrics["valid_messages"] += 1
            metrics["total_amount"] += tx["amount"]
            last_tx_preview = tx
        else:
            metrics["corrupted_messages"] += 1

        if time.time() - last_ui_update > 0.3:
            draw_statistics_dashboard()
            last_ui_update = time.time()

except KeyboardInterrupt:
    print("\n[-] Zatrzymano testowego konsumenta.")