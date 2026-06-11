import os
# Flagi otwierające wewnętrzne pakiety Javy dla PyFlanka
java_opts = (
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
)
os.environ["JVM_ARGS"] = java_opts
os.environ["FLINK_ENV_JAVA_OPTS"] = java_opts

import json
import math
import time
import urllib.request
import pathlib

from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from pyflink.common import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import MapFunction

JAR_NAME = "flink-sql-connector-kafka-1.17.1.jar"
JAR_URL = f"https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/{JAR_NAME}"

if not os.path.exists(JAR_NAME):
    try:
        urllib.request.urlretrieve(JAR_URL, JAR_NAME)
    except Exception:
        pass

def haversine_distance(coord1, coord2):
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

class FraudDetectorMap(MapFunction):
    def __init__(self):
        # Słownik stanu przechowuje: 
        # { card_id: { "last_tx": tx_dict, "welford_mean": float, "tx_count": int, "last_freq_alert": datetime/None } }
        self.cards_cache = {}
        self.producer = None

    def open(self, runtime_context):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def map(self, value_str):
        tx = json.loads(value_str)
        card_id = tx["card_id"]
        current_gps = tx["gps"]
        current_amount = tx["amount"]
        current_time = datetime.fromisoformat(tx["timestamp"])
        
        alert_triggered = None

        # Jeśli karta pojawiła się już w systemie, wyciągamy jej profil analityczny
        if card_id in self.cards_cache:
            card_profile = self.cards_cache[card_id]
            last_tx = card_profile["last_tx"]
            running_mean = card_profile["welford_mean"]
            last_freq_alert = card_profile.get("last_freq_alert", None)
            
            prev_gps = last_tx["gps"]
            prev_time = datetime.fromisoformat(last_tx["timestamp"])

            time_delta = (current_time - prev_time).total_seconds()
            time_delta_hours = time_delta / 3600.0

            # 1. DETEKCJA ANOMALII LOKALIZACJI (Wzór Haversine)
            distance = haversine_distance(prev_gps, current_gps)
            if time_delta_hours > 0:
                speed = distance / time_delta_hours
                if distance > 50 and speed > 900:
                    alert_triggered = {
                        "alert_type": "LOCATION_ANOMALY",
                        "card_id": card_id,
                        "details": f"Impossible Travel: {round(speed, 2)} km/h na dystansie {round(distance, 2)} km.",
                        "timestamp": tx["timestamp"],
                        "amount": current_amount,
                        "execution_engine": "Apache Flink (Stateful)"
                    }

            # 2. DETEKCJA KWOTY (Bazująca na przyrostowej średniej kroczącej użytkownika)
            if current_amount > 500 and current_amount > (running_mean * 4):
                alert_triggered = {
                    "alert_type": "AMOUNT_ANOMALY",
                    "card_id": card_id,
                    "details": f"Gwałtowny skok kwoty. Średnia historyczna: {round(running_mean, 2)} PLN, obecna transakcja: {current_amount} PLN.",
                    "timestamp": tx["timestamp"],
                    "amount": current_amount,
                    "execution_engine": "Apache Flink (Stateful)"
                }

            # 3. DETEKCJA ANOMALII CZĘSTOTLIWOŚCI (Z inteligentnym filtrem spamu seryjnego)
            if 0 < time_delta < 5:
                # Sprawdzamy, czy w ciągu ostatnich 10 sekund wysłaliśmy już alert dla tej serii
                if last_freq_alert and (current_time - last_freq_alert).total_seconds() < 10:
                    # To kolejna fala z tej samej trefnej serii - wyciszamy logi i narzut sieciowy
                    alert_triggered = {"alert_type": "FREQUENCY_ANOMALY_SILENT"}
                else:
                    # Pierwszy wykryty strzał z serii - generujemy pełny raport
                    alert_triggered = {
                        "alert_type": "FREQUENCY_ANOMALY",
                        "card_id": card_id,
                        "details": f"Podejrzana seria transakcji. Odstęp czasu: {round(time_delta, 2)} sek.",
                        "timestamp": tx["timestamp"],
                        "amount": current_amount,
                        "execution_engine": "Apache Flink (Stateful)"
                    }
                    # Zapamiętujemy moment rzucenia alertu głównego w profilu karty
                    card_profile["last_freq_alert"] = current_time

        # ======================================================================
        # 🛡️ SYSTEM OCHRONY STANU PRZED ZATRUWANIEM (State Poisoning Protection)
        # ======================================================================
        if alert_triggered is None:
            # Transakcja jest prawidłowa -> bezpiecznie aktualizujemy bazowy profil stanowy karty
            if card_id not in self.cards_cache:
                # Pierwsza transakcja w historii karty
                self.cards_cache[card_id] = {
                    "last_tx": tx,
                    "welford_mean": current_amount,
                    "tx_count": 1,
                    "last_freq_alert": None
                }
            else:
                # Kolejna prawidłowa transakcja -> przeliczamy średnią matematycznie w locie
                profile = self.cards_cache[card_id]
                profile["tx_count"] += 1
                profile["welford_mean"] += (current_amount - profile["welford_mean"]) / profile["tx_count"]
                profile["last_tx"] = tx
        else:
            # 🚨 Wykryto oszustwo! Całkowicie ODRZUCAMY tę transakcję z kesza.
            # Dzięki temu kolejne ataki oszusta nadal mierzymy miarą legalnych zachowań klienta!
            pass

        # Realna wysyłka alertu na topik Kafki (tylko dla unikalnych operacji, pomijając duplikaty)
        if alert_triggered and alert_triggered["alert_type"] != "FREQUENCY_ANOMALY_SILENT" and self.producer:
            print(f"⚠️ [FLINK REAL-TIME DETECTED]: {alert_triggered['alert_type']} na karcie {card_id}")
            self.producer.send('alerts', value=alert_triggered)
            self.producer.flush()
            return json.dumps(alert_triggered)
        
        return None

def run_flink_job():
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
    ALERTS_TOPIC = 'alerts'

    # ==============================================================================
    # ⚙️ DYNAMICZNA KONFIGURACJA STRUMIENIA (Złoty as na obronie)
    # ==============================================================================
    # True  -> Tryb Earliest: Flink czyta od początku, ładuje plik JSON i buduje profile kart
    # False -> Tryb Latest: Flink ignoruje historię i nasłuchuje czystego ruchu na żywo
    READ_FROM_EARLIEST = True

    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id='det_admin')
        existing_topics = admin_client.list_topics()
        
        if ALERTS_TOPIC in existing_topics:
            print(f"[*] Topik alertów '{ALERTS_TOPIC}' już istnieje. Czyszczenie przed startem...")
            admin_client.delete_topics(topics=[ALERTS_TOPIC])
            time.sleep(2)
            
        print(f"[*] Tworzenie świeżego topiku '{ALERTS_TOPIC}'...")
        topic_list = [NewTopic(name=ALERTS_TOPIC, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        admin_client.close()
    except Exception as e:
        print(f"[-] Ostrzeżenie podczas czyszczenia topiku alertów: {e}")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable("python3")
    
    jar_path = pathlib.Path(JAR_NAME).absolute()
    env.add_jars(jar_path.as_uri())

    kafka_consumer = FlinkKafkaConsumer(
        topics='transactions',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'group.id': 'flink_fraud_detector_group'}
    )

    # Aplikowanie wybranego trybu offsetu danych
    if READ_FROM_EARLIEST:
        print("[*] Tryb: EARLIEST. Flink przetwarza historię i odbudowuje profile behawioralne.")
        kafka_consumer.set_start_from_earliest()
    else:
        print("[*] Tryb: LATEST. Flink ignoruje bazę uczącą i analizuje wyłącznie ruch na żywo.")
        kafka_consumer.set_start_from_latest()

    ds = env.add_source(kafka_consumer)
    
    alerts_ds = ds.map(FraudDetectorMap()).filter(lambda value: value is not None)
    alerts_ds.print()
    
    print("[+] Silnik strumieniowy Apache Flink został pomyślnie zainicjalizowany.")
    print("[*] Uruchamianie zadania 'Card Fraud Detection Job'...")
    
    env.execute("Card Fraud Detection Architecture")

if __name__ == '__main__':
    run_flink_job()