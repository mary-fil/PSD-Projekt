import os
java_opts = "--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
os.environ["JVM_ARGS"] = java_opts
os.environ["FLINK_ENV_JAVA_OPTS"] = java_opts

import json
import math
import urllib.request
from datetime import datetime
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
        # Pamięć stanowa Flinka przechowująca historię transakcji kart
        self.cards_cache = {}

    def map(self, value_str):
        tx = json.loads(value_str)
        card_id = tx["card_id"]
        current_gps = tx["gps"]
        current_amount = tx["amount"]
        current_time = datetime.fromisoformat(tx["timestamp"])
        
        generator_tag = tx.get("anomaly_type", "NORMAL")
        
        alert_triggered = None

        if card_id in self.cards_cache:
            last_tx = self.cards_cache[card_id]
            prev_gps = last_tx["gps"]
            prev_amount = last_tx["amount"]
            prev_time = datetime.fromisoformat(last_tx["timestamp"])

            time_delta = (current_time - prev_time).total_seconds()  # Różnica w sekundach
            time_delta_hours = time_delta / 3600.0

            # LOKALIZACJA (Impossible Travel) ---
            distance = haversine_distance(prev_gps, current_gps)
            if time_delta_hours > 0:
                speed = distance / time_delta_hours
                if distance > 50 and speed > 900:
                    alert_triggered = {
                        "alert_type": "LOCATION_ANOMALY",
                        "card_id": card_id,
                        "details": f"Nierealna prędkość: {round(speed, 2)} km/h. Generator otagował to jako: {generator_tag}",
                        "timestamp": tx["timestamp"],
                        "amount": current_amount
                    }

            # KWOTA (Drastyczny skok wartości) ---
            if current_amount > 500 and current_amount > (prev_amount * 4):
                alert_triggered = {
                    "alert_type": "AMOUNT_ANOMALY",
                    "card_id": card_id,
                    "details": f"Skok kwoty z {prev_amount} do {current_amount} PLN. Generator otagował to jako: {generator_tag}",
                    "timestamp": tx["timestamp"],
                    "amount": current_amount
                }

            # CZĘSTOTLIWOŚĆ (Seria transakcji poniżej 5 sekund) ---
            if time_delta > 0 and time_delta < 5:
                alert_triggered = {
                    "alert_type": "FREQUENCY_ANOMALY",
                    "card_id": card_id,
                    "details": f"Odstęp czasu: {round(time_delta, 2)} sek. Generator otagował to jako: {generator_tag}",
                    "timestamp": tx["timestamp"],
                    "amount": current_amount
                }

        # Zapisujemy obecną transakcję do stanu
        self.cards_cache[card_id] = tx

        if alert_triggered:
            print(f"⚠️ [FLINK DETECTED]: {alert_triggered['alert_type']} na karcie {card_id} | {alert_triggered['details']}")
            return json.dumps(alert_triggered)
        
        return None

def run_flink_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_python_executable("python3")
    
    jar_path = os.path.abspath(JAR_NAME)
    env.add_jars("file:" + jar_path)

    kafka_consumer = FlinkKafkaConsumer(
        topics='transactions',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'flink_fraud_detector_group'}
    )
    kafka_consumer.set_start_from_latest()
    
    ds = env.add_source(kafka_consumer)
    alerts_ds = ds.map(FraudDetectorMap()).filter(lambda value: value is not None)
    alerts_ds.print()
    
    env.execute("Card Fraud Detection Architecture")

if __name__ == '__main__':
    run_flink_job()