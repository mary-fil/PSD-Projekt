import unittest
import json
from datetime import datetime, timedelta
from detector.flink_detector import FraudDetectorMap, haversine_distance

class MockProducer:
    def send(self, topic, value):
        pass
    def flush(self):
        pass

class TestFraudDetector(unittest.TestCase):

    def setUp(self):
        """Inicjalizacja detektora przed każdym testem w izolacji."""
        self.detector = FraudDetectorMap()
        self.detector.producer = MockProducer() 

    def test_haversine_distance_standard(self):
        """Test poprawnego obliczania odległości geograficznej (Warszawa -> Berlin)."""
        warszawa = (52.2297, 21.0122)
        berlin = (52.5200, 13.4050)
        distance = haversine_distance(warszawa, berlin)
        self.assertAlmostEqual(distance, 517.0, delta=5.0)

    def test_haversine_same_coordinates(self):
        """Test brzegowy: Odległość między dokładnie tymi samymi punktami musi wynosić 0 km."""
        punkt = (52.2297, 21.0122)
        distance = haversine_distance(punkt, punkt)
        self.assertEqual(distance, 0.0)

    def test_amount_anomaly_detection(self):
        """Test wykrywania gwałtownego skoku kwoty ponad 4-krotność średniej życiowej."""
        card_id = "test_card_1"
        base_time = datetime(2026, 6, 11, 12, 0, 0)

        tx1 = {"card_id": card_id, "user_id": "user_1", "gps": [52.2, 21.0], "amount": 100.0, "card_limit": 5000.0, "timestamp": base_time.isoformat()}
        self.detector.map(json.dumps(tx1))

        tx_fraud = {"card_id": card_id, "user_id": "user_1", "gps": [52.2, 21.0], "amount": 650.0, "card_limit": 5000.0, "timestamp": (base_time + timedelta(minutes=5)).isoformat()}
        result_str = self.detector.map(json.dumps(tx_fraud))
        
        self.assertIsNotNone(result_str)
        self.assertEqual(json.loads(result_str)["alert_type"], "AMOUNT_ANOMALY")

    def test_amount_high_but_no_anomaly(self):
        """Test stabilności: Kwota powyżej 500 PLN, ale mieszcząca się w średniej (brak alertu)."""
        card_id = "test_card_high_normal"
        base_time = datetime(2026, 6, 11, 12, 0, 0)

        tx1 = {"card_id": card_id, "user_id": "user_2", "gps": [52.2, 21.0], "amount": 600.0, "card_limit": 15000.0, "timestamp": base_time.isoformat()}
        self.detector.map(json.dumps(tx1))

        tx2 = {"card_id": card_id, "user_id": "user_2", "gps": [52.2, 21.0], "amount": 700.0, "card_limit": 15000.0, "timestamp": (base_time + timedelta(minutes=10)).isoformat()}
        result_str = self.detector.map(json.dumps(tx2))
        
        self.assertIsNone(result_str)

    def test_state_poisoning_protection(self):
        """Test sprawdzający, czy trefna transakcja kwotowa NIE zatruwa średniej użytkownika."""
        card_id = "test_card_2"
        base_time = datetime(2026, 6, 11, 12, 0, 0)

        tx1 = {"card_id": card_id, "user_id": "user_1", "gps": [52.2, 21.0], "amount": 100.0, "card_limit": 5000.0, "timestamp": base_time.isoformat()}
        self.detector.map(json.dumps(tx1))

        tx_fraud = {"card_id": card_id, "user_id": "user_1", "gps": [52.2, 21.0], "amount": 650.0, "card_limit": 5000.0, "timestamp": (base_time + timedelta(minutes=5)).isoformat()}
        self.detector.map(json.dumps(tx_fraud))

        current_profile = self.detector.cards_cache[card_id]
        self.assertEqual(current_profile["welford_mean"], 100.0)
        self.assertEqual(current_profile["tx_count"], 1)

    def test_frequency_anomaly_and_spam_filter(self):
        """Test wykrywania anomalii częstotliwości oraz upewnienie się, że duplikaty są wyciszane."""
        card_id = "test_card_3"
        base_time = datetime(2026, 6, 11, 12, 0, 0)

        tx1 = {"card_id": card_id, "user_id": "user_1", "gps": [52.2, 21.0], "amount": 20.0, "card_limit": 5000.0, "timestamp": base_time.isoformat()}
        self.detector.map(json.dumps(tx1))

        tx2 = {"card_id": card_id, "user_id": "user_1", "gps": [52.2, 21.0], "amount": 22.0, "card_limit": 5000.0, "timestamp": (base_time + timedelta(seconds=1)).isoformat()}
        res2_str = self.detector.map(json.dumps(tx2))
        self.assertIsNotNone(res2_str)
        self.assertEqual(json.loads(res2_str)["alert_type"], "FREQUENCY_ANOMALY")

        tx3 = {"card_id": card_id, "user_id": "user_1", "gps": [52.2, 21.0], "amount": 25.0, "card_limit": 5000.0, "timestamp": (base_time + timedelta(seconds=2)).isoformat()}
        res3_str = self.detector.map(json.dumps(tx3))
        self.assertIsNone(res3_str)

    def test_frequency_reset_after_time_window(self):
        """Test wygasania wyciszenia: Po 15 sekundach kolejna szybka transakcja powinna wygenerować nowy alert."""
        card_id = "test_card_reset"
        base_time = datetime(2026, 6, 11, 12, 0, 0)

        tx1 = {"card_id": card_id, "user_id": "user_3", "gps": [52.2, 21.0], "amount": 15.0, "card_limit": 3000.0, "timestamp": base_time.isoformat()}
        self.detector.map(json.dumps(tx1))

        tx2 = {"card_id": card_id, "user_id": "user_3", "gps": [52.2, 21.0], "amount": 15.0, "card_limit": 3000.0, "timestamp": (base_time + timedelta(seconds=1)).isoformat()}
        self.detector.map(json.dumps(tx2))

        tx_normal = {"card_id": card_id, "user_id": "user_3", "gps": [52.2, 21.0], "amount": 15.0, "card_limit": 3000.0, "timestamp": (base_time + timedelta(seconds=16)).isoformat()}
        self.detector.map(json.dumps(tx_normal))

        tx_new_fraud = {"card_id": card_id, "user_id": "user_3", "gps": [52.2, 21.0], "amount": 15.0, "card_limit": 3000.0, "timestamp": (base_time + timedelta(seconds=17)).isoformat()}
        res_str = self.detector.map(json.dumps(tx_new_fraud))
        
        self.assertIsNotNone(res_str)
        self.assertEqual(json.loads(res_str)["alert_type"], "FREQUENCY_ANOMALY")

    def test_location_anomaly_detection(self):
        """Test niemożliwej podróży (Impossible Travel) - Warszawa do Nowego Jorku w 1 minutę."""
        card_id = "test_card_4"
        base_time = datetime(2026, 6, 11, 12, 0, 0)

        tx_pl = {"card_id": card_id, "user_id": "user_1", "gps": [52.2297, 21.0122], "amount": 50.0, "card_limit": 5000.0, "timestamp": base_time.isoformat()}
        self.detector.map(json.dumps(tx_pl))

        tx_ny = {"card_id": card_id, "user_id": "user_1", "gps": [40.7128, -74.0060], "amount": 60.0, "card_limit": 5000.0, "timestamp": (base_time + timedelta(minutes=1)).isoformat()}
        result_str = self.detector.map(json.dumps(tx_ny))
        
        self.assertIsNotNone(result_str)
        self.assertEqual(json.loads(result_str)["alert_type"], "LOCATION_ANOMALY")

    def test_location_travel_possible(self):
        """Test dopuszczalnego ruchu: Przejazd 15 km w 45 minut nie może generować alertu."""
        card_id = "test_card_travel_ok"
        base_time = datetime(2026, 6, 11, 12, 0, 0)

        tx_a = {"card_id": card_id, "user_id": "user_4", "gps": [52.2297, 21.0122], "amount": 30.0, "card_limit": 5000.0, "timestamp": base_time.isoformat()}
        self.detector.map(json.dumps(tx_a))

        tx_b = {"card_id": card_id, "user_id": "user_4", "gps": [52.3000, 21.1000], "amount": 35.0, "card_limit": 5000.0, "timestamp": (base_time + timedelta(minutes=45)).isoformat()}
        result_str = self.detector.map(json.dumps(tx_b))
        
        self.assertIsNone(result_str)

if __name__ == '__main__':
    unittest.main()