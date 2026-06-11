import unittest
import ast
import random
from datetime import datetime, timedelta

cards_pool = []
current_simulation_time = datetime.utcnow()

class FakeFaker:
    def latitude(self): return 52.2297
    def longitude(self): return 21.0122
fake = FakeFaker()

with open("simulator/generator.py", "r", encoding="utf-8") as f:
    source_code = f.read()

tree = ast.parse(source_code)
function_node = None

for node in ast.walk(tree):
    if isinstance(node, ast.FunctionDef) and node.name == "generate_live_transaction":
        function_node = node
        break

if function_node is None:
    raise ImportError("Nie znaleziono funkcji generate_live_transaction w generator.py")

module_node = ast.Module(body=[function_node], type_ignores=[])
compiled_code = compile(module_node, filename="<string>", mode="exec")

local_namespace = {}
global_namespace = {
    "cards_pool": cards_pool,
    "current_simulation_time": current_simulation_time,
    "random": random,
    "timedelta": timedelta,
    "round": round,
    "print": lambda *args, **kwargs: None,
    "fake": fake
}

exec(compiled_code, global_namespace, local_namespace)
generate_live_transaction = local_namespace["generate_live_transaction"]


class TestTransactionGenerator(unittest.TestCase):

    def setUp(self):
        """Przygotowanie stabilnej puli z dokładnie jedną testową kartą."""
        self.mock_card = {
            "card_id": "card_test_gen",
            "user_id": "user_test_gen",
            "home_gps": [52.2297, 21.0122],
            "limit": 5000.0,
            "sum_amount": 1000.0,
            "tx_count": 20,
            "last_tx_time": datetime.utcnow()
        }
        
        cards_pool.clear()
        cards_pool.append(self.mock_card)

    def test_normal_transaction_generation(self):
        """Test sprawdzający, czy normalna transakcja mieści się w granicach średniej."""
        payload, card_id = generate_live_transaction(anomaly_type="NORMAL")
        
        self.assertEqual(card_id, "card_test_gen")
        self.assertEqual(payload["user_id"], "user_test_gen")
        
        historical_mean = self.mock_card["sum_amount"] / self.mock_card["tx_count"]
        lower_bound = historical_mean * 0.7
        upper_bound = historical_mean * 1.3
        
        self.assertTrue(lower_bound <= payload["amount"] <= upper_bound)

    def test_amount_anomaly_generation(self):
        """Test sprawdzający, czy anomalia kwotowa drastycznie przekracza próg 500 PLN i x4 średniej."""
        payload, _ = generate_live_transaction(anomaly_type="AMOUNT")
        
        historical_mean = self.mock_card["sum_amount"] / self.mock_card["tx_count"]
        self.assertTrue(payload["amount"] > 500.0)
        self.assertTrue(payload["amount"] > (historical_mean * 4))

    def test_location_anomaly_generation(self):
        """Test sprawdzający, czy anomalia lokalizacyjna poprawnie generuje współrzędne spoza bazy."""
        payload, _ = generate_live_transaction(anomaly_type="LOCATION")
        
        self.assertIn("gps", payload)
        self.assertEqual(len(payload["gps"]), 2)

    def test_simulation_time_advancement(self):
        """Test sprawdzający, czy czas globalny symulacji poprawnie przesuwa się do przodu przy generowaniu ruchu."""
        start_time = global_namespace["current_simulation_time"]
        
        generate_live_transaction(anomaly_type="NORMAL")
        
        updated_time = global_namespace["current_simulation_time"]
        self.assertTrue(updated_time > start_time)

if __name__ == '__main__':
    unittest.main()