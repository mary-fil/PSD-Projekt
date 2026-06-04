import json
import random
import uuid
from faker import Faker
from datetime import datetime, timezone

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_CARDS = 10000
NUM_USERS = 6000 

def generate_base_data():
    """Generowanie początkowej bazy użytkowników i kart"""
    print("Trwa generowanie bazy 10 000 kart...")
    
    users = [f"user_{uuid.uuid4().hex[:8]}" for _ in range(NUM_USERS)]
    cards = []

    for i in range(NUM_CARDS):
        user_id = random.choice(users)
        
        limit = round(random.uniform(1000, 20000), 2)
        
        card = {
            "card_id": f"card_{uuid.uuid4().hex[:8]}",
            "user_id": user_id,
            "total_limit": limit,
            "available_limit": limit,
            "base_location": {
                "latitude": float(fake.latitude()),
                "longitude": float(fake.longitude())
            }
        }
        cards.append(card)
        
    print(f"Wygenerowano {len(users)} użytkowników i {len(cards)} kart.")
    return cards

def generate_transaction(card):
    """Generowanie pojedynczej transakcji"""
    amount = round(random.uniform(5.0, 300.0), 2)
    
    card["available_limit"] -= amount
    
    lat = card["base_location"]["latitude"] + random.uniform(-0.05, 0.05)
    lon = card["base_location"]["longitude"] + random.uniform(-0.05, 0.05)
    
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card["card_id"],
        "user_id": card["user_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": {
            "latitude": round(lat, 6),
            "longitude": round(lon, 6)
        },
        "amount": amount,
        "available_limit": round(card["available_limit"], 2)
    }
    return transaction

if __name__ == "__main__":
    active_cards = generate_base_data()
    
    print("\nTestowe generowanie transakcji:")
    for _ in range(5):
        random_card = random.choice(active_cards)
        txn = generate_transaction(random_card)
        print(json.dumps(txn, indent=2))