import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'raw-user-events'

# 1. On associe strictement chaque utilisateur à une partition (0, 1 ou 2)
USERS = [
    {"name": "Paul", "partition": 0},
    {"name": "Marie", "partition": 1},
    {"name": "Marc", "partition": 2}
]

PRODUCTS = [
    {"product_id": "P1", "category": "PC", "price": 1200.0},
    {"product_id": "P2", "category": "PC", "price": 850.0},
    {"product_id": "P3", "category": "Smartphone", "price": 700.0}
]
ACTIONS = ["view", "view", "click", "purchase"] 

print("🚀 Démarrage du simulateur pour Paul, Marie et Marc...")

try:
    while True:
        # On choisit un utilisateur au hasard
        user = random.choice(USERS)
        product = random.choice(PRODUCTS)
        action = random.choice(ACTIONS)

        event = {
            "user_id": user["name"],
            "category": product["category"],
            "action": action,
            "timestamp": int(time.time())
        }

        # LA MAGIE DU ROUTAGE : On force l'envoi dans la bonne partition !
        producer.send(
            TOPIC_NAME, 
            value=event,
            partition=user["partition"] # <-- On dicte la partition exacte
        )
        
        print(f"Envoyé : {user['name']} a fait '{action}' sur {product['category']}")
        time.sleep(random.uniform(1.0, 3.0))

except KeyboardInterrupt:
    print("\n🛑 Arrêt du simulateur.")
finally:
    producer.close()