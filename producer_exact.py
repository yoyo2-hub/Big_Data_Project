import sys
import json
import time
import random
from kafka import KafkaProducer

if len(sys.argv) < 2:
    print("❌ Erreur : Précise l'utilisateur (ex: python producer.py Paul)")
    sys.exit(1)

current_user = sys.argv[1]
user_to_partition = {"ranim": 0, "chayma": 1, "oumayma": 2}



if current_user not in user_to_partition:
    print("❌ Utilisateur inconnu. Choisis : ranim ,chayma et oumayma .")
    sys.exit(1)

partition_id = user_to_partition[current_user]

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'raw-user-events'

PRODUCTS = [
    {"category": "PC", "price": 1200.0},
    {"category": "Smartphone", "price": 700.0},
    {"category": "Ecouteurs", "price": 150.0}
]
ACTIONS = ["view", "view", "click", "purchase"] 

print(f"📱 Démarrage de la session (Téléphone) de : {current_user}...")

try:
    while True:
        product = random.choice(PRODUCTS)
        action = random.choice(ACTIONS)

        event = {
            "user_id": current_user,
            "category": product["category"],
            "action": action,
            "timestamp": int(time.time())
        }

        producer.send(
            TOPIC_NAME, 
            value=event,
            partition=partition_id
        )
        
        print(f"[{current_user}] a fait '{action}' sur {product['category']}")
        
        time.sleep(random.uniform(2.0, 5.0))

except KeyboardInterrupt:
    print(f"\n🛑 Fin de session pour {current_user}.")
finally:
    producer.close()