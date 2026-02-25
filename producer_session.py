import sys
import json
import time
import random
from kafka import KafkaProducer

# 1. On vérifie quel utilisateur ce terminal représente (Le téléphone de qui ?)
if len(sys.argv) < 2:
    print("❌ Erreur : Précise l'utilisateur (ex: python producer.py Paul)")
    sys.exit(1)

current_user = sys.argv[1]
user_to_partition = {"Paul": 0, "Marie": 1, "Marc": 2}

if current_user not in user_to_partition:
    print("❌ Utilisateur inconnu. Choisis : Paul, Marie ou Marc.")
    sys.exit(1)

partition_id = user_to_partition[current_user]

# 2. Initialisation du Producteur
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
        # L'utilisateur navigue sur son téléphone
        product = random.choice(PRODUCTS)
        action = random.choice(ACTIONS)

        event = {
            "user_id": current_user,
            "category": product["category"],
            "action": action,
            "timestamp": int(time.time())
        }

        # On envoie l'action directement dans le tuyau (partition) réservé à cet utilisateur
        producer.send(
            TOPIC_NAME, 
            value=event,
            partition=partition_id
        )
        
        print(f"[{current_user}] a fait '{action}' sur {product['category']}")
        
        # Le client met du temps à cliquer (entre 2 et 5 secondes)
        time.sleep(random.uniform(2.0, 5.0))

except KeyboardInterrupt:
    print(f"\n🛑 Fin de session pour {current_user}.")
finally:
    producer.close()