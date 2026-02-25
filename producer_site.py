import json
import time
import random
from kafka import KafkaProducer

# 1. Initialisation du Producteur Kafka
# value_serializer permet de transformer automatiquement nos dictionnaires Python en format JSON
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'raw-user-events'

# 2. Quelques données fictives pour notre site e-commerce
USER_IDS = [f"user_{i}" for i in range(1, 21)]  # 20 utilisateurs (user_1 à user_20)
PRODUCTS = [
    {"product_id": "P1", "category": "PC", "price": 1200.0},
    {"product_id": "P2", "category": "PC", "price": 850.0},
    {"product_id": "P3", "category": "Smartphone", "price": 700.0},
    {"product_id": "P4", "category": "Smartphone", "price": 400.0},
    {"product_id": "P5", "category": "Accessoires", "price": 50.0},
    {"product_id": "P6", "category": "Accessoires", "price": 25.0}
]
# On met plus de "view" que de "purchase" pour que ce soit réaliste
ACTIONS = ["view", "view", "view", "click", "click", "purchase"] 

print("🚀 Démarrage du simulateur e-commerce... (Appuie sur Ctrl+C pour l'arrêter)")

try:
    while True:
        # 3. Génération d'un événement aléatoire
        user = random.choice(USER_IDS)
        product = random.choice(PRODUCTS)
        action = random.choice(ACTIONS)

        event = {
            "user_id": user,
            "product_id": product["product_id"],
            "category": product["category"],
            "price": product["price"],
            "action": action,
            "timestamp": int(time.time())
        }

        # 4. Envoi de l'événement dans le topic Kafka
        producer.send(
            TOPIC_NAME,
            key=user.encode('utf-8'), # La clé ajoutée
            value=event
        )
        
        print(f"Événement envoyé : {event}")
        
        # Pause aléatoire entre 0.5 et 2 secondes pour simuler un trafic en temps réel
        time.sleep(random.uniform(0.5, 2.0))

except KeyboardInterrupt:
    print("\n🛑 Arrêt du simulateur.")
finally:
    # On s'assure de bien fermer la connexion à Kafka
    producer.close()