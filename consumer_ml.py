import json
from kafka import KafkaConsumer, KafkaProducer

# 1. Configuration des connexions Kafka
# Le consommateur écoute les événements bruts du site web
consumer = KafkaConsumer(
    'raw-user-events',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest', # On veut seulement les messages en direct
    group_id='ml-team',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Le producteur va envoyer les recommandations générées
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Le "Profil Utilisateur" en mémoire locale (Stateful Processing)
# Ça va ressembler à ça : {"user_1": {"PC": 5, "Smartphone": 2}}
user_profiles = {}

print("🧠 Démarrage du Moteur d'Analyse et de Recommandation... (En attente de données)")

try:
    # 3. Boucle infinie pour traiter les données en Temps Réel
    for message in consumer:
        event = message.value
        user_id = event['user_id']
        category = event['category']
        action = event['action']
        
        # Si c'est un nouvel utilisateur, on lui crée un profil vide
        if user_id not in user_profiles:
            user_profiles[user_id] = {}
            
        # Si c'est une nouvelle catégorie pour lui, on l'initialise à 0
        if category not in user_profiles[user_id]:
            user_profiles[user_id][category] = 0
            
        # 4. FEATURE ENGINEERING (Pondération des actions)
        # On donne un "poids" différent selon l'importance de l'action
        if action == "view":
            user_profiles[user_id][category] += 1
        elif action == "click":
            user_profiles[user_id][category] += 3
        elif action == "purchase":
            user_profiles[user_id][category] += 10 # Un achat montre un fort intérêt
            
        # 5. LOGIQUE DE RECOMMANDATION (Content-Based très simple)
        # On cherche la catégorie avec le plus haut score pour cet utilisateur
        top_category = max(user_profiles[user_id], key=user_profiles[user_id].get)
        
        # On construit le message de recommandation
        recommendation = {
            "user_id": user_id,
            "recommended_category": top_category,
            "reason_score": user_profiles[user_id][top_category]
        }
        
        # 6. Envoi dans le nouveau topic Kafka "recommendations"
        producer.send('recommendations', recommendation)
        print(f"💡 [RECO] {user_id} s'intéresse à {category} -> On lui recommande : {top_category} (Score: {recommendation['reason_score']})")

except KeyboardInterrupt:
    print("\n🛑 Arrêt du moteur d'analyse.")
finally:
    consumer.close()
    producer.close()