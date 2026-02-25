import sys
import json
from kafka import KafkaConsumer, TopicPartition

# 1. On demande à l'exécution quel utilisateur ce terminal doit surveiller
if len(sys.argv) < 2:
    print("❌ Erreur : Tu dois préciser le nom (ex: python consumer_ml.py Paul)")
    sys.exit(1)

target_user = sys.argv[1]
user_to_partition = {"Paul": 0, "Marie": 1, "Marc": 2}

if target_user not in user_to_partition:
    print("❌ Utilisateur inconnu. Choisis : Paul, Marie ou Marc.")
    sys.exit(1)

partition_id = user_to_partition[target_user]

# 2. Création du consommateur (SANS group_id, car on gère manuellement)
consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# 3. L'ASSIGNATION MANUELLE : On branche ce terminal sur UNE SEULE partition
tp = TopicPartition('raw-user-events', partition_id)
consumer.assign([tp])

user_profile = {} # Le profil uniquement pour cet utilisateur

print(f"🎧 Je suis le Terminal dédié à {target_user} (Partition {partition_id})")
print("En attente de ses clics...")

try:
    for message in consumer:
        event = message.value
        category = event['category']
        action = event['action']
        
        if category not in user_profile:
            user_profile[category] = 0
            
        # Feature Engineering simple
        if action == "view": user_profile[category] += 1
        elif action == "click": user_profile[category] += 3
        elif action == "purchase": user_profile[category] += 10
            
        top_category = max(user_profile, key=user_profile.get)
        
        print(f"💡 [RECO POUR {target_user.upper()}] Catégorie préférée : {top_category} (Score: {user_profile[top_category]})")

except KeyboardInterrupt:
    print(f"\n🛑 Arrêt du terminal de {target_user}.")
finally:
    consumer.close()