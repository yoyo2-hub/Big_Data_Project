import sys
import json
from kafka import KafkaConsumer, TopicPartition

if len(sys.argv) < 2:
    print("❌ Erreur : Tu dois préciser le nom (ex: python consumer_ml.py ranim)")
    sys.exit(1)

target_user = sys.argv[1]
user_to_partition = {"ranim": 0, "chayma": 1, "oumayma": 2}

if target_user not in user_to_partition:
    print("❌ Utilisateur inconnu. Choisis : ranim ,chayma et oumayma .")
    sys.exit(1)

partition_id = user_to_partition[target_user]

consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

tp = TopicPartition('raw-user-events', partition_id)
consumer.assign([tp])

user_profile = {}

print(f"🎧 Je suis le Terminal dédié à {target_user} (Partition {partition_id})")
print("En attente de ses clics...")

try:
    for message in consumer:
        event = message.value
        category = event['category']
        action = event['action']
        
        if category not in user_profile:
            user_profile[category] = 0
            
        if action == "view": user_profile[category] += 1
        elif action == "click": user_profile[category] += 3
        elif action == "purchase": user_profile[category] += 10
            
        top_category = max(user_profile, key=user_profile.get)
        
        print(f"💡 [RECO POUR {target_user.upper()}] Catégorie préférée : {top_category} (Score: {user_profile[top_category]})")

except KeyboardInterrupt:
    print(f"\n🛑 Arrêt du terminal de {target_user}.")
finally:
    consumer.close()