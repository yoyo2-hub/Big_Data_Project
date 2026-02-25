from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Connexion à Kafka en tant qu'Administrateur
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='ecommerce_init'
)

# On définit nos topics avec 3 PARTITIONS chacun !
topic_list = [
    NewTopic(name="raw-user-events", num_partitions=3, replication_factor=1),
    NewTopic(name="recommendations", num_partitions=3, replication_factor=1)
]

try:
    print("⏳ Création des topics avec 3 partitions en cours...")
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("✅ Succès ! Les topics 'raw-user-events' et 'recommendations' ont été créés avec 3 partitions.")
except TopicAlreadyExistsError:
    print("⚠️ Les topics existent déjà.")
except Exception as e:
    print(f"❌ Erreur : {e}")
finally:
    admin_client.close()