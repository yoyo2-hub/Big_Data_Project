from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import json, time, threading

app = Flask(__name__)
CORS(app)  # Autorise le HTML à appeler l'API

# ─── PRODUCER ───────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ─── SCORES EN MÉMOIRE (comme ton consumer.py) ──────
user_profiles = {}  # { "ranim": {"Soin Visage": 5, ...} }
user_partitions = {}
partition_counter = [0]

def get_or_create_user(user_id):
    if user_id not in user_profiles:
        user_profiles[user_id] = {
            "Soin Visage": 0, "Maquillage": 0,
            "Soin Corps": 0, "Parfum": 0, "Cheveux": 0
        }
        user_partitions[user_id] = partition_counter[0] % 3
        partition_counter[0] += 1
    return user_profiles[user_id], user_partitions[user_id]

# ─── ROUTE : Envoyer un événement → Kafka ───────────
@app.route('/event', methods=['POST'])
def send_event():
    data = request.json
    user_id  = data.get('user_id')
    category = data.get('category')
    action   = data.get('action')   # view / click / purchase

    if not all([user_id, category, action]):
        return jsonify({"error": "Champs manquants"}), 400

    profile, partition = get_or_create_user(user_id)

    # Même logique que ton consumer.py
    if action == "view":     profile[category] = profile.get(category, 0) + 1
    elif action == "click":  profile[category] = profile.get(category, 0) + 3
    elif action == "purchase": profile[category] = profile.get(category, 0) + 10

    event = {
        "user_id": user_id,
        "category": category,
        "action": action,
        "timestamp": int(time.time())
    }

    # Envoie sur Kafka
    producer.send('raw-user-events', value=event, partition=partition)
    producer.flush()

    top_cat = max(profile, key=profile.get)
    print(f"[KAFKA] {user_id} → {action} sur {category} | Top: {top_cat} ({profile[top_cat]}pts)")

    return jsonify({
        "success": True,
        "partition": partition,
        "profile": profile,
        "top_category": top_cat,
        "top_score": profile[top_cat]
    })

# ─── ROUTE : Récupérer le profil d'un utilisateur ───
@app.route('/profile/<user_id>', methods=['GET'])
def get_profile(user_id):
    profile, partition = get_or_create_user(user_id)
    top_cat = max(profile, key=profile.get)
    return jsonify({
        "user_id": user_id,
        "partition": partition,
        "profile": profile,
        "top_category": top_cat,
        "total_score": sum(profile.values())
    })

# ─── ROUTE : Tous les profils (pour analytics) ──────
@app.route('/profiles', methods=['GET'])
def all_profiles():
    result = {}
    for uid in user_profiles:
        p = user_profiles[uid]
        result[uid] = {
            "partition": user_partitions[uid],
            "profile": p,
            "top_category": max(p, key=p.get),
            "total_score": sum(p.values())
        }
    return jsonify(result)

# ─── ROUTE : Recommandations pour un utilisateur ────
@app.route('/recommendations/<user_id>', methods=['GET'])
def get_recommendations(user_id):
    profile, partition = get_or_create_user(user_id)
    sorted_cats = sorted(profile.items(), key=lambda x: x[1], reverse=True)
    top_cats = [c for c, _ in sorted_cats[:2]]

    # Envoie la reco sur le topic "recommendations"
    reco_event = {
        "user_id": user_id,
        "recommended_categories": top_cats,
        "scores": profile,
        "timestamp": int(time.time())
    }
    producer.send('recommendations', value=reco_event, partition=partition)
    producer.flush()

    return jsonify({
        "user_id": user_id,
        "top_categories": top_cats,
        "profile": profile
    })

if __name__ == '__main__':
    print("✅ API Flask démarrée sur http://localhost:5000")
    print("📡 Connectée à Kafka sur localhost:9092")
    app.run(debug=True, port=5000)