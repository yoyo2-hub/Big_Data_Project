from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

producer.send('test-topic', b'Hello Kafka depuis Chayma')
producer.flush()

print("Message envoyé !")
