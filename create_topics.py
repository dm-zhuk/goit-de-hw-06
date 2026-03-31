from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

my_name = "goit"
topics = [
    "building_sensors", 
    "temperature_alerts", 
    "humidity_alerts", 
    "alerts_output"  # Новий топік -HW6
]

new_topics = [
    NewTopic(name=f"{my_name}_{t}", num_partitions=3, replication_factor=1)
    for t in topics
]

try:
    print(f"Спроба створення топіків для: {my_name}...")
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"Запит на створення відправлено успішно.")

except Exception as e:
    print(f"Примітка: {e}")

finally:
    print("\nПеревірка топіків:")
    all_topics = admin_client.list_topics()
    my_topics = [topic for topic in all_topics if my_name in topic]
    
    for topic in my_topics:
        print(topic)
    
    admin_client.close()
    print("\nЗ'єднання з Kafka закрите")