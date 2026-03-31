from kafka import KafkaConsumer
from configs import kafka_config
import json

consumer = KafkaConsumer(
    "goit_alerts_output",
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="monitoring_center_hw6",
)

print("Центр моніторингу HW6 активований. Очікування агрегованих алертів...")

try:
    for message in consumer:
        data = message.value
        start = data.get('window_start', 'N/A')
        end = data.get('window_end', 'N/A')
        
        print(f"[{start} - {end}] ALERT: {data.get('message')}")
        print(f"Код: {data.get('code')} | Середня Т: {data.get('avg_temp'):.2f} | Середня В: {data.get('avg_hum'):.2f}")
        print("-" * 50)
except KeyboardInterrupt:
    print("Зупинка моніторингу...")
finally:
    consumer.close()