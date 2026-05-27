import os
import json
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_DETECTIONS", "test")
GROUP_ID = "test-detection-group"  # можно задать через os.getenv при необходимости

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset='earliest',   # читаем с самого начала
    enable_auto_commit=True,        # автоматически фиксируем смещения
    group_id=GROUP_ID,
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"Ожидание сообщений из топика '{TOPIC}' (для выхода нажмите Ctrl+C)...\n")
try:
    for msg in consumer:
        print(f"→ Ключ: {msg.key}, Значение: {msg.value} [раздел={msg.partition}, смещение={msg.offset}]")
except KeyboardInterrupt:
    print("\nЧтение остановлено пользователем.")
finally:
    consumer.close()