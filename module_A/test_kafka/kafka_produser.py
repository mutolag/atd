import os
import json
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_DETECTIONS", "test")
NUM_MESSAGES = 5

def create_topic_if_not_exists():
    """Создаёт топик, если его ещё нет."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        existing_topics = admin.list_topics()
        if TOPIC not in existing_topics:
            # Параметры топика: 1 партиция, фактор репликации 1 (для локальной разработки)
            topic_obj = NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)
            admin.create_topics([topic_obj])
            print(f"Топик '{TOPIC}' успешно создан.")
        else:
            print(f"Топик '{TOPIC}' уже существует.")
    except TopicAlreadyExistsError:
        print(f"Топик '{TOPIC}' уже был создан параллельно.")
    except Exception as e:
        print(f"Предупреждение при создании топика: {e}. Возможно, создастся автоматически при первой записи.")
    finally:
        try:
            admin.close()
        except:
            pass

# Явно создаём топик (если автосоздание отключено)
create_topic_if_not_exists()

# Инициализация продюсера
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

print(f"Отправка {NUM_MESSAGES} сообщений в топик '{TOPIC}'...")
for i in range(1, NUM_MESSAGES + 1):
    key = f"key-{i}"
    value = {"number": i, "message": f"Тестовое сообщение номер {i}", "status": "active"}
    future = producer.send(TOPIC, key=key, value=value)
    try:
        metadata = future.get(timeout=10)
        print(f"✓ Отправлено: ключ='{key}', значение={value} -> раздел {metadata.partition}, смещение {metadata.offset}")
    except Exception as e:
        print(f"✗ Ошибка отправки сообщения {i}: {e}")

producer.flush()
producer.close()
print("Продюсер завершил работу.")