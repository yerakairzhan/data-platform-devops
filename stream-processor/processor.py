from kafka import KafkaConsumer
from minio import Minio
import os

KAFKA_TOPIC = "events"
KAFKA_BOOTSTRAP = "kafka:9092"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = "stream-data"

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(BUCKET):
    minio_client.make_bucket(BUCKET)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

for message in consumer:
    content = message.value.decode()
    object_name = f"event-{message.offset}.txt"

    minio_client.put_object(
        BUCKET,
        object_name,
        content.encode(),
        length=len(content)
    )

    print(f"Processed message {message.offset}")
