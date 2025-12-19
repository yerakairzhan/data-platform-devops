import pandas as pd
from minio import Minio
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = "batch-data"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not client.bucket_exists(BUCKET):
    client.make_bucket(BUCKET)

data = pd.DataFrame({
    "value": [10, 20, 30, 40]
})

output_file = "/tmp/batch_result.csv"
data.to_csv(output_file, index=False)

client.fput_object(
    BUCKET,
    "batch_result.csv",
    output_file
)

print("Batch processing completed")
