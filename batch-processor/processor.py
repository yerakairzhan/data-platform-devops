import pandas as pd
from minio import Minio
import os
import io

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

INPUT_BUCKET = "input-data"
OUTPUT_BUCKET = "batch-data"

if not client.bucket_exists(OUTPUT_BUCKET):
    client.make_bucket(OUTPUT_BUCKET)

objects = list(client.list_objects(INPUT_BUCKET, recursive=True))
if not objects:
    raise RuntimeError("No input files found")

frames = []
for obj in objects:
    data = client.get_object(INPUT_BUCKET, obj.object_name).read()
    df = pd.read_csv(io.BytesIO(data))
    df["source_file"] = obj.object_name
    frames.append(df)

df = pd.concat(frames, ignore_index=True)

report = {}

# Basic metrics
report["total_rows"] = len(df)
report["total_columns"] = len(df.columns)

# Column types
report["columns"] = {
    "numeric": df.select_dtypes(include="number").columns.tolist(),
    "categorical": df.select_dtypes(exclude="number").columns.tolist()
}

# Identifier analytics (realistic use case)
if "Identifier" in df.columns:
    report["identifier_stats"] = {
        "min": int(df["Identifier"].min()),
        "max": int(df["Identifier"].max()),
        "mean": float(df["Identifier"].mean()),
        "unique": int(df["Identifier"].nunique())
    }

# Data quality
report["missing_values"] = df.isnull().sum().to_dict()
report["duplicate_rows"] = int(df.duplicated().sum())

# Save report
output = io.BytesIO(pd.json_normalize(report).to_json(indent=2).encode())
client.put_object(
    OUTPUT_BUCKET,
    "analytics_report.json",
    output,
    length=output.getbuffer().nbytes,
    content_type="application/json"
)

print("Batch analytics completed successfully")
