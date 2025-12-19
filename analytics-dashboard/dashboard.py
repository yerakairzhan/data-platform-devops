import streamlit as st
from minio import Minio
import pandas as pd
import io
import os

st.set_page_config(page_title="Data Platform Dashboard")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

st.title("Analytics Dashboard")

if client.bucket_exists("batch-data"):
    obj = client.get_object("batch-data", "batch_result.csv")
    df = pd.read_csv(io.BytesIO(obj.read()))
    st.dataframe(df)
else:
    st.warning("No batch data available")
