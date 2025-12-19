import streamlit as st
import pandas as pd
from minio import Minio
import os
import io
import json
import matplotlib.pyplot as plt
import numpy as np

# ------------------ CONFIG ------------------
st.set_page_config(
    page_title="Data Platform Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# ------------------ STYLING ------------------
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stDataFrame {
        border-radius: 5px;
    }
    </style>
""", unsafe_allow_html=True)


# ------------------ CLIENT ------------------
@st.cache_resource
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


client = get_minio_client()


# ------------------ HELPERS ------------------
def load_dataset(filename: str, raw: bytes) -> pd.DataFrame:
    """Load dataset with robust error handling"""
    try:
        if filename.lower().endswith(".json"):
            try:
                return pd.read_json(io.BytesIO(raw))
            except ValueError:
                # Handle JSON lines format
                return pd.read_json(io.BytesIO(raw), lines=True)

        if filename.lower().endswith((".csv", ".tsv")):
            for sep in [",", "\t", ";", "|"]:
                try:
                    df = pd.read_csv(io.BytesIO(raw), sep=sep, encoding='utf-8')
                    if len(df.columns) > 1:
                        return df
                except Exception:
                    continue
            # Try with different encodings
            for encoding in ['latin-1', 'iso-8859-1']:
                try:
                    return pd.read_csv(io.BytesIO(raw), encoding=encoding)
                except Exception:
                    continue
            raise ValueError("Could not parse CSV with any delimiter or encoding")

        raise ValueError(f"Unsupported file format: {filename}")
    except Exception as e:
        raise ValueError(f"Failed to load dataset: {str(e)}")


def safe_nunique(series):
    """Calculate unique values safely, handling unhashable types"""
    try:
        return series.nunique()
    except TypeError:
        # Handle unhashable types (lists, dicts)
        try:
            return series.astype(str).nunique()
        except:
            return "N/A"


def get_column_info(df):
    """Get comprehensive column information with error handling"""
    info = []
    for col in df.columns:
        col_data = {
            "Column": col,
            "Type": str(df[col].dtype),
            "Unique": safe_nunique(df[col]),
            "Missing": int(df[col].isnull().sum()),
            "Missing %": f"{(df[col].isnull().sum() / len(df) * 100):.1f}%"
        }
        info.append(col_data)
    return pd.DataFrame(info)


def is_numeric_safe(series):
    """Check if column is numeric, handling edge cases"""
    return pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)


# ------------------ UI ------------------
st.title("📊 Data Platform Analytics Dashboard")
st.markdown("---")

# ------------------ DATASET SELECTION ------------------
with st.sidebar:
    st.header("📁 Dataset Selection")

    if not client.bucket_exists("input-data"):
        st.error("Input bucket `input-data` does not exist.")
        st.stop()

    objects = list(client.list_objects("input-data", recursive=True))

    if not objects:
        st.warning("No datasets uploaded yet.")
        st.stop()

    file_names = sorted([obj.object_name for obj in objects])

    selected_file = st.selectbox(
        "Select dataset to analyze",
        file_names,
        index=0
    )

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ------------------ LOAD DATA ------------------
@st.cache_data
def load_cached_data(filename):
    raw = client.get_object("input-data", filename).read()
    return load_dataset(filename, raw)


try:
    with st.spinner("Loading dataset..."):
        df = load_cached_data(selected_file)
except Exception as e:
    st.error(f"❌ Failed to load dataset: {e}")
    st.stop()

# ------------------ OVERVIEW METRICS ------------------
st.header("📈 Dataset Overview")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Rows", f"{len(df):,}")

with col2:
    st.metric("Total Columns", len(df.columns))

with col3:
    numeric_count = sum(is_numeric_safe(df[col]) for col in df.columns)
    st.metric("Numeric Columns", numeric_count)

with col4:
    total_missing = df.isnull().sum().sum()
    st.metric("Total Missing Values", f"{total_missing:,}")

st.markdown("---")

# ------------------ PREVIEW ------------------
st.header("📋 Data Preview")
st.caption(f"Showing dataset: **{selected_file}**")

preview_rows = st.slider("Number of rows to preview", 5, min(100, len(df)), 10)
st.dataframe(df.head(preview_rows), use_container_width=True, height=400)

st.markdown("---")

# ------------------ DETAILED SCHEMA ------------------
st.header("🧱 Schema & Data Quality")

schema_info = get_column_info(df)

tab1, tab2 = st.tabs(["📊 Column Details", "🔍 Data Quality Report"])

with tab1:
    st.dataframe(
        schema_info,
        use_container_width=True,
        height=400,
        column_config={
            "Column": st.column_config.TextColumn("Column Name", width="medium"),
            "Type": st.column_config.TextColumn("Data Type", width="small"),
            "Unique": st.column_config.NumberColumn("Unique Values", format="%d"),
            "Missing": st.column_config.NumberColumn("Missing Count", format="%d"),
            "Missing %": st.column_config.TextColumn("Missing %", width="small")
        }
    )

with tab2:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Missing Values Analysis")
        missing_data = schema_info[schema_info["Missing"] > 0].sort_values("Missing", ascending=False)

        if len(missing_data) > 0:
            st.dataframe(missing_data[["Column", "Missing", "Missing %"]], use_container_width=True)
        else:
            st.success("✅ No missing values detected!")

    with col2:
        st.subheader("Duplicate Rows")

        try:
            dup_count = int(df.duplicated().sum())

            if dup_count > 0:
                st.warning(f"⚠️ Found {dup_count:,} duplicate rows ({dup_count / len(df) * 100:.1f}%)")
            else:
                st.success("✅ No duplicate rows detected!")
        except (TypeError, ValueError):
            # Handle unhashable types by converting to string
            try:
                df_str = df.astype(str)
                dup_count = int(df_str.duplicated().sum())

                if dup_count > 0:
                    st.warning(f"⚠️ Found ~{dup_count:,} duplicate rows ({dup_count / len(df) * 100:.1f}%)")
                    st.caption("(Approximate due to complex data types)")
                else:
                    st.success("✅ No duplicate rows detected!")
            except Exception as e:
                st.info("ℹ️ Duplicate detection not available for this dataset (contains complex nested data)")

st.markdown("---")

# ------------------ NUMERIC ANALYTICS ------------------
numeric_cols = [col for col in df.columns if is_numeric_safe(df[col])]

if numeric_cols:
    st.header("🔢 Numeric Analytics")

    metric_col = st.selectbox(
        "Select numeric column for analysis",
        numeric_cols,
        index=0
    )

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        st.subheader("Descriptive Statistics")
        stats_df = df[metric_col].describe().to_frame()
        stats_df.columns = ["Value"]
        st.dataframe(stats_df, use_container_width=True)

    with col2:
        st.subheader("Distribution")
        fig, ax = plt.subplots(figsize=(8, 5))

        # Remove NaN values for plotting
        clean_data = df[metric_col].dropna()

        if len(clean_data) > 0:
            ax.hist(clean_data, bins=min(30, len(clean_data.unique())),
                    color='#1f77b4', alpha=0.7, edgecolor='black')
            ax.set_xlabel(metric_col, fontsize=12)
            ax.set_ylabel("Frequency", fontsize=12)
            ax.set_title(f"Distribution of {metric_col}", fontsize=14, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)
            st.pyplot(fig)
        else:
            st.warning("No valid data to plot")

    with col3:
        st.subheader("Boxplot")
        fig, ax = plt.subplots(figsize=(8, 5))

        clean_data = df[metric_col].dropna()

        if len(clean_data) > 0:
            ax.boxplot(clean_data, vert=False, patch_artist=True,
                       boxprops=dict(facecolor='#ff7f0e', alpha=0.7),
                       medianprops=dict(color='red', linewidth=2))
            ax.set_xlabel(metric_col, fontsize=12)
            ax.set_title(f"Boxplot of {metric_col}", fontsize=14, fontweight='bold')
            ax.grid(axis='x', alpha=0.3)
            st.pyplot(fig)
        else:
            st.warning("No valid data to plot")

    st.markdown("---")
else:
    st.info("ℹ️ No numeric columns available for analysis")

# ------------------ BATCH OUTPUT ------------------
st.header("📈 Batch Analytics Output")

if client.bucket_exists("batch-data"):
    try:
        obj = client.get_object("batch-data", "analytics_report.json")
        report = json.loads(obj.read())
        st.json(report, expanded=True)
    except Exception:
        st.info("ℹ️ Batch analytics not generated for this dataset yet.")
else:
    st.info("ℹ️ No batch analytics available.")

# ------------------ FOOTER ------------------
st.markdown("---")
st.caption("Data Platform Analytics Dashboard | Powered by Streamlit & MinIO")