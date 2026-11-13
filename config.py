import os
import sys
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Flask
SECRET_KEY = os.getenv("SECRET_KEY", "sudheer-fraud-benchmark-secret-2026")
FLASK_PORT = int(os.getenv("PORT", "5001"))
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "True").lower() == "true"

# Paths
DATA_DIR = BASE_DIR / os.getenv("DATA_DIR", "data")
RESULTS_DIR = BASE_DIR / "results"
PROCESSED_DATA_DIR = BASE_DIR / os.getenv("PROCESSED_DATA_DIR", "data/processed")

DATA_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Elliptic Dataset Paths
DATASET_NAME = os.getenv("DATASET_NAME", "elliptic")
DATASET_DIR = BASE_DIR / os.getenv("DATASET_DIR", "data/elliptic_bitcoin_dataset")
CLASSES_FILE = BASE_DIR / os.getenv("CLASSES_FILE", "data/elliptic_bitcoin_dataset/elliptic_txs_classes.csv")
EDGELIST_FILE = BASE_DIR / os.getenv("EDGELIST_FILE", "data/elliptic_bitcoin_dataset/elliptic_txs_edgelist.csv")
FEATURES_FILE = BASE_DIR / os.getenv("FEATURES_FILE", "data/elliptic_bitcoin_dataset/elliptic_txs_features.csv")

# Processed Cache Files
PROCESSED_TXS_FILE = PROCESSED_DATA_DIR / "elliptic_transactions_merged.csv"
PROCESSED_EDGES_FILE = PROCESSED_DATA_DIR / "elliptic_edgelist_clean.csv"
METADATA_FILE = PROCESSED_DATA_DIR / "dataset_metadata.json"

RESULTS_DB = BASE_DIR / os.getenv("RESULTS_DB", "results/experiments.db")

# InfluxDB
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8087")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "sudheer-benchmarking-secret-token-2026")
INFLUX_ORG = os.getenv("INFLUX_ORG", "sudheer")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "sudheer_transactions")
INFLUX_MEASUREMENT = "elliptic_transactions"

# Neo4j
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7688")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "sudheer_password_123")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")

# Defaults
DEFAULT_BATCH_SIZE = int(os.getenv("STREAM_BATCH_SIZE", "1000"))
DEFAULT_SUBSET_SIZE = int(os.getenv("DEFAULT_SUBSET_SIZE", "10000"))
