import os
import sqlite3
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Data Directories
DATA_DIR = BASE_DIR / "data"
SOURCES_DIR = DATA_DIR / "Sources/Structured/source_structured_data"
TARGET_DIR = DATA_DIR / "Target"

# Medallion Zones
RAW_DIR = TARGET_DIR / "Raw"
CURATED_DIR = TARGET_DIR / "Curated"
GOLD_DIR = TARGET_DIR / "Gold"
SYSTEM_DIR = TARGET_DIR / "System"

# Database Paths - mimicking separate zones
RAW_DB_PATH = RAW_DIR / "raw.db"
CURATED_DB_PATH = CURATED_DIR / "curated.db"
GOLD_DB_PATH = GOLD_DIR / "gold.db"
AUDIT_DB_PATH = SYSTEM_DIR / "audit.db"

# Standard System Columns
SYS_COLS = {
    'BATCH_ID': 'batch_id',
    'SOURCE_SYSTEM': 'source_system',
    'INGESTION_TS': 'ingestion_timestamp',
    'PROCESS_NAME': 'process_name'
}

def get_db_connection(db_path):
    """Creates and returns a connection to the specified SQLite database."""
    # Ensure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    return conn
