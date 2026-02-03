import sqlite3
import pandas as pd
import os

# Configuration
DB_PATHS = {
    "RAW": "data/Target/Raw/raw.db",
    "CURATED": "data/Target/Curated/curated.db",
    "GOLD": "data/Target/Gold/gold.db",
    "AUDIT": "data/Target/System/audit.db"
}

def inspect_database(zone_name, db_path):
    print(f"\n{'='*20} {zone_name} ZONE {'='*20}")
    print(f"Path: {db_path}")
    
    if not os.path.exists(db_path):
        print("❌ Database file not found!")
        return

    conn = sqlite3.connect(db_path)
    try:
        # Get all tables
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
        
        if tables.empty:
            print("⚠️  No tables found in this database.")
        
        for table in tables['name']:
            print(f"\n>> Table: [{table}]")
            # Get count
            count = pd.read_sql(f"SELECT COUNT(*) as c FROM {table}", conn).iloc[0]['c']
            print(f"   Total Rows: {count}")
            
            # Get sample data
            df = pd.read_sql(f"SELECT * FROM {table} LIMIT 3", conn)
            print(f"   Sample Data (First 3 rows):")
            print(df.to_string(index=False))
            print("-" * 50)
            
    except Exception as e:
        print(f"❌ Error inspecting {zone_name}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    for zone, path in DB_PATHS.items():
        inspect_database(zone, path)
