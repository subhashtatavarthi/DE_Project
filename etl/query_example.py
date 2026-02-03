import pandas as pd
import config
from config import GOLD_DB_PATH, RAW_DB_PATH

def query_gold_sales():
    """Example: Querying the Gold Zone for daily sales."""
    print(f"--- Querying Gold Database: {GOLD_DB_PATH} ---")
    
    conn = config.get_db_connection(GOLD_DB_PATH)
    try:
        query = """
        SELECT date, total_sales, unique_customers 
        FROM sales_summary_daily 
        ORDER BY total_sales DESC 
        LIMIT 5
        """
        print("\nTop 5 Days by Sales:")
        df = pd.read_sql(query, conn)
        print(df)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

def query_raw_count():
    """Example: Counting rows in Raw Customer table."""
    print(f"\n--- Querying Raw Database: {RAW_DB_PATH} ---")
    
    conn = config.get_db_connection(RAW_DB_PATH)
    try:
        query = "SELECT COUNT(*) as count FROM customers"
        df = pd.read_sql(query, conn)
        print(f"Total Raw Customers: {df.iloc[0]['count']}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    query_gold_sales()
    query_raw_count()
