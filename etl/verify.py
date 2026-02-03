import pandas as pd
import config
import sqlite3
from config import GOLD_DB_PATH, CURATED_DB_PATH

def verify_data():
    print("--- Verifying Gold Zone Data ---")
    gold_conn = config.get_db_connection(GOLD_DB_PATH)
    curated_conn = config.get_db_connection(CURATED_DB_PATH)
    
    try:
        # Check Daily Sales
        print("\n[Sales Summary Daily] (First 5 rows):")
        daily_sales = pd.read_sql("SELECT * FROM sales_summary_daily LIMIT 5", gold_conn)
        print(daily_sales)
        
        # Verify Total Sales consistency
        gold_total = pd.read_sql("SELECT SUM(total_sales) as total FROM sales_summary_daily", gold_conn).iloc[0]['total']
        curated_total = pd.read_sql("SELECT SUM(total_amount) as total FROM fact_orders", curated_conn).iloc[0]['total']
        
        # Verify Metadata exists
        raw_conn = config.get_db_connection(config.RAW_DB_PATH)
        raw_row = pd.read_sql("SELECT ingestion_timestamp, batch_id FROM orders LIMIT 1", raw_conn).iloc[0]
        print(f"\nMetadata Check:")
        print(f"  Raw Ingestion Timestamp: {raw_row['ingestion_timestamp']}")
        print(f"  Batch ID: {raw_row['batch_id']}")
        raw_conn.close()
        
        # Verify Audit Log
        audit_conn = sqlite3.connect(config.AUDIT_DB_PATH)
        audit_log = pd.read_sql("SELECT * FROM pipeline_execution_log ORDER BY start_time DESC LIMIT 5", audit_conn)
        print(f"\nAudit Log (Last 5 Entries):")
        print(audit_log[['process_name', 'status', 'rows_processed']])
        
        # Verify Watermark
        print("\n[Watermark Table]:")
        try:
            watermarks = pd.read_sql("SELECT * FROM pipeline_watermark", audit_conn)
            print(watermarks)
        except Exception:
            print("  -> Table not found (Expected if first run just finished)")
        
        audit_conn.close()
        
        print(f"\nTotal Sales Consistency Check:")
        print(f"  Gold (Daily Sum): {gold_total}")
        print(f"  Curated (Orders Sum): {curated_total}")
        
        if abs(gold_total - curated_total) < 0.01:
            print("  -> MATCHED: Data integrity verified.")
        else:
            print("  -> MISMATCH: Check aggregation logic.")
            
        # Verify Wide Reporting Table
        print("\n[Reporting Sales Wide] (First 5 rows):")
        wide_df = pd.read_sql("SELECT order_id, total_amount, product_name, category, payment_method FROM reporting_sales_wide LIMIT 5", gold_conn)
        print(wide_df)
        
        # Verify Customer Stats
        print("\n[Reporting Customer Stats] (First 5 rows):")
        cust_stats = pd.read_sql("SELECT * FROM reporting_customer_stats LIMIT 5", gold_conn)
        print(cust_stats)
            


    finally:
        gold_conn.close()
        curated_conn.close()

if __name__ == "__main__":
    verify_data()
