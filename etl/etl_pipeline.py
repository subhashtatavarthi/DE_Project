import pandas as pd
import os
import sqlite3
import config
from datetime import datetime
from config import RAW_DB_PATH, CURATED_DB_PATH, GOLD_DB_PATH, SOURCES_DIR, SYS_COLS
from audit_manager import AuditManager

def extract_to_raw(audit, batch_id):
    """Reads CSV files and loads them into the Raw database with system columns."""
    process_name = "Extract_Source_to_Raw"
    execution_id = audit.log_start(process_name, "Raw")
    total_rows = 0
    
    conn = config.get_db_connection(RAW_DB_PATH)
    files_to_load = ["customers.csv", "order_lines.csv", "orders.csv", "payments.csv", "products.csv"]
    
    try:
        for file_name in files_to_load:
            file_path = SOURCES_DIR / file_name
            if not file_path.exists():
                print(f"Skipping missing file: {file_name}")
                continue
            
            df = pd.read_csv(file_path)
            
            # --- System Columns ---
            df[SYS_COLS['INGESTION_TS']] = datetime.now()
            df[SYS_COLS['BATCH_ID']] = batch_id
            df[SYS_COLS['SOURCE_SYSTEM']] = 'CSV_Source'
            df['source_filename'] = file_name
            
            table_name = file_name.replace(".csv", "")
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            total_rows += len(df)
            print(f"  -> Processed {len(df)} rows for {table_name}")
            
        # Update Watermark to NOW
        audit.update_watermark(process_name, datetime.now(), batch_id)
        
        audit.log_end(execution_id, status='SUCCESS', rows_processed=total_rows)
        
    except Exception as e:
        audit.log_end(execution_id, status='FAILED', error_message=str(e))
        print(f"Error in Extraction: {e}")
        raise e
    finally:
        conn.close()

def load_dimensions(audit, batch_id):
    """Processes Customers and Products (Dimensions)."""
    process_name = "Load_Dimensions"
    execution_id = audit.log_start(process_name, "Curated")
    total_rows = 0
    
    raw_conn = config.get_db_connection(RAW_DB_PATH)
    curated_conn = config.get_db_connection(CURATED_DB_PATH)
    
    try:
        # --- Dim Customers ---
        # In a real scenario, we would implement SCD logic here.
        # For now, we clean and load standard columns.
        customers = pd.read_sql("SELECT * FROM customers", raw_conn)
        customers = customers.drop_duplicates(subset=['customer_id'])
        
        # Add System Columns
        customers[SYS_COLS['BATCH_ID']] = batch_id
        customers[SYS_COLS['Process_Name'.upper()]] = process_name # Using key from config if mapped, else manual
        customers['transformation_timestamp'] = datetime.now()
        
        customers.to_sql("dim_customers", curated_conn, if_exists="replace", index=False)
        total_rows += len(customers)
        
        # --- Dim Products ---
        products = pd.read_sql("SELECT * FROM products", raw_conn)
        products = products.drop_duplicates(subset=['product_id'])
        
        products[SYS_COLS['BATCH_ID']] = batch_id
        products['transformation_timestamp'] = datetime.now()
        
        products.to_sql("dim_products", curated_conn, if_exists="replace", index=False)
        total_rows += len(products)
        
        # Update Watermark
        audit.update_watermark(process_name, datetime.now(), batch_id)
        
        audit.log_end(execution_id, status='SUCCESS', rows_processed=total_rows)
        print(f"  -> Processed {len(customers)} Customers and {len(products)} Products.")
        
    except Exception as e:
        audit.log_end(execution_id, status='FAILED', error_message=str(e))
        print(f"Error in Dimensions: {e}")
        raise e
    finally:
        raw_conn.close()
        curated_conn.close()

def load_facts(audit, batch_id):
    """Processes Orders (Facts)."""
    process_name = "Load_Facts"
    execution_id = audit.log_start(process_name, "Curated")
    total_rows = 0
    
    raw_conn = config.get_db_connection(RAW_DB_PATH)
    curated_conn = config.get_db_connection(CURATED_DB_PATH)
    
    try:
        orders = pd.read_sql("SELECT * FROM orders", raw_conn)
        order_lines = pd.read_sql("SELECT * FROM order_lines", raw_conn)
        payments = pd.read_sql("SELECT * FROM payments", raw_conn)
        
        # Cleaning
        orders['order_date'] = pd.to_datetime(orders['order_date'])
        
        # --- Fact Orders (Pure) ---
        fact_orders = orders.copy()
        fact_orders[SYS_COLS['BATCH_ID']] = batch_id
        fact_orders['transformation_timestamp'] = datetime.now()
        
        # --- Fact Order Lines ---
        fact_order_lines = order_lines.copy()
        fact_order_lines[SYS_COLS['BATCH_ID']] = batch_id
        fact_order_lines['transformation_timestamp'] = datetime.now()
        
        # --- Fact Payments (New Separate Table) ---
        fact_payments = payments.copy()
        fact_payments[SYS_COLS['BATCH_ID']] = batch_id
        fact_payments['transformation_timestamp'] = datetime.now()
        
        # Load to Curated
        fact_orders.to_sql("fact_orders", curated_conn, if_exists="replace", index=False)
        fact_order_lines.to_sql("fact_order_lines", curated_conn, if_exists="replace", index=False)
        fact_payments.to_sql("fact_payments", curated_conn, if_exists="replace", index=False)
        
        total_rows = len(fact_orders) + len(fact_order_lines) + len(fact_payments)
        
        # Update Watermark
        audit.update_watermark(process_name, datetime.now(), batch_id)
        
        audit.log_end(execution_id, status='SUCCESS', rows_processed=total_rows)
        print(f"  -> Processed {len(fact_orders)} Orders, {len(fact_payments)} Payments.")
        
    except Exception as e:
        audit.log_end(execution_id, status='FAILED', error_message=str(e))
        print(f"Error in Facts: {e}")
        raise e
    finally:
        raw_conn.close()
        curated_conn.close()

def aggregate_to_gold(audit, batch_id):
    """Aggregates Business Metrics."""
    process_name = "Aggregate_Gold"
    execution_id = audit.log_start(process_name, "Gold")
    total_rows = 0
    
    curated_conn = config.get_db_connection(CURATED_DB_PATH)
    gold_conn = config.get_db_connection(GOLD_DB_PATH)
    
    try:
        # Load necessary tables from Curated
        fact_orders = pd.read_sql("SELECT * FROM fact_orders", curated_conn)
        fact_lines = pd.read_sql("SELECT * FROM fact_order_lines", curated_conn)
        dim_products = pd.read_sql("SELECT * FROM dim_products", curated_conn)
        dim_customers = pd.read_sql("SELECT * FROM dim_customers", curated_conn)
        fact_payments = pd.read_sql("SELECT * FROM fact_payments", curated_conn)
        
        fact_orders['order_date'] = pd.to_datetime(fact_orders['order_date'])
        
        # 1. Sales Summary Daily (Existing)
        daily_sales = fact_orders.groupby(fact_orders['order_date'].dt.date).agg(
            total_sales=('total_amount', 'sum'),
            total_orders=('order_id', 'count')
        ).reset_index()
        daily_sales['date'] = daily_sales['order_date']
        daily_sales[SYS_COLS['BATCH_ID']] = batch_id
        daily_sales['aggregation_timestamp'] = datetime.now()
        daily_sales.to_sql("sales_summary_daily", gold_conn, if_exists="replace", index=False)
        
        # 2. Reporting Sales Wide (New - OBT)
        # Join: Orders -> Lines -> Products -> Customers -> Payments
        wide_df = fact_orders.merge(fact_lines, on='order_id', suffixes=('', '_line'))
        wide_df = wide_df.merge(dim_products, on='product_id', suffixes=('', '_prod'))
        wide_df = wide_df.merge(dim_customers, on='customer_id', suffixes=('', '_cust'))
        wide_df = wide_df.merge(fact_payments, on='order_id', how='left', suffixes=('', '_pay')) # Left join for payments
        
        # Select useful columns for BI
        cols_to_keep = [
            'order_id', 'order_date', 'status', 'total_amount', 
            'product_name', 'category', 'sub_category', 'brand', 'quantity', 'line_total',
            'first_name', 'last_name', 'city', 'state', 'segment',
            'payment_method', 'payment_amount'
        ]
        reporting_sales_wide = wide_df[[c for c in cols_to_keep if c in wide_df.columns]].copy()
        
        reporting_sales_wide[SYS_COLS['BATCH_ID']] = batch_id
        reporting_sales_wide['aggregation_timestamp'] = datetime.now()
        reporting_sales_wide.to_sql("reporting_sales_wide", gold_conn, if_exists="replace", index=False)
        
        # 3. Reporting Customer Stats (New)
        cust_stats = fact_orders.groupby('customer_id').agg(
            first_order=('order_date', 'min'),
            last_order=('order_date', 'max'),
            total_spend=('total_amount', 'sum'),
            orders_count=('order_id', 'count')
        ).reset_index()
        
        # Enrich with name
        cust_stats = cust_stats.merge(dim_customers[['customer_id', 'first_name', 'last_name', 'email']], on='customer_id')
        
        cust_stats[SYS_COLS['BATCH_ID']] = batch_id
        cust_stats['aggregation_timestamp'] = datetime.now()
        cust_stats.to_sql("reporting_customer_stats", gold_conn, if_exists="replace", index=False)
        
        total_rows = len(daily_sales) + len(reporting_sales_wide) + len(cust_stats)
        
        # Update Watermark
        audit.update_watermark(process_name, datetime.now(), batch_id)
        
        audit.log_end(execution_id, status='SUCCESS', rows_processed=total_rows)
        print(f"  -> Aggregated Daily Sales, Wide Reporting Table ({len(reporting_sales_wide)} rows), and Customer Stats.")
        
    except Exception as e:
        audit.log_end(execution_id, status='FAILED', error_message=str(e))
        print(f"Error in Aggregation: {e}")
        raise e
    finally:
        curated_conn.close()
        gold_conn.close()

def main():
    print("--- Starting Enterprise ETL Pipeline ---")
    
    # 1. Init Audit
    audit = AuditManager()
    batch_id = audit.batch_id
    print(f"Batch ID: {batch_id}")
    
    try:
        # 2. Extract
        extract_to_raw(audit, batch_id)
        
        # 3. Transform (Separated)
        load_dimensions(audit, batch_id)
        load_facts(audit, batch_id)
        
        # 4. Aggregate
        aggregate_to_gold(audit, batch_id)
        
        print("\nPipeline Competed Successfully.")
        
    except Exception as e:
        print(f"\nPipeline Failed: {e}")

if __name__ == "__main__":
    main()
