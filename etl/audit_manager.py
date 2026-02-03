import sqlite3
import uuid
from datetime import datetime
import config
import os

class AuditManager:
    def __init__(self):
        self.db_path = config.AUDIT_DB_PATH
        self._ensure_audit_db()
        self.batch_id = str(uuid.uuid4())
        
    def _ensure_audit_db(self):
        """Creates the audit table if it doesn't exist."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Pipeline Log Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_execution_log (
                execution_id TEXT PRIMARY KEY,
                batch_id TEXT,
                process_name TEXT,
                layer TEXT,
                status TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                rows_processed INTEGER,
                error_message TEXT
            );
        """)
        
        # Watermark Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_watermark (
                process_name TEXT PRIMARY KEY,
                last_processed_timestamp TIMESTAMP,
                last_batch_id TEXT
            );
        """)
        
        conn.commit()
        conn.close()

    def log_start(self, process_name, layer):
        """Logs the start of a pipeline step."""
        execution_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_execution_log 
            (execution_id, batch_id, process_name, layer, status, start_time)
            VALUES (?, ?, ?, ?, 'STARTED', ?)
        """, (execution_id, self.batch_id, process_name, layer, start_time))
        conn.commit()
        conn.close()
        
        return execution_id

    def log_end(self, execution_id, status='SUCCESS', rows_processed=0, error_message=None):
        """Logs the completion (success or failure) of a pipeline step."""
        end_time = datetime.now()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE pipeline_execution_log
            SET status = ?, end_time = ?, rows_processed = ?, error_message = ?
            WHERE execution_id = ?
        """, (status, end_time, rows_processed, error_message, execution_id))
        conn.commit()
        conn.close()

    def get_watermark(self, process_name):
        """Retrieves the last successful timestamp for a process."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        # Initialize table if manual queries messed it up
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_watermark'")
        if not cursor.fetchone():
             return "1900-01-01 00:00:00"
             
        cursor.execute("SELECT last_processed_timestamp FROM pipeline_watermark WHERE process_name = ?", (process_name,))
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            return result[0]
        else:
            return "1900-01-01 00:00:00" # Default for initial load

    def update_watermark(self, process_name, timestamp, batch_id):
        """Updates the watermark for a process."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO pipeline_watermark (process_name, last_processed_timestamp, last_batch_id)
            VALUES (?, ?, ?)
        """, (process_name, timestamp, batch_id))
        conn.commit()
        conn.close()
