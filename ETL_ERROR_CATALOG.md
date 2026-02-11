# 🛠️ ETL Error & Fix Catalog

This document serves as a troubleshooting guide for the Enterprise Medallion ETL pipeline. It categorizes common failures and provides step-by-step instructions for resolution.

---

## 1. Schema & Structural Errors
Errors related to the "shape" of the source data.

| Error Message / Symptom | Root Cause | Recommended Fix |
| :--- | :--- | :--- |
| `KeyError: 'customer_id'` | A required column has been renamed or removed from the source CSV. | Check the source file headers. Rename the column back to the expected name (e.g., `client_id` -> `customer_id`). |
| `Index(['...'], dtype='str')` | Pandas failed to find a column during a deduplication or join operation. | Verify the source file has not been corrupted. Compare headers against the `config.py` expectation. |
| `AttributeError: 'NoneType' ...` | A file path in `config.py` is incorrect or a database connection failed to initialize. | Verify that all database paths in `config.py` are absolute or correctly relative to the working directory. |

---

## 2. Data Quality Errors
Errors where the data exists but is "bad."

| Error Message / Symptom | Root Cause | Recommended Fix |
| :--- | :--- | :--- |
| `sqlite3.IntegrityError: UNIQUE constraint failed` | Attempted to insert a duplicate Primary Key value (e.g., same `order_id` twice). | Identify the duplicate ID in the source CSV. In a production system, implement "UPSERT" logic to handle the conflict. |
| `TypeError: '<' not supported ...` | Mixed data types in a column (e.g., a "Total" column containing both `100` and `"None"`). | Clean the source data to ensure consistent types. Use `pd.to_numeric(df['col'], errors='coerce')` to force conversion. |
| **Logic Failure**: Totals don't match | Data was lost during a join (Inner Join on a missing ID). | Change join type (e.g., from `inner` to `left`) to identify "orphan" records. Check if a dimension record is missing. |

---

## 3. System & Environmental Errors
Errors related to the infrastructure or file system.

| Error Message / Symptom | Root Cause | Recommended Fix |
| :--- | :--- | :--- |
| `sqlite3.OperationalError: database is locked` | Another process (like an external SQL editor) is holding an open transaction on the `.db` file. | Close any external database browsers (DB Browser for SQLite, etc.) and re-run the pipeline. |
| `FileNotFoundError` | The `data/Sources/...` directory or a specific CSV file is missing. | Verify your folder structure. Ensure all source CSVs are placed in the directory defined in `config.py`. |
| `ModuleNotFoundError` | A Python dependency is not installed in the current environment. | Run `pip install -r requirements.txt` to sync your virtual environment. |

---

## 4. Audit & Watermark Issues

| Error Message / Symptom | Root Cause | Recommended Fix |
| :--- | :--- | :--- |
| **Old data loading again** | The `pipeline_watermark` table failed to update during the last successful run. | Check the `audit.db`. Manually update the watermark timestamp to the "High Water Mark" of your data. |
| **Empty Logs** | The `AuditManager` cannot write to the system database. | Ensure the `audit.db` file has write permissions for the current user. |

---

## ✅ Best Practices for Resolution
1.  **Check the Audit Log first**: Always run `python3 etl/inspect_tables.py` or check the `audit.db` to see the exact error message trapped by the engine.
2.  **Use the Debugger**: Set a breakpoint at the start of the failing function (e.g., `load_dimensions`) and inspect the `df` (dataframe) variable to see the actual state of the data before it crashes.
3.  **Validate Source**: When in doubt, open the CSV file in a text editor to check for hidden characters or formatting shifts.
