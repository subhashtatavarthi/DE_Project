# Incremental Load & SCD Strategy Notes

## 1. Incremental Load Logic
Unlike the Initial Full Load, the **Incremental Load** is designed to save time and resources by only processing what has changed.

### How it works:
1.  **High Watermark**: The system checks the `pipeline_watermark` table in `audit.db` for the `last_processed_timestamp`.
2.  **Delta Extraction**: It filter source files (or database rows) to find only those created/modified *after* that timestamp.
3.  **Merge/Upsert**: New data is merged into the existing `Curated` and `Gold` layers.
4.  **Log Success**: A new entry is added to `pipeline_execution_log`, and the Watermark is updated to "Now".

---

## 2. SCD (Slowly Changing Dimensions) Strategy
Dimensions are tables that describe your data (Customers, Products). We chose **SCD Type 1 (Overwrite)** for the initial implementation.

### Why SCD Type 1?
*   **Goal**: Keep the data "current."
*   **Action**: When a customer's attribute (like city or phone number) changes in the source, we update the existing record in `dim_customers`.
*   **Rationale**: Businesses often need the *most recent* contact info more than they need a historical list of every address a customer has ever had.
*   **Record Count**: Your `dim_customers` stays at **500 rows** (matches unique IDs), prevent duplicate entries.

---

## 3. Observability & State (Audit Tables)
We use two distinct tables in `audit.db` to manage the pipeline's health.

### 🟢 `pipeline_watermark` (The State)
*   **Role**: Stores the "Bookmark".
*   **Record Count**: **4 rows** (One for each major pipeline step).
*   **Logic**: It ensures that if the pipeline runs today, it knows exactly where it left off yesterday.

### 📜 `pipeline_execution_log` (The Audit)
*   **Role**: Stores the "History".
*   **Record Count**: **4 rows** per successful full run (grows over time).
*   **Logic**: It records performance metrics (rows processed, success/failure) for every execution. This is your first stop for debugging errors.

---

## 4. Final Table Inventory (Incremental Focus)
When you run an incremental load, you expect these many rows to be *added* or *updated*:

| Layer | Table | Expected Change |
| :--- | :--- | :--- |
| **Raw** | All Tables | + Rows arriving since last run. |
| **Curated** | `fact_orders` | + New orders only. |
| **Curated** | `dim_customers` | Updated if attributes changed. |
| **Gold** | `sales_summary` | Re-calculated for the current period. |
