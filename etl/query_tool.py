import sqlite3
import pandas as pd
import os

DB_PATHS = {
    "1": ("Raw", "data/Target/Raw/raw.db"),
    "2": ("Curated", "data/Target/Curated/curated.db"),
    "3": ("Gold", "data/Target/Gold/gold.db"),
    "4": ("Audit", "data/Target/System/audit.db")
}

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_tables(db_path):
    conn = sqlite3.connect(db_path)
    try:
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
        return tables['name'].tolist()
    finally:
        conn.close()

def show_data(db_path, table_name):
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 10", conn)
        print(f"\n--- Data: {table_name} (First 10 rows) ---")
        print(df.to_string(index=False))
        input("\nPress Enter to continue...")
    except Exception as e:
        print(f"Error: {e}")
        input("Press Enter...")
    finally:
        conn.close()

def main():
    while True:
        clear_screen()
        print("=== DATA VIEWER TOOL ===")
        print("Select a Database:")
        for key, (name, _) in DB_PATHS.items():
            print(f"{key}. {name}")
        print("Q. Quit")
        
        choice = input("\nSelect (1-4): ").strip().upper()
        if choice == 'Q':
            break
            
        if choice in DB_PATHS:
            name, path = DB_PATHS[choice]
            if not os.path.exists(path):
                print(f"❌ Database {name} not found!")
                input("Press Enter...")
                continue
                
            while True:
                clear_screen()
                print(f"=== DATABASE: {name} ===")
                tables = get_tables(path)
                
                if not tables:
                    print("(No tables found)")
                    input("Press Enter...")
                    break
                    
                for i, t in enumerate(tables, 1):
                    print(f"{i}. {t}")
                print("B. Back")
                
                t_choice = input(f"\nSelect Table (1-{len(tables)}): ").strip().upper()
                if t_choice == 'B':
                    break
                
                if t_choice.isdigit() and 1 <= int(t_choice) <= len(tables):
                    table_name = tables[int(t_choice)-1]
                    show_data(path, table_name)
                
if __name__ == "__main__":
    main()
