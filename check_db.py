"""Check if taxi data was loaded into DuckDB."""
import duckdb
import os

db_path = "taxi_pipeline.duckdb"

if os.path.exists(db_path):
    print(f"DuckDB file exists: {db_path}")
    conn = duckdb.connect(db_path)
    
    # List all tables
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema != 'information_schema'"
    ).fetchall()
    print(f"\nTables in database: {tables}")
    
    if tables:
        for table in tables:
            table_name = table[0]
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"\nTable '{table_name}': {count} rows")
    
    conn.close()
else:
    print(f"DuckDB database does not exist: {db_path}")
