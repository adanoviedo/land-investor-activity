"""
load_deeds.py
Loads raw qPublic sales CSV for a single county into the DuckDB staging table.
Usage: python src/ingest/load_deeds.py
"""
from pathlib import Path
import duckdb
import pandas as pd

# --------------------------------------------------------------------------
# Paths — all relative to project root, no hardcoded usernames
# --------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH  = BASE_DIR / "db" / "land_investor_activity.duckdb"
CSV_PATH = BASE_DIR / "data" / "raw" / "elbert" / "sales_raw.csv"

def main():
    print(f"Project root: {BASE_DIR}")
    print("Connecting to DuckDB...")
    con = duckdb.connect(str(DB_PATH))

    print(f"Loading raw CSV from: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    df["county"] = "Elbert"

    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    print(f"Columns: {df.columns.tolist()}")
    print(f"Row count: {len(df)}")

    con.execute("DROP TABLE IF EXISTS stg_deeds_raw")
    con.execute("CREATE TABLE stg_deeds_raw AS SELECT * FROM df")

    result = con.execute("SELECT COUNT(*) FROM stg_deeds_raw").fetchone()
    print(f"Rows loaded into stg_deeds_raw: {result[0]}")

    sample = con.execute("SELECT * FROM stg_deeds_raw LIMIT 3").df()
    print("\nSample rows:")
    print(sample)

    con.close()
    print("\nStaging complete.")

if __name__ == "__main__":
    main()
