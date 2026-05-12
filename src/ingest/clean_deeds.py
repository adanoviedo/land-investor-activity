"""
clean_deeds.py
Cleans the raw staging table and writes to clean_deeds in DuckDB.
Usage: python src/ingest/clean_deeds.py
"""
from pathlib import Path
import duckdb
import pandas as pd

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH  = BASE_DIR / "db" / "land_investor_activity.duckdb"

def main():
    print(f"Project root: {BASE_DIR}")
    print("Connecting to DuckDB...")
    con = duckdb.connect(str(DB_PATH))

    print("Cleaning deed data...")
    df = con.execute("SELECT * FROM stg_deeds_raw").df()

    # Fix double underscore column names
    df = df.rename(columns={
        "parcel__class":        "parcel_class",
        "year__built":          "year_built",
        "price_per__square_ft": "price_per_sqft"
    })

    # Clean sale_price
    df["sale_price"] = (
        df["sale_price"]
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
        .astype(float)
    )

    # Clean price_per_sqft
    df["price_per_sqft"] = (
        df["price_per_sqft"]
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
        .astype(float)
    )

    # Parse dates
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")

    # Clean parcel ID and build key value
    df["parcel_id"] = df["parcel_id"].str.strip()
    df["key_value"] = df["parcel_id"].str.replace(" ", "+")

    # Flags
    df["no_structure"]      = (df["year_built"].isna() & df["square_ft"].isna()).astype(int)
    df["rural_acreage"]     = ((df["acres"] >= 1) & (df["acres"] <= 40)).astype(int)
    df["low_consideration"] = (df["sale_price"] < 5000).astype(int)

    # Clean neighborhood
    df["neighborhood"] = df["neighborhood"].str.strip().str.upper()

    # Drop columns no longer needed
    df = df.drop(columns=["price_per_sqft", "square_ft", "year_built"])

    print(f"Cleaned row count: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    print(f"\nFlag summary:")
    print(f"  no_structure:      {df['no_structure'].sum()} of {len(df)}")
    print(f"  rural_acreage:     {df['rural_acreage'].sum()} of {len(df)}")
    print(f"  low_consideration: {df['low_consideration'].sum()} of {len(df)}")

    con.execute("DROP TABLE IF EXISTS clean_deeds")
    con.execute("CREATE TABLE clean_deeds AS SELECT * FROM df")

    sample = con.execute("SELECT * FROM clean_deeds LIMIT 3").df()
    print("\nSample cleaned rows:")
    print(sample[["parcel_id", "sale_date", "sale_price", "acres",
                  "no_structure", "rural_acreage", "low_consideration"]])

    con.close()
    print("\nCleaning complete.")

if __name__ == "__main__":
    main()
