"""
run_all_counties.py
Runs the full pipeline (ingest, clean, score, IAI) for all 5 counties
and saves results to DuckDB and CSV outputs.
Usage: python src/ingest/run_all_counties.py
"""
from pathlib import Path
from datetime import datetime
import duckdb
import pandas as pd

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
BASE_DIR     = Path(__file__).resolve().parent.parent.parent
DB_PATH      = BASE_DIR / "db" / "land_investor_activity.duckdb"
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
CSV_OUT_DIR  = BASE_DIR / "outputs" / "csv"

COUNTIES = ["elbert", "lincoln", "wilkes", "warren", "mcduffie"]

WEIGHTS = {
    "low_consideration":   0.20,
    "rural_acreage":       0.10,
    "no_structure":        0.10,
    "quick_flip":          0.25,
    "subdivision_cluster": 0.10,
}

# --------------------------------------------------------------------------
# Pipeline functions
# --------------------------------------------------------------------------
def load_county(county):
    path = RAW_DATA_DIR / county / "sales_raw.csv"
    df = pd.read_csv(path)
    df["county"] = county.capitalize()

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    core_cols = [
        "parcel_id", "address", "sale_date", "sale_price",
        "qualified_sales", "reason", "acres", "parcel__class",
        "year__built", "square_ft", "price_per__square_ft",
        "neighborhood", "county"
    ]
    df = df[[c for c in core_cols if c in df.columns]]
    return df


def clean_county(df):
    df = df.rename(columns={
        "parcel__class":        "parcel_class",
        "year__built":          "year_built",
        "price_per__square_ft": "price_per_sqft"
    })

    df["sale_price"] = (
        df["sale_price"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    df["sale_price"] = pd.to_numeric(df["sale_price"], errors="coerce")
    df["sale_date"]  = pd.to_datetime(df["sale_date"], errors="coerce")
    df["parcel_id"]  = df["parcel_id"].astype(str).str.strip()
    df["key_value"]  = df["parcel_id"].str.replace(" ", "+")

    df["no_structure"]      = (df["year_built"].isna() & df["square_ft"].isna()).astype(int)
    df["rural_acreage"]     = ((df["acres"] >= 1) & (df["acres"] <= 40)).astype(int)
    df["low_consideration"] = (df["sale_price"] < 5000).astype(int)

    df["neighborhood"] = df["neighborhood"].astype(str).str.strip().str.upper()

    for col in ["price_per_sqft", "square_ft", "year_built"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    return df


def score_county(df):
    flip_counts = df.groupby("parcel_id")["sale_date"].count()
    multi_sale  = flip_counts[flip_counts > 1].index.tolist()
    df["quick_flip"] = df["parcel_id"].isin(multi_sale).astype(int)

    nb_counts = df.groupby("neighborhood")["parcel_id"].count()
    active_nb = nb_counts[nb_counts >= 5].index.tolist()
    df["subdivision_cluster"] = df["neighborhood"].isin(active_nb).astype(int)

    def compute_score(row):
        score = sum(WEIGHTS[f] for f in WEIGHTS if row.get(f, 0) == 1)
        return min(round(score, 3), 1.0)

    df["investor_score"] = df.apply(compute_score, axis=1)

    def classify(score):
        if score >= 0.60: return "LIKELY_INVESTOR"
        if score >= 0.35: return "POSSIBLE_INVESTOR"
        return "LIKELY_RETAIL"

    df["classification"] = df["investor_score"].apply(classify)
    return df


def compute_iai(df, county):
    investor_df             = df[df["classification"] != "LIKELY_RETAIL"]
    unique_investor_parcels = investor_df["parcel_id"].nunique()
    date_range_months       = (df["sale_date"].max() - df["sale_date"].min()).days / 30
    transaction_velocity    = len(df) / max(date_range_months, 1)
    last_activity           = df["sale_date"].max()
    days_since_last         = (pd.Timestamp.now() - last_activity).days
    recency_score           = max(0, 1 - (days_since_last / 365))
    clustered               = df[df["subdivision_cluster"] == 1]
    subdivision_conc        = len(clustered) / max(len(df), 1)
    investor_count_score    = min(unique_investor_parcels / 50, 1.0)
    velocity_score          = min(transaction_velocity / 10, 1.0)

    iai = round((
        0.35 * investor_count_score +
        0.25 * velocity_score +
        0.20 * recency_score +
        0.20 * subdivision_conc
    ) * 100, 1)

    return {
        "county":                         f"{county.capitalize()}, GA",
        "iai_score":                      iai,
        "total_transactions":             len(df),
        "investor_flagged":               len(investor_df),
        "investor_pct":                   round(len(investor_df) / max(len(df), 1) * 100, 1),
        "quick_flip_parcels":             int(df["quick_flip"].sum()),
        "unique_investor_parcels":        unique_investor_parcels,
        "transaction_velocity_per_month": round(transaction_velocity, 1),
        "last_activity":                  last_activity.strftime("%Y-%m-%d"),
        "recency_score":                  round(recency_score, 3),
        "subdivision_concentration_pct":  round(subdivision_conc * 100, 1),
        "avg_sale_price":                 round(df["sale_price"].mean(), 0),
        "avg_acres":                      round(df["acres"].mean(), 1),
        "analysis_date":                  datetime.now().strftime("%Y-%m-%d"),
    }

# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    print(f"Project root: {BASE_DIR}")
    con = duckdb.connect(str(DB_PATH))

    all_facts = []
    all_iai   = []

    for county in COUNTIES:
        print(f"\n{'='*50}")
        print(f"  Processing {county.upper()} county...")
        print(f"{'='*50}")

        df = load_county(county)
        print(f"  Loaded:  {len(df)} rows")

        df = clean_county(df)
        df = score_county(df)

        print(f"  Scored:  {len(df)} transactions")
        print(f"  Flags:   {int(df['quick_flip'].sum())} quick flips | "
              f"{int(df['subdivision_cluster'].sum())} clustered")
        print(f"  Classes: {df['classification'].value_counts().to_dict()}")

        summary = compute_iai(df, county)
        print(f"  IAI Score: {summary['iai_score']} / 100")

        all_facts.append(df)
        all_iai.append(summary)

    facts_df = pd.concat(all_facts, ignore_index=True)
    iai_df   = pd.DataFrame(all_iai).sort_values("iai_score", ascending=False).reset_index(drop=True)
    iai_df.insert(0, "rank", range(1, len(iai_df) + 1))

    con.execute("DROP TABLE IF EXISTS fact_transactions_all")
    con.execute("CREATE TABLE fact_transactions_all AS SELECT * FROM facts_df")
    con.execute("DROP TABLE IF EXISTS mart_county_rankings")
    con.execute("CREATE TABLE mart_county_rankings AS SELECT * FROM iai_df")

    CSV_OUT_DIR.mkdir(parents=True, exist_ok=True)
    iai_df.to_csv(CSV_OUT_DIR / "county_rankings.csv", index=False)
    facts_df.to_csv(CSV_OUT_DIR / "all_transactions.csv", index=False)

    con.close()

    print(f"\n{'='*50}")
    print("  COUNTY RANKINGS — INVESTOR ACTIVITY INDEX")
    print(f"{'='*50}")
    print(f"{'Rank':<6}{'County':<16}{'IAI':>6}{'Deals':>7}{'Flagged':>9}{'Flips':>7}{'Avg $':>12}")
    print("-" * 56)
    for _, row in iai_df.iterrows():
        print(
            f"{int(row['rank']):<6}"
            f"{row['county']:<16}"
            f"{row['iai_score']:>6}"
            f"{int(row['total_transactions']):>7}"
            f"{int(row['investor_flagged']):>9}"
            f"{int(row['quick_flip_parcels']):>7}"
            f"${row['avg_sale_price']:>10,.0f}"
        )
    print(f"{'='*50}")
    print("\nAll data saved. Ready to generate comparison report.")

if __name__ == "__main__":
    main()
