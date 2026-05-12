# Land Investor Activity Estimator

A Python + DuckDB data pipeline that analyzes public Georgia deed records to identify
land investor activity by county and scores each county with an **Investor Activity Index (IAI)**.

Built as a proof of concept to demonstrate data engineering capability for the LandGeek land investing team.

---

## What It Does

1. Ingests raw qPublic county sales CSV files for 5 Northeast Georgia counties
2. Cleans and normalizes the data (prices, dates, flags)
3. Scores every transaction using 5 investor-detection heuristics
4. Computes a composite IAI score for each county
5. Generates polished HTML and PDF reports for presentation

---

## Counties Analyzed

| County | Transactions | IAI Score | Activity Level |
|---|---|---|---|
| Elbert, GA | 112 | 35.9 | High |
| Wilkes, GA | 59 | 28.5 | Moderate |
| McDuffie, GA | 14 | 21.1 | Moderate |
| Lincoln, GA | 21 | 13.0 | Low |
| Warren, GA | 2 | 0.2 | Low |

---

## Project Structure

```
land_investor_activity/
├── data/
│   └── raw/                  # Raw CSVs from qPublic (one folder per county)
│       ├── elbert/
│       ├── lincoln/
│       ├── wilkes/
│       ├── warren/
│       └── mcduffie/
├── db/                       # DuckDB database file (auto-generated)
├── outputs/                  # Generated reports and CSVs
│   ├── csv/
│   ├── elbert_county_report.html
│   ├── elbert_county_report.pdf
│   ├── county_comparison_report.html
│   └── county_comparison_report.pdf
├── src/
│   ├── ingest/
│   │   ├── load_deeds.py          # Load single county CSV into DuckDB
│   │   ├── clean_deeds.py         # Clean staging table
│   │   └── run_all_counties.py    # Full pipeline for all 5 counties
│   ├── score/
│   │   └── score_transactions.py  # Score individual transactions
│   ├── mart/
│   │   └── build_marts.py         # Build IAI summary tables
│   └── report/
│       ├── generate_html_report.py       # Elbert County single report
│       └── generate_comparison_report.py # 5-county comparison report
├── sql/                      # SQL scripts (for reference/future use)
├── tests/                    # Test scripts
├── requirements.txt
└── README.md
```

---

## Setup

### Requirements
- Python 3.10 or higher
- pip

### Install dependencies

```bash
pip install -r requirements.txt
```

### Get the data

Download the `sales_raw.csv` file for each county from qPublic using these settings:

- **Sale Type:** Land Only
- **Property Type:** Agriculture
- **Date Range:** 01/01/2022 to 12/31/2024

Save each file to:
```
data/raw/{county_name}/sales_raw.csv
```

qPublic links:
- [Elbert County](https://qpublic.schneidercorp.com/Application.aspx?AppID=667&LayerID=11830&PageTypeID=2&PageID=5730)
- [Lincoln County](https://qpublic.schneidercorp.com/Application.aspx?AppID=669&LayerID=11838&PageTypeID=2&PageID=5756)
- [Wilkes County](https://qpublic.schneidercorp.com/Application.aspx?AppID=800&LayerID=14348&PageTypeID=2&PageID=7013)
- [Warren County](https://qpublic.schneidercorp.com/Application.aspx?AppID=692&LayerID=12026&PageTypeID=2&PageID=5976)
- [McDuffie County](https://qpublic.schneidercorp.com/Application.aspx?AppID=676&LayerID=11874&PageTypeID=2&PageID=5820)

---

## Run the Pipeline

### Step 1 — Run all counties and build the database

```bash
python src/ingest/run_all_counties.py
```

This processes all 5 counties and saves results to DuckDB.

### Step 2 — Generate the comparison report

```bash
python src/report/generate_comparison_report.py
```

Opens as `outputs/county_comparison_report.html` and `outputs/county_comparison_report.pdf`.

### Step 3 — Generate the Elbert County detail report (optional)

```bash
python src/report/generate_html_report.py
```

---

## Investor Activity Index (IAI)

The IAI is a composite score (0–100) built from 5 heuristic flags:

| Flag | Weight | Description |
|---|---|---|
| `low_consideration` | 20% | Sale price under $5,000 |
| `quick_flip` | 25% | Same parcel sold 2+ times in the window |
| `rural_acreage` | 10% | Parcel between 1 and 40 acres |
| `no_structure` | 10% | No year built or square footage (vacant land) |
| `subdivision_cluster` | 10% | Neighborhood with 5+ recorded sales |

**5 additional flags are planned** (pending GSCCCA deed data):
`llc_grantee`, `non_local_buyer`, `repeat_buyer`, `price_spread`, `quit_claim_source`

---

## Data Sources

- **qPublic** — County sales records (public, free, no login required for CSV export)
- **GSCCCA** — Georgia deed records with grantor/grantee names (free limited account available)

All data used in this project is publicly available government property records.

---

## Next Steps

- [ ] Integrate GSCCCA deed data to activate 5 remaining heuristic flags
- [ ] Expand to all 159 Georgia counties
- [ ] Add a `run.py` master script for one-command refresh
- [ ] Schedule quarterly data refresh
- [ ] Validate top investor-flagged parcels against public LLC registries

---

## Author

Built as a data engineering proof of concept.
Analysis window: January 2022 – December 2024.
