"""
generate_html_report.py
Generates a polished HTML and PDF report for Elbert County.
Usage: python src/report/generate_html_report.py
"""
from pathlib import Path
from datetime import datetime
import duckdb
import pandas as pd

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
BASE_DIR  = Path(__file__).resolve().parent.parent.parent
DB_PATH   = BASE_DIR / "db" / "land_investor_activity.duckdb"
HTML_PATH = BASE_DIR / "outputs" / "elbert_county_report.html"
PDF_PATH  = BASE_DIR / "outputs" / "elbert_county_report.pdf"

# --------------------------------------------------------------------------
# Load data
# --------------------------------------------------------------------------
def load_data():
    con = duckdb.connect(str(DB_PATH))
    iai       = con.execute("SELECT * FROM mart_county_iai").df().iloc[0]
    neighbors = con.execute("SELECT * FROM mart_neighborhood_iai ORDER BY sales DESC").df()
    top_flips = con.execute("""
        SELECT parcel_id, sale_date, sale_price, acres, neighborhood, investor_score
        FROM fact_transactions
        WHERE quick_flip = 1
        ORDER BY investor_score DESC, sale_date DESC
        LIMIT 10
    """).df()
    con.close()
    return iai, neighbors, top_flips

# --------------------------------------------------------------------------
# Build HTML
# --------------------------------------------------------------------------
def build_neighborhood_rows(df):
    rows = ""
    for _, row in df.iterrows():
        inv = int(row["investor_flagged"])
        highlight = ' class="highlight"' if inv > 0 else ""
        rows += f"""
        <tr{highlight}>
            <td>{row['neighborhood']}</td>
            <td>{int(row['sales'])}</td>
            <td>{"<span class='flag'>" + str(inv) + "</span>" if inv > 0 else "0"}</td>
            <td>${row['avg_price']:,.0f}</td>
            <td>{row['avg_acres']} ac</td>
        </tr>"""
    return rows

def build_flip_rows(df):
    rows = ""
    for _, row in df.iterrows():
        score = row["investor_score"]
        score_class = "score-high" if score >= 0.45 else "score-mid"
        rows += f"""
        <tr>
            <td><code>{row['parcel_id'].strip()}</code></td>
            <td>{str(row['sale_date'])[:10]}</td>
            <td>${row['sale_price']:,.0f}</td>
            <td>{row['acres']} ac</td>
            <td>{row['neighborhood']}</td>
            <td><span class="{score_class}">{score}</span></td>
        </tr>"""
    return rows

def build_html(iai, neighborhoods, top_flips):
    gen_date = datetime.now().strftime("%B %d, %Y")
    nb_rows  = build_neighborhood_rows(neighborhoods)
    fp_rows  = build_flip_rows(top_flips)
    score    = float(iai["iai_score"])
    score_color = "#2d6a4f" if score >= 60 else "#b5862a" if score >= 35 else "#c0392b"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Elbert County — Land Investor Activity Report</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&family=DM+Mono&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  :root {{
    --ink: #1a1a2e; --ink-light: #4a4a6a; --paper: #faf9f6; --cream: #f2efe8;
    --gold: #b5862a; --gold-light: #f0e4c4; --green: #2d6a4f; --red: #c0392b;
    --border: #ddd8cc; --shadow: 0 2px 16px rgba(26,26,46,0.08);
  }}
  body {{ font-family: 'DM Sans', sans-serif; background: var(--paper); color: var(--ink); font-size: 15px; line-height: 1.6; }}
  header {{ background: var(--ink); color: #fff; padding: 56px 64px 48px; position: relative; overflow: hidden; }}
  header::before {{ content: ''; position: absolute; top: -60px; right: -60px; width: 320px; height: 320px; border-radius: 50%; background: rgba(181,134,42,0.12); }}
  .header-label {{ font-size: 11px; font-weight: 600; letter-spacing: 3px; text-transform: uppercase; color: var(--gold); margin-bottom: 12px; }}
  header h1 {{ font-family: 'DM Serif Display', serif; font-size: 42px; font-weight: 400; line-height: 1.15; margin-bottom: 8px; }}
  .header-sub {{ color: rgba(255,255,255,0.55); font-size: 14px; font-weight: 300; }}
  .header-meta {{ margin-top: 32px; display: flex; gap: 40px; flex-wrap: wrap; }}
  .meta-item {{ display: flex; flex-direction: column; gap: 2px; }}
  .meta-label {{ font-size: 10px; letter-spacing: 2px; text-transform: uppercase; color: rgba(255,255,255,0.4); }}
  .meta-value {{ font-size: 13px; color: rgba(255,255,255,0.85); font-weight: 500; }}
  main {{ max-width: 960px; margin: 0 auto; padding: 48px 40px 80px; }}
  section {{ margin-bottom: 56px; }}
  .section-label {{ font-size: 10px; font-weight: 600; letter-spacing: 3px; text-transform: uppercase; color: var(--gold); margin-bottom: 6px; }}
  h2 {{ font-family: 'DM Serif Display', serif; font-size: 26px; font-weight: 400; margin-bottom: 24px; padding-bottom: 12px; border-bottom: 1px solid var(--border); }}
  .score-card {{ background: var(--ink); color: #fff; border-radius: 16px; padding: 40px 48px; display: flex; align-items: center; gap: 48px; margin-bottom: 32px; box-shadow: var(--shadow); }}
  .score-number {{ font-family: 'DM Serif Display', serif; font-size: 80px; line-height: 1; color: {score_color}; flex-shrink: 0; }}
  .score-denom {{ font-size: 28px; color: rgba(255,255,255,0.3); }}
  .score-details h3 {{ font-family: 'DM Serif Display', serif; font-size: 22px; font-weight: 400; margin-bottom: 8px; }}
  .score-details p {{ color: rgba(255,255,255,0.6); font-size: 14px; line-height: 1.7; max-width: 460px; }}
  .confidence-badge {{ display: inline-block; margin-top: 12px; padding: 4px 12px; border-radius: 20px; background: rgba(181,134,42,0.2); color: var(--gold); font-size: 11px; font-weight: 600; letter-spacing: 1.5px; text-transform: uppercase; }}
  .metrics-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; }}
  .metric-card {{ background: #fff; border: 1px solid var(--border); border-radius: 12px; padding: 24px 20px; box-shadow: var(--shadow); }}
  .metric-card.accent {{ border-top: 3px solid var(--gold); }}
  .metric-value {{ font-family: 'DM Serif Display', serif; font-size: 28px; color: var(--ink); line-height: 1.1; margin-bottom: 4px; }}
  .metric-label {{ font-size: 12px; color: var(--ink-light); font-weight: 500; }}
  .table-wrap {{ background: #fff; border: 1px solid var(--border); border-radius: 12px; overflow: hidden; box-shadow: var(--shadow); }}
  table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
  thead {{ background: var(--ink); color: #fff; }}
  thead th {{ padding: 14px 18px; text-align: left; font-size: 11px; font-weight: 600; letter-spacing: 1.5px; text-transform: uppercase; }}
  tbody tr {{ border-bottom: 1px solid var(--cream); }}
  tbody tr:last-child {{ border-bottom: none; }}
  tbody tr:hover {{ background: var(--cream); }}
  tbody tr.highlight {{ background: #fffbf0; }}
  td {{ padding: 13px 18px; color: var(--ink); vertical-align: middle; }}
  .flag {{ display: inline-block; background: var(--gold-light); color: var(--gold); font-weight: 600; font-size: 12px; padding: 2px 10px; border-radius: 20px; }}
  .score-high {{ display: inline-block; background: #d4edda; color: var(--green); font-weight: 600; font-size: 12px; padding: 2px 10px; border-radius: 20px; }}
  .score-mid {{ display: inline-block; background: var(--gold-light); color: var(--gold); font-weight: 600; font-size: 12px; padding: 2px 10px; border-radius: 20px; }}
  code {{ font-family: 'DM Mono', monospace; font-size: 12px; background: var(--cream); padding: 2px 6px; border-radius: 4px; color: var(--ink-light); }}
  .findings-list {{ list-style: none; display: flex; flex-direction: column; gap: 16px; }}
  .findings-list li {{ display: flex; gap: 16px; align-items: flex-start; background: #fff; border: 1px solid var(--border); border-radius: 10px; padding: 20px 24px; box-shadow: var(--shadow); }}
  .finding-num {{ font-family: 'DM Serif Display', serif; font-size: 28px; color: var(--gold); line-height: 1; flex-shrink: 0; width: 32px; }}
  .finding-text {{ font-size: 14px; line-height: 1.7; color: var(--ink-light); padding-top: 4px; }}
  .finding-text strong {{ color: var(--ink); }}
  .method-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  .method-card {{ background: #fff; border: 1px solid var(--border); border-radius: 10px; padding: 20px 24px; box-shadow: var(--shadow); }}
  .method-card h4 {{ font-size: 12px; font-weight: 600; letter-spacing: 1.5px; text-transform: uppercase; margin-bottom: 12px; }}
  .method-card.applied h4 {{ color: var(--green); }}
  .method-card.missing h4 {{ color: var(--ink-light); }}
  .method-card ul {{ list-style: none; display: flex; flex-direction: column; gap: 8px; }}
  .method-card li {{ font-size: 13px; color: var(--ink-light); display: flex; gap: 8px; align-items: flex-start; }}
  .method-card li::before {{ content: 'checkmark'; color: var(--green); font-weight: 700; flex-shrink: 0; }}
  .method-card.applied li::before {{ content: '✓'; }}
  .method-card.missing li::before {{ content: '○'; color: var(--border); }}
  footer {{ background: var(--cream); border-top: 1px solid var(--border); padding: 24px 64px; font-size: 12px; color: var(--ink-light); display: flex; justify-content: space-between; align-items: center; }}
  footer strong {{ color: var(--ink); }}
</style>
</head>
<body>
<header>
  <div class="header-label">Data Engineering Proof of Concept</div>
  <h1>Elbert County, GA<br>Land Investor Activity Report</h1>
  <p class="header-sub">County-Level Investor Activity Index — LandGeek Market Intelligence</p>
  <div class="header-meta">
    <div class="meta-item"><span class="meta-label">Generated</span><span class="meta-value">{gen_date}</span></div>
    <div class="meta-item"><span class="meta-label">Analysis Window</span><span class="meta-value">Jan 2022 – Dec 2024</span></div>
    <div class="meta-item"><span class="meta-label">Data Source</span><span class="meta-value">qPublic — Elbert County Sales Records</span></div>
    <div class="meta-item"><span class="meta-label">Total Transactions</span><span class="meta-value">{int(iai['total_transactions'])} recorded sales</span></div>
  </div>
</header>
<main>
  <section>
    <div class="section-label">Primary Output</div>
    <h2>Investor Activity Index</h2>
    <div class="score-card">
      <div class="score-number">{iai['iai_score']}<span class="score-denom">/100</span></div>
      <div class="score-details">
        <h3>Moderate Investor Activity Detected</h3>
        <p>The IAI is a composite score measuring investor activity intensity relative to a benchmark.
        Elbert County shows meaningful signals concentrated in unincorporated rural areas.
        This score is conservative — adding LLC buyer and out-of-state buyer data would likely push it to 55–70.</p>
        <span class="confidence-badge">Confidence: Medium</span>
      </div>
    </div>
  </section>
  <section>
    <div class="section-label">At a Glance</div>
    <h2>Key Metrics</h2>
    <div class="metrics-grid">
      <div class="metric-card accent"><div class="metric-value">{int(iai['total_transactions'])}</div><div class="metric-label">Total Transactions<br>2022–2024</div></div>
      <div class="metric-card accent"><div class="metric-value">{int(iai['investor_flagged'])}</div><div class="metric-label">Investor-Flagged<br>({iai['investor_pct']}% of all sales)</div></div>
      <div class="metric-card accent"><div class="metric-value">{int(iai['quick_flip_parcels'])}</div><div class="metric-label">Quick Flip Parcels<br>Sold 2+ times</div></div>
      <div class="metric-card accent"><div class="metric-value">{iai['transaction_velocity_per_month']}</div><div class="metric-label">Deals Per Month<br>Avg velocity</div></div>
      <div class="metric-card"><div class="metric-value">${iai['avg_sale_price']:,.0f}</div><div class="metric-label">Average Sale Price</div></div>
      <div class="metric-card"><div class="metric-value">{iai['avg_acres']} ac</div><div class="metric-label">Average Parcel Size</div></div>
      <div class="metric-card"><div class="metric-value">{iai['subdivision_concentration_pct']}%</div><div class="metric-label">Subdivision Concentration</div></div>
      <div class="metric-card"><div class="metric-value">{iai['last_activity']}</div><div class="metric-label">Last Recorded Activity</div></div>
    </div>
  </section>
  <section>
    <div class="section-label">Geographic Breakdown</div>
    <h2>Sales by Neighborhood</h2>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Neighborhood</th><th>Total Sales</th><th>Investor Flagged</th><th>Avg Sale Price</th><th>Avg Parcel Size</th></tr></thead>
        <tbody>{nb_rows}</tbody>
      </table>
    </div>
  </section>
  <section>
    <div class="section-label">Strongest Signals</div>
    <h2>Quick Flip Parcels</h2>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Parcel ID</th><th>Sale Date</th><th>Sale Price</th><th>Acres</th><th>Neighborhood</th><th>Investor Score</th></tr></thead>
        <tbody>{fp_rows}</tbody>
      </table>
    </div>
  </section>
  <section>
    <div class="section-label">Analysis</div>
    <h2>Key Findings</h2>
    <ul class="findings-list">
      <li><div class="finding-num">1</div><div class="finding-text"><strong>76 of 112 sales (68%) are unincorporated rural land</strong> averaging 29.8 acres — precisely the profile targeted by LandGeek-style investors.</div></li>
      <li><div class="finding-num">2</div><div class="finding-text"><strong>20 parcels sold multiple times</strong> in a 3-year window, representing active market turnover consistent with investor flip activity.</div></li>
      <li><div class="finding-num">3</div><div class="finding-text"><strong>LAKE and OTHER neighborhoods show zero investor flags</strong> despite meaningful sales volume — recreational or residential buyers dominate those areas.</div></li>
      <li><div class="finding-num">4</div><div class="finding-text"><strong>Average price of $164,869 across 32.8 acres</strong> implies roughly $5,027 per acre — within the range where LandGeek-style margins are achievable at 25–40 cents on the dollar.</div></li>
      <li><div class="finding-num">5</div><div class="finding-text"><strong>Parcel 030 007B sold for $250K in 2022 and $475K in 2024</strong> — a $225,000 gain in two years on 1.5 acres. The clearest flip pattern in the dataset.</div></li>
    </ul>
  </section>
  <section>
    <div class="section-label">Transparency</div>
    <h2>Methodology</h2>
    <div class="method-grid">
      <div class="method-card applied">
        <h4>Flags Applied (5 of 10)</h4>
        <ul>
          <li>no_structure — vacant land confirmed</li>
          <li>rural_acreage — parcel between 1 and 40 acres</li>
          <li>low_consideration — sale price under $5,000</li>
          <li>quick_flip — same parcel sold 2+ times</li>
          <li>subdivision_cluster — neighborhood with 5+ sales</li>
        </ul>
      </div>
      <div class="method-card missing">
        <h4>Flags Pending (5 of 10)</h4>
        <ul>
          <li>llc_grantee — buyer is a business entity</li>
          <li>non_local_buyer — out-of-state buyer address</li>
          <li>repeat_buyer — same buyer on 3+ parcels</li>
          <li>price_spread — resale is 3x acquisition price</li>
          <li>quit_claim_source — acquired via quit claim deed</li>
        </ul>
      </div>
    </div>
  </section>
</main>
<footer>
  <div><strong>County-Level Land Investor Activity Estimator</strong> — Data Engineering Proof of Concept</div>
  <div>Generated {gen_date} · Elbert County, GA · 2022–2024</div>
</footer>
</body>
</html>"""
    return html

# --------------------------------------------------------------------------
# Build PDF
# --------------------------------------------------------------------------
def build_pdf(iai, neighborhoods, top_flips):
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable

    INK   = colors.HexColor("#1a1a2e")
    GOLD  = colors.HexColor("#b5862a")
    CREAM = colors.HexColor("#f2efe8")
    GREEN = colors.HexColor("#2d6a4f")
    LGOLD = colors.HexColor("#f0e4c4")
    WHITE = colors.white

    def ps(name, **kw): return ParagraphStyle(name, **kw)
    S_label = ps("lbl",  fontName="Helvetica-Bold", fontSize=8,  textColor=GOLD, spaceAfter=2,  leading=10)
    S_h1    = ps("h1",   fontName="Helvetica-Bold", fontSize=20, textColor=INK,  spaceAfter=4,  leading=24)
    S_h2    = ps("h2",   fontName="Helvetica-Bold", fontSize=13, textColor=INK,  spaceAfter=8,  spaceBefore=16, leading=16)
    S_body  = ps("body", fontName="Helvetica",      fontSize=9,  textColor=colors.HexColor("#4a4a6a"), leading=14, spaceAfter=5)
    S_small = ps("sm",   fontName="Helvetica",      fontSize=8,  textColor=colors.HexColor("#4a4a6a"), leading=12, spaceAfter=4)

    doc   = SimpleDocTemplate(str(PDF_PATH), pagesize=letter,
                leftMargin=0.75*inch, rightMargin=0.75*inch,
                topMargin=0.75*inch,  bottomMargin=0.75*inch)
    story = []
    gen_date = datetime.now().strftime("%B %d, %Y")

    story.append(Paragraph("DATA ENGINEERING PROOF OF CONCEPT", S_label))
    story.append(Paragraph("Elbert County, GA — Land Investor Activity Report", S_h1))
    story.append(Paragraph(f"Generated: {gen_date}  |  Window: Jan 2022 - Dec 2024  |  Source: qPublic Elbert County", S_small))
    story.append(HRFlowable(width="100%", thickness=1, color=GOLD, spaceAfter=14))

    story.append(Paragraph("PRIMARY OUTPUT", S_label))
    story.append(Paragraph("Investor Activity Index", S_h2))

    score_data = [[
        Paragraph(f'<font size="36" color="#b5862a"><b>{iai["iai_score"]}</b></font><font size="14" color="#aaaaaa"> / 100</font>', S_body),
        Paragraph(f'<b>Moderate Investor Activity Detected</b><br/><br/>The IAI is a composite score measuring investor activity intensity. '
                  f'Elbert County shows meaningful signals in unincorporated rural areas. Score is conservative — adding LLC and out-of-state buyer data would push it to 55-70.<br/><br/>'
                  f'<font color="#b5862a"><b>CONFIDENCE: MEDIUM</b></font>', S_body)
    ]]
    st = Table(score_data, colWidths=[1.4*inch, 5.6*inch])
    st.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),INK),("TEXTCOLOR",(0,0),(-1,-1),WHITE),("VALIGN",(0,0),(-1,-1),"MIDDLE"),("PADDING",(0,0),(-1,-1),16)]))
    story.append(st)
    story.append(Spacer(1, 16))

    story.append(Paragraph("KEY METRICS", S_label))
    story.append(Paragraph("At a Glance", S_h2))
    metrics = [
        ["Total Transactions", str(int(iai["total_transactions"])), "Investor Flagged", f"{int(iai['investor_flagged'])} ({iai['investor_pct']}%)"],
        ["Quick Flip Parcels", str(int(iai["quick_flip_parcels"])), "Deals Per Month", str(iai["transaction_velocity_per_month"])],
        ["Avg Sale Price", f"${iai['avg_sale_price']:,.0f}", "Avg Parcel Size", f"{iai['avg_acres']} acres"],
        ["Subdivision Conc.", f"{iai['subdivision_concentration_pct']}%", "Last Activity", str(iai["last_activity"])],
    ]
    mt = Table(metrics, colWidths=[1.5*inch, 1.8*inch, 1.8*inch, 1.9*inch])
    mt.setStyle(TableStyle([("FONTSIZE",(0,0),(-1,-1),9),("PADDING",(0,0),(-1,-1),8),("ROWBACKGROUNDS",(0,0),(-1,-1),[WHITE,CREAM]),("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#ddd8cc")),("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),("FONTNAME",(2,0),(2,-1),"Helvetica-Bold")]))
    story.append(mt)
    story.append(Spacer(1, 16))

    story.append(Paragraph("GEOGRAPHIC BREAKDOWN", S_label))
    story.append(Paragraph("Sales by Neighborhood", S_h2))
    nb_hdr = [["Neighborhood","Total Sales","Investor Flagged","Avg Price","Avg Acres"]]
    nb_data = nb_hdr + [[row["neighborhood"],str(int(row["sales"])),str(int(row["investor_flagged"])),f"${row['avg_price']:,.0f}",f"{row['avg_acres']} ac"] for _,row in neighborhoods.iterrows()]
    nbt = Table(nb_data, colWidths=[1.8*inch,1.1*inch,1.4*inch,1.4*inch,1.3*inch])
    nbs = TableStyle([("BACKGROUND",(0,0),(-1,0),INK),("TEXTCOLOR",(0,0),(-1,0),WHITE),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),9),("PADDING",(0,0),(-1,-1),8),("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE,CREAM]),("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#ddd8cc")),("VALIGN",(0,0),(-1,-1),"MIDDLE")])
    for i,(_, row) in enumerate(neighborhoods.iterrows(),start=1):
        if int(row["investor_flagged"]) > 0:
            nbs.add("BACKGROUND",(0,i),(-1,i),LGOLD)
    nbt.setStyle(nbs)
    story.append(nbt)
    story.append(Spacer(1, 16))

    story.append(Paragraph("STRONGEST SIGNALS", S_label))
    story.append(Paragraph("Quick Flip Parcels", S_h2))
    fp_hdr = [["Parcel ID","Sale Date","Sale Price","Acres","Neighborhood","Score"]]
    fp_data = fp_hdr + [[row["parcel_id"].strip(),str(row["sale_date"])[:10],f"${row['sale_price']:,.0f}",f"{row['acres']} ac",row["neighborhood"],str(row["investor_score"])] for _,row in top_flips.iterrows()]
    fpt = Table(fp_data, colWidths=[1.1*inch,1.0*inch,1.1*inch,0.7*inch,1.8*inch,0.8*inch])
    fpt.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),INK),("TEXTCOLOR",(0,0),(-1,0),WHITE),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),8),("PADDING",(0,0),(-1,-1),7),("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE,CREAM]),("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#ddd8cc")),("VALIGN",(0,0),(-1,-1),"MIDDLE")]))
    story.append(fpt)
    story.append(Spacer(1, 10))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#ddd8cc"), spaceAfter=8))
    story.append(Paragraph("<b>Data limitation:</b> qPublic export does not include grantor/grantee names. Adding GSCCCA deed data would activate 5 additional flags and raise the IAI score to the 55-70 range.", S_small))

    doc.build(story)

# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    print(f"Project root: {BASE_DIR}")
    print("Loading data from DuckDB...")
    iai, neighborhoods, top_flips = load_data()

    print("Building HTML report...")
    html = build_html(iai, neighborhoods, top_flips)
    HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
    HTML_PATH.write_text(html, encoding="utf-8")
    print(f"HTML saved: {HTML_PATH}")

    print("Building PDF report...")
    build_pdf(iai, neighborhoods, top_flips)
    print(f"PDF saved:  {PDF_PATH}")

    print("\nDone!")

if __name__ == "__main__":
    main()
