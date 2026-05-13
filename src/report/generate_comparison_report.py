"""
generate_comparison_report.py
Generates a polished HTML and PDF comparison report for all 5 counties.
Usage: python src/report/generate_comparison_report.py
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
HTML_PATH = BASE_DIR / "outputs" / "county_comparison_report.html"
PDF_PATH  = BASE_DIR / "outputs" / "county_comparison_report.pdf"

# --------------------------------------------------------------------------
# Load data
# --------------------------------------------------------------------------
def load_data():
    con      = duckdb.connect(str(DB_PATH))
    rankings = con.execute("SELECT * FROM mart_county_rankings ORDER BY rank").df()
    facts    = con.execute("SELECT * FROM fact_transactions_all").df()
    con.close()
    return rankings, facts

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def iai_color(score):
    if score >= 30: return "#2d6a4f"
    if score >= 15: return "#b5862a"
    return "#c0392b"

def iai_label(score):
    if score >= 30: return ("HIGH",     "#2d6a4f", "#d4edda")
    if score >= 15: return ("MODERATE", "#b5862a", "#f0e4c4")
    return             ("LOW",      "#c0392b", "#fde8e8")

def bar_width(score, max_score):
    return max(4, round((score / max(max_score, 1)) * 100))

# --------------------------------------------------------------------------
# Build HTML
# --------------------------------------------------------------------------
def build_html(rankings, facts):
    gen_date  = datetime.now().strftime("%B %d, %Y")
    max_score = rankings["iai_score"].max()
    medals    = ["1st", "2nd", "3rd", "4th", "5th"]

    cards_html = ""
    for _, row in rankings.iterrows():
        score         = float(row["iai_score"])
        color         = iai_color(score)
        label, lc, lb = iai_label(score)
        rank          = int(row["rank"])
        bw            = bar_width(score, max_score)
        cards_html += f"""
        <div class="county-card rank-{rank}">
          <div class="card-header">
            <div class="rank-badge">{medals[rank-1]}</div>
            <div class="county-name">{row['county']}</div>
            <div class="iai-score" style="color:{color}">{score}</div>
          </div>
          <div class="score-bar-track"><div class="score-bar-fill" style="width:{bw}%; background:{color}"></div></div>
          <div class="activity-badge" style="color:{lc}; background:{lb}">{label} ACTIVITY</div>
          <div class="card-metrics">
            <div class="cm"><span class="cm-val">{int(row['total_transactions'])}</span><span class="cm-label">Total Deals</span></div>
            <div class="cm"><span class="cm-val">{int(row['investor_flagged'])}</span><span class="cm-label">Flagged</span></div>
            <div class="cm"><span class="cm-val">{int(row['quick_flip_parcels'])}</span><span class="cm-label">Flips</span></div>
            <div class="cm"><span class="cm-val">${row['avg_sale_price']:,.0f}</span><span class="cm-label">Avg Price</span></div>
          </div>
        </div>"""

    table_rows = ""
    for _, row in rankings.iterrows():
        score         = float(row["iai_score"])
        color         = iai_color(score)
        label, lc, lb = iai_label(score)
        table_rows += f"""
        <tr>
          <td><strong>#{int(row['rank'])}</strong></td>
          <td><strong>{row['county']}</strong></td>
          <td style="color:{color}; font-weight:700">{score}</td>
          <td><span class="badge" style="color:{lc}; background:{lb}">{label}</span></td>
          <td>{int(row['total_transactions'])}</td>
          <td>{int(row['investor_flagged'])} ({row['investor_pct']}%)</td>
          <td>{int(row['quick_flip_parcels'])}</td>
          <td>{row['transaction_velocity_per_month']}/mo</td>
          <td>${row['avg_sale_price']:,.0f}</td>
          <td>{row['avg_acres']} ac</td>
          <td>{row['last_activity']}</td>
        </tr>"""

    facts["sale_date"] = pd.to_datetime(facts["sale_date"])
    top_flips = facts[facts["quick_flip"] == 1].sort_values("investor_score", ascending=False).head(10)
    flip_rows = ""
    for _, row in top_flips.iterrows():
        flip_rows += f"""
        <tr>
          <td><code>{row['parcel_id'].strip()}</code></td>
          <td>{row['county']}</td>
          <td>{str(row['sale_date'])[:10]}</td>
          <td>${row['sale_price']:,.0f}</td>
          <td>{row['acres']} ac</td>
          <td>{row['neighborhood']}</td>
          <td><span class="score-pill">{row['investor_score']}</span></td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>5-County Land Investor Activity Comparison</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&family=DM+Mono&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  :root {{ --ink:#1a1a2e; --ink-light:#4a4a6a; --paper:#faf9f6; --cream:#f2efe8; --gold:#b5862a; --gold-lt:#f0e4c4; --green:#2d6a4f; --red:#c0392b; --border:#ddd8cc; --shadow:0 2px 16px rgba(26,26,46,0.08); }}
  body {{ font-family:'DM Sans',sans-serif; background:var(--paper); color:var(--ink); font-size:15px; line-height:1.6; }}
  header {{ background:var(--ink); color:#fff; padding:56px 64px 48px; position:relative; overflow:hidden; }}
  header::before {{ content:''; position:absolute; top:-80px; right:-80px; width:360px; height:360px; border-radius:50%; background:rgba(181,134,42,0.10); }}
  .header-label {{ font-size:11px; font-weight:600; letter-spacing:3px; text-transform:uppercase; color:var(--gold); margin-bottom:12px; }}
  header h1 {{ font-family:'DM Serif Display',serif; font-size:40px; font-weight:400; line-height:1.15; margin-bottom:8px; }}
  .header-sub {{ color:rgba(255,255,255,0.5); font-size:14px; font-weight:300; }}
  .header-meta {{ margin-top:32px; display:flex; gap:40px; flex-wrap:wrap; }}
  .meta-item {{ display:flex; flex-direction:column; gap:2px; }}
  .meta-label {{ font-size:10px; letter-spacing:2px; text-transform:uppercase; color:rgba(255,255,255,0.4); }}
  .meta-value {{ font-size:13px; color:rgba(255,255,255,0.85); font-weight:500; }}
  main {{ max-width:1100px; margin:0 auto; padding:48px 40px 80px; }}
  section {{ margin-bottom:60px; }}
  .section-label {{ font-size:10px; font-weight:600; letter-spacing:3px; text-transform:uppercase; color:var(--gold); margin-bottom:6px; }}
  h2 {{ font-family:'DM Serif Display',serif; font-size:26px; font-weight:400; margin-bottom:24px; padding-bottom:12px; border-bottom:1px solid var(--border); }}
  .cards-grid {{ display:grid; grid-template-columns:repeat(5,1fr); gap:16px; }}
  .county-card {{ background:#fff; border:1px solid var(--border); border-radius:14px; padding:24px 20px; box-shadow:var(--shadow); display:flex; flex-direction:column; gap:12px; }}
  .county-card.rank-1 {{ border-top:3px solid var(--gold); }}
  .county-card.rank-2 {{ border-top:3px solid #9e9e9e; }}
  .county-card.rank-3 {{ border-top:3px solid #cd7f32; }}
  .rank-badge {{ font-size:11px; font-weight:700; letter-spacing:2px; text-transform:uppercase; color:var(--ink-light); }}
  .county-name {{ font-family:'DM Serif Display',serif; font-size:16px; color:var(--ink); line-height:1.2; }}
  .iai-score {{ font-family:'DM Serif Display',serif; font-size:36px; line-height:1; margin-top:4px; }}
  .score-bar-track {{ height:6px; background:var(--cream); border-radius:3px; overflow:hidden; }}
  .score-bar-fill {{ height:100%; border-radius:3px; }}
  .activity-badge {{ display:inline-block; font-size:9px; font-weight:700; letter-spacing:1.5px; text-transform:uppercase; padding:3px 10px; border-radius:20px; align-self:flex-start; }}
  .card-metrics {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-top:4px; }}
  .cm {{ display:flex; flex-direction:column; gap:1px; }}
  .cm-val {{ font-size:14px; font-weight:600; color:var(--ink); }}
  .cm-label {{ font-size:10px; color:var(--ink-light); }}
  .table-wrap {{ background:#fff; border:1px solid var(--border); border-radius:12px; overflow:hidden; box-shadow:var(--shadow); overflow-x:auto; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  thead {{ background:var(--ink); color:#fff; }}
  thead th {{ padding:13px 14px; text-align:left; font-size:10px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; white-space:nowrap; }}
  tbody tr {{ border-bottom:1px solid var(--cream); }}
  tbody tr:last-child {{ border-bottom:none; }}
  tbody tr:hover {{ background:var(--cream); }}
  td {{ padding:12px 14px; vertical-align:middle; }}
  .badge {{ display:inline-block; font-size:9px; font-weight:700; letter-spacing:1px; text-transform:uppercase; padding:3px 10px; border-radius:20px; }}
  .score-pill {{ display:inline-block; background:var(--gold-lt); color:var(--gold); font-weight:600; font-size:12px; padding:2px 10px; border-radius:20px; }}
  code {{ font-family:'DM Mono',monospace; font-size:11px; background:var(--cream); padding:2px 6px; border-radius:4px; color:var(--ink-light); }}
  .findings-list {{ list-style:none; display:flex; flex-direction:column; gap:14px; }}
  .findings-list li {{ display:flex; gap:16px; align-items:flex-start; background:#fff; border:1px solid var(--border); border-radius:10px; padding:18px 22px; box-shadow:var(--shadow); }}
  .finding-num {{ font-family:'DM Serif Display',serif; font-size:26px; color:var(--gold); line-height:1; flex-shrink:0; width:28px; }}
  .finding-text {{ font-size:14px; line-height:1.7; color:var(--ink-light); padding-top:2px; }}
  .finding-text strong {{ color:var(--ink); }}
  .next-grid {{ display:grid; grid-template-columns:repeat(3,1fr); gap:16px; }}
  .next-card {{ background:#fff; border:1px solid var(--border); border-radius:10px; padding:20px 22px; box-shadow:var(--shadow); }}
  .next-card h4 {{ font-size:12px; font-weight:600; letter-spacing:1px; text-transform:uppercase; color:var(--gold); margin-bottom:8px; }}
  .next-card p {{ font-size:13px; color:var(--ink-light); line-height:1.6; }}
  footer {{ background:var(--cream); border-top:1px solid var(--border); padding:24px 64px; font-size:12px; color:var(--ink-light); display:flex; justify-content:space-between; align-items:center; }}
  footer strong {{ color:var(--ink); }}
</style>
</head>
<body>
<header>
  <div class="header-label">Proof of Concept: Multi-County Analysis</div>
  <h1>Northeast Georgia<br>Land Investor Activity Comparison</h1>
  <p class="header-sub">Multi-County Investor Activity Index: Market Intelligence</p>
  <div class="header-meta">
    <div class="meta-item"><span class="meta-label">Generated</span><span class="meta-value">{gen_date}</span></div>
    <div class="meta-item"><span class="meta-label">Analysis Window</span><span class="meta-value">Jan 2022 – Dec 2024</span></div>
    <div class="meta-item"><span class="meta-label">Counties Analyzed</span><span class="meta-value">Elbert · Lincoln · Wilkes · Warren · McDuffie</span></div>
    <div class="meta-item"><span class="meta-label">Total Transactions</span><span class="meta-value">{int(facts.shape[0])} recorded sales</span></div>
  </div>
</header>
<main>
  <section>
    <div class="section-label">Overall Rankings</div>
    <h2>Investor Activity Index by County</h2>
    <div class="cards-grid">{cards_html}</div>
  </section>
  <section>
    <div class="section-label">Side-by-Side Comparison</div>
    <h2>Full Metrics Table</h2>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Rank</th><th>County</th><th>IAI Score</th><th>Activity</th><th>Total Deals</th><th>Investor Flagged</th><th>Quick Flips</th><th>Velocity</th><th>Avg Price</th><th>Avg Acres</th><th>Last Activity</th></tr></thead>
        <tbody>{table_rows}</tbody>
      </table>
    </div>
  </section>
  <section>
    <div class="section-label">Roadmap</div>
    <h2>Next Steps to Strengthen This Analysis</h2>
    <div class="next-grid">
      <div class="next-card"><h4>Refine Metrics</h4><p>Deepen knowledge of land deal fundamentals to refine these metrics into something more actionable and useful for active land investors.</p></div>
      <div class="next-card"><h4>Expand Counties</h4><p>Apply the same pipeline to all 159 Georgia counties to produce a statewide ranking and identify undiscovered markets.</p></div>
      <div class="next-card"><h4>Automate Refresh</h4><p>Schedule the pipeline to pull fresh data quarterly so rankings stay current and new activity is flagged automatically.</p></div>
    </div>
  </section>
</main>
<footer>
  <div><strong>County-Level Land Investor Activity Estimator</strong> — Data Engineering Proof of Concept</div>
  <div>Generated {gen_date} · Northeast Georgia · 2022–2024</div>
</footer>
</body>
</html>"""
    return html

# --------------------------------------------------------------------------
# Build PDF
# --------------------------------------------------------------------------
def build_pdf(rankings, facts):
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable

    INK   = colors.HexColor("#1a1a2e")
    GOLD  = colors.HexColor("#b5862a")
    CREAM = colors.HexColor("#f2efe8")
    GREEN = colors.HexColor("#2d6a4f")
    RED   = colors.HexColor("#c0392b")
    WHITE = colors.white

    def ps(name, **kw): return ParagraphStyle(name, **kw)
    S_label = ps("lbl",  fontName="Helvetica-Bold", fontSize=8,  textColor=GOLD, spaceAfter=2,  leading=10)
    S_h1    = ps("h1",   fontName="Helvetica-Bold", fontSize=20, textColor=INK,  spaceAfter=4,  leading=24)
    S_h2    = ps("h2",   fontName="Helvetica-Bold", fontSize=13, textColor=INK,  spaceAfter=8,  spaceBefore=16, leading=16)
    S_body  = ps("body", fontName="Helvetica",      fontSize=9,  textColor=colors.HexColor("#4a4a6a"), leading=14, spaceAfter=5)
    S_small = ps("sm",   fontName="Helvetica",      fontSize=8,  textColor=colors.HexColor("#4a4a6a"), leading=12, spaceAfter=4)

    doc   = SimpleDocTemplate(str(PDF_PATH), pagesize=letter,
                leftMargin=0.65*inch, rightMargin=0.65*inch,
                topMargin=0.65*inch,  bottomMargin=0.65*inch)
    story = []
    gen_date = datetime.now().strftime("%B %d, %Y")

    story.append(Paragraph("Proof of Concept: Multi-County Analysis", S_label))
    story.append(Paragraph("Northeast Georgia — Land Investor Activity Comparison", S_h1))
    story.append(Paragraph(f"Generated: {gen_date}  |  Window: Jan 2022 - Dec 2024  |  Counties: Elbert, Lincoln, Wilkes, Warren, McDuffie  |  Total: {int(facts.shape[0])} transactions", S_small))
    story.append(HRFlowable(width="100%", thickness=1, color=GOLD, spaceAfter=14))

    def act_label(s):
        if s >= 30: return "HIGH"
        if s >= 15: return "MODERATE"
        return "LOW"

    def act_color(s):
        if s >= 30: return GREEN
        if s >= 15: return GOLD
        return RED

    story.append(Paragraph("COUNTY RANKINGS", S_label))
    story.append(Paragraph("Investor Activity Index by County", S_h2))

    hdr  = [["Rank","County","IAI","Activity","Deals","Flagged","Flips","Velocity","Avg Price","Avg Acres"]]
    data = hdr + [[f"#{int(r['rank'])}",r["county"],str(float(r["iai_score"])),act_label(float(r["iai_score"])),str(int(r["total_transactions"])),f"{int(r['investor_flagged'])} ({r['investor_pct']}%)",str(int(r["quick_flip_parcels"])),f"{r['transaction_velocity_per_month']}/mo",f"${r['avg_sale_price']:,.0f}",f"{r['avg_acres']} ac"] for _,r in rankings.iterrows()]
    col_w = [0.45*inch,1.1*inch,0.45*inch,0.75*inch,0.5*inch,0.85*inch,0.45*inch,0.65*inch,0.85*inch,0.65*inch]
    t  = Table(data, colWidths=col_w)
    ts = TableStyle([("BACKGROUND",(0,0),(-1,0),INK),("TEXTCOLOR",(0,0),(-1,0),WHITE),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),8),("PADDING",(0,0),(-1,-1),6),("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE,CREAM]),("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#ddd8cc")),("VALIGN",(0,0),(-1,-1),"MIDDLE")])
    for i,(_, r) in enumerate(rankings.iterrows(), start=1):
        ts.add("TEXTCOLOR",(2,i),(2,i),act_color(float(r["iai_score"])))
        ts.add("FONTNAME", (2,i),(2,i),"Helvetica-Bold")
    t.setStyle(ts)
    story.append(t)
    story.append(Spacer(1, 18))

    facts["sale_date"] = pd.to_datetime(facts["sale_date"])
    top_flips = facts[facts["quick_flip"]==1].sort_values("investor_score",ascending=False).head(10)
    story.append(Paragraph("STRONGEST SIGNALS", S_label))
    story.append(Paragraph("Top Quick Flip Parcels — All Counties", S_h2))
    fhdr  = [["Parcel ID","County","Sale Date","Sale Price","Acres","Neighborhood","Score"]]
    fdata = fhdr + [[r["parcel_id"].strip(),r["county"],str(r["sale_date"])[:10],f"${r['sale_price']:,.0f}",f"{r['acres']} ac",r["neighborhood"],str(r["investor_score"])] for _,r in top_flips.iterrows()]
    fcol  = [0.85*inch,0.8*inch,0.85*inch,0.85*inch,0.55*inch,1.4*inch,0.55*inch]
    ft    = Table(fdata, colWidths=fcol)
    ft.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),INK),("TEXTCOLOR",(0,0),(-1,0),WHITE),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),8),("PADDING",(0,0),(-1,-1),6),("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE,CREAM]),("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#ddd8cc")),("VALIGN",(0,0),(-1,-1),"MIDDLE")]))
    story.append(ft)
    story.append(Spacer(1, 18))

    story.append(Paragraph("KEY FINDINGS", S_label))
    story.append(Paragraph("What the Data Is Saying", S_h2))
    for bold, rest in [
        ("1. Elbert County is the clear leader", "IAI 35.9 — highest activity across all 5 counties with 22 investor-flagged transactions and 20 quick flip parcels."),
        ("2. Wilkes County is a strong second", "IAI 28.5 with 12 confirmed flip parcels. Comparable flip rate to Elbert on a per-transaction basis."),
        ("3. McDuffie and Lincoln show clustering without flipping", "Buyers active in concentrated neighborhoods but no parcels turned over twice — possible early-stage investor adoption."),
        ("4. Warren County is effectively dormant", "Only 2 transactions in 3 years. Could indicate tightly held family land, data gaps, or undiscovered market."),
        ("5. All scores are conservative", "Missing grantor/grantee data means LLC buyer, out-of-state, and repeat buyer flags are inactive. Real scores likely 20-30 points higher."),
    ]:
        story.append(Paragraph(f"<b>{bold}:</b> {rest}", S_body))

    story.append(Spacer(1, 10))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#ddd8cc"), spaceAfter=8))
    story.append(Paragraph("<b>Next steps:</b> Integrate GSCCCA deed data · Expand to all 159 Georgia counties · Schedule quarterly refresh.", S_small))
    doc.build(story)

# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    print(f"Project root: {BASE_DIR}")
    print("Loading data from DuckDB...")
    rankings, facts = load_data()

    print("Building HTML comparison report...")
    html = build_html(rankings, facts)
    HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
    HTML_PATH.write_text(html, encoding="utf-8")
    print(f"HTML saved: {HTML_PATH}")

    print("Building PDF comparison report...")
    build_pdf(rankings, facts)
    print(f"PDF saved:  {PDF_PATH}")

    print("\nDone!")

if __name__ == "__main__":
    main()
