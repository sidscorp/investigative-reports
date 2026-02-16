"""
Medicaid Provider Spending — Investigative Dashboard

A Streamlit app that visualizes precomputed summary data from 7 investigations
into Medicaid provider spending patterns (2018-2024).

Features:
- "Tufte-inspired" clean design (custom CSS).
- Interactive Plotly visualizations.
- Deep dive into E&M Upcoding, Ghost Providers, and Regional Fraud.

Usage:
    streamlit run scripts/dashboard.py
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from pathlib import Path

# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "output"
DASH_DIR = OUTPUT_DIR / "dashboard"

# ─── Tufte-inspired Design System ─────────────────────────────────────────────
COLORS = {
    "primary": "#2c3e50",    # Midnight Blue
    "accent": "#c0392b",     # Pomegranate
    "success": "#27ae60",    # Nephritis
    "warning": "#f39c12",    # Orange
    "info": "#2980b9",       # Belize Hole
    "muted": "#95a5a6",      # Concrete
    "bg": "#fdfdfd",         # Off-white
}

PALETTE = [
    "#2c3e50", "#c0392b", "#2980b9", "#27ae60", "#8e44ad",
    "#d35400", "#16a085", "#7f8c8d", "#f39c12", "#1abc9c",
]

PLOTLY_LAYOUT = dict(
    font=dict(family="Inter, sans-serif", color="#2c3e50", size=13),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=40, r=20, t=40, b=40),
    hoverlabel=dict(bgcolor="white", font_size=12, bordercolor="#ddd"),
)

def tufte_axes(fig, show_xgrid=False, show_ygrid=True):
    fig.update_xaxes(
        showgrid=show_xgrid,
        gridcolor="#eee",
        zeroline=False,
        showline=True,
        linewidth=1,
        linecolor="#ccc",
        tickfont=dict(size=11),
    )
    fig.update_yaxes(
        showgrid=show_ygrid,
        gridcolor="#eee",
        gridwidth=0.5,
        zeroline=False,
        showline=False,
        tickfont=dict(size=11),
    )
    return fig

def fmt_dollars(val):
    if val is None: return "$0"
    if val >= 1e12: return f"${val/1e12:.2f}T"
    if val >= 1e9: return f"${val/1e9:.2f}B"
    if val >= 1e6: return f"${val/1e6:.1f}M"
    if val >= 1e3: return f"${val/1e3:.0f}K"
    return f"${val:,.0f}"

def fmt_num(val):
    if val is None: return "0"
    if val >= 1e9: return f"{val/1e9:.2f}B"
    if val >= 1e6: return f"{val/1e6:.1f}M"
    if val >= 1e3: return f"{val/1e3:.0f}K"
    return f"{val:,.0f}"

# ─── Data Loading ─────────────────────────────────────────────────────────────
@st.cache_data
def load_csv(name):
    path = DASH_DIR / name
    if path.exists():
        return pl.read_csv(str(path)).to_pandas()
    return None

@st.cache_data
def load_investigation_csv(name):
    path = OUTPUT_DIR / name
    if path.exists():
        return pl.read_csv(str(path)).to_pandas()
    return None

# ─── Page Config & Custom CSS ─────────────────────────────────────────────────
st.set_page_config(
    page_title="Medicaid Provider Investigations",
    page_icon="🕵️‍♂️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* Global Typography */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    
    html, body, [class*="css"]  {
        font-family: 'Inter', sans-serif;
        color: #2c3e50;
    }
    
    h1, h2, h3 {
        font-weight: 600;
        letter-spacing: -0.02em;
    }
    
    h1 { font-size: 2.2rem; margin-bottom: 1rem; }
    h2 { font-size: 1.6rem; color: #34495e; margin-top: 2rem; border-bottom: 1px solid #eee; padding-bottom: 0.5rem; }
    h3 { font-size: 1.2rem; color: #7f8c8d; margin-top: 1.5rem; }
    
    /* Metrics */
    .stMetric label { font-size: 0.8rem; color: #7f8c8d; text-transform: uppercase; letter-spacing: 0.05em; }
    .stMetric [data-testid="stMetricValue"] { font-size: 1.8rem; font-weight: 600; color: #2c3e50; }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #f8f9fa;
        border-right: 1px solid #eee;
    }
    
    /* Dataframes */
    .stDataFrame { font-size: 0.85rem; border: 1px solid #eee; border-radius: 4px; }
    
    /* Custom Alert Boxes */
    .custom-alert {
        padding: 1rem;
        border-radius: 4px;
        margin-bottom: 1rem;
        font-size: 0.9rem;
    }
    .alert-info { background-color: #eaf4fc; border-left: 4px solid #2980b9; color: #2c3e50; }
    .alert-warning { background-color: #fdf6e7; border-left: 4px solid #f39c12; color: #2c3e50; }
    
    hr { margin: 2rem 0; border-top: 1px solid #eee; }
</style>
""", unsafe_allow_html=True)

# ─── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.title("Medicaid Investigations")
st.sidebar.markdown("**2018–2024 Provider Spending**")

PAGES = [
    "Overview",
    "E&M Upcoding",
    "Ghost Providers",
    "Brooklyn T1019",
    "Minnesota Fraud",
    "Service Analysis",
    "Top Providers",
    "Temporal Anomalies",
    "Individual Outliers",
]

page = st.sidebar.radio("Navigate", PAGES)
st.sidebar.markdown("---")
st.sidebar.info("Data: CMS Medicaid Provider Utilization & NPI Registry")

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
def page_overview():
    st.markdown("# National Overview")
    st.markdown("High-level metrics on Medicaid provider spending and entity distribution.")

    state_df = load_csv("state_spending.csv")
    entity_df = load_csv("entity_segmentation.csv")
    conc_df = load_csv("concentration.csv")
    ts_nat = load_csv("ts_national_monthly.csv")

    if state_df is not None:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Spending", fmt_dollars(state_df["TOTAL_SPENT"].sum()))
        c2.metric("Unique Providers", fmt_num(state_df["UNIQUE_PROVIDERS"].sum()))
        c3.metric("Beneficiary Count (Est)", fmt_num(state_df["BENE_SUM"].sum()))
        c4.metric("Dataset Coverage", "2018 – 2024")

    # Layout: Map | Entity Pie
    c1, c2 = st.columns([3, 2])
    
    with c1:
        st.markdown("### Spending by State")
        if state_df is not None:
            fig = go.Figure(go.Choropleth(
                locations=state_df["STATE"],
                z=state_df["TOTAL_SPENT"],
                locationmode="USA-states",
                colorscale="Blues",
                colorbar=dict(title="", tickformat="$.2s"),
                hovertemplate="<b>%{location}</b><br>%{z:$,.0f}<extra></extra>",
            ))
            fig.update_layout(**PLOTLY_LAYOUT, geo=dict(scope="usa", showlakes=False), height=400)
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("### Entity Segmentation")
        if entity_df is not None:
            fig = px.pie(
                entity_df, 
                values="TOTAL_SPENT", 
                names="ENTITY_LABEL", 
                color_discrete_sequence=PALETTE,
                hole=0.6
            )
            fig.update_layout(**PLOTLY_LAYOUT, showlegend=False, height=400, 
                              annotations=[dict(text="Spending", x=0.5, y=0.5, font_size=16, showarrow=False)])
            fig.update_traces(textinfo="label+percent")
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    
    # Layout: Trend | Concentration
    c1, c2 = st.columns([3, 2])
    
    with c1:
        st.markdown("### National Monthly Spending")
        if ts_nat is not None:
            fig = px.area(ts_nat, x="CLAIM_FROM_MONTH", y="TOTAL_PAID", color_discrete_sequence=[COLORS["primary"]])
            fig.update_layout(**PLOTLY_LAYOUT, height=300, yaxis_tickformat="$.2s", xaxis_title="")
            tufte_axes(fig)
            st.plotly_chart(fig, use_container_width=True)
            
    with c2:
        st.markdown("### Market Concentration")
        if conc_df is not None:
            conc_df["label"] = conc_df["TOP_N"].apply(lambda x: f"Top {x}")
            fig = px.bar(conc_df, y="label", x="SHARE_OF_TOTAL", orientation="h", 
                         text_auto=".1%", color_discrete_sequence=[COLORS["primary"]])
            fig.update_layout(**PLOTLY_LAYOUT, height=300, xaxis_title="% of Total Spending", yaxis_title="")
            tufte_axes(fig)
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: E&M UPCODING
# ═══════════════════════════════════════════════════════════════════════════════
def page_em_upcoding():
    st.markdown("# Comprehensive E&M Upcoding")
    st.markdown("""
    <div class="custom-alert alert-info">
    <b>What is Upcoding?</b> Providers systematically billing high-complexity codes (Level 4/5) 
    that pay more, at rates statistically impossible for their peer group.
    </div>
    """, unsafe_allow_html=True)
    
    # Load Data
    prov_df = load_investigation_csv("em_upcoding_providers.csv")
    spec_df = load_investigation_csv("em_upcoding_by_specialty.csv")
    state_df = load_investigation_csv("em_upcoding_by_state.csv")
    
    if prov_df is None:
        st.warning("E&M data not found. Run scripts/investigate_em_upcoding.py first.")
        return

    # KPI Row
    total_excess = spec_df["TOTAL_EXCESS_REVENUE"].sum() if spec_df is not None else 0
    c1, c2, c3 = st.columns(3)
    c1.metric("Est. Excess Revenue", fmt_dollars(total_excess), help="Revenue attributable to billing complexity above peer median.")
    c2.metric("Analyzed Providers", fmt_num(len(prov_df)))
    c3.metric("Avg Upcoding Index", f"{prov_df['UPCODING_INDEX'].mean():.2f}", help="Weighted average complexity (1-5)")

    tab1, tab2, tab3 = st.tabs(["By Specialty", "By State", "Provider Deep Dive"])

    with tab1:
        st.markdown("### Upcoding Intensity by Specialty")
        if spec_df is not None:
            fig = px.bar(
                spec_df.head(15), 
                y="SPECIALTY", 
                x="TOTAL_EXCESS_REVENUE",
                color="OUTLIER_PCT",
                color_continuous_scale="Reds",
                orientation="h",
                title="Top Specialties by Excess Revenue",
                labels={"TOTAL_EXCESS_REVENUE": "Est. Excess Revenue", "SPECIALTY": ""}
            )
            fig.update_layout(**PLOTLY_LAYOUT, height=500, yaxis=dict(autorange="reversed"))
            tufte_axes(fig)
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(
                spec_df.style.format({
                    "TOTAL_EXCESS_REVENUE": "${:,.0f}",
                    "AVG_INDEX": "{:.2f}",
                    "AVG_L5_RATIO": "{:.1%}",
                    "OUTLIER_PCT": "{:.1%}"
                }),
                use_container_width=True
            )

    with tab2:
        st.markdown("### Geographic Hotspots")
        if state_df is not None:
            c1, c2 = st.columns([3, 1])
            with c1:
                fig = px.choropleth(
                    state_df,
                    locations="STATE",
                    locationmode="USA-states",
                    color="AVG_EXCESS_PER_PROVIDER",
                    scope="usa",
                    color_continuous_scale="Oranges",
                    title="Avg Excess Revenue per Provider"
                )
                fig.update_layout(**PLOTLY_LAYOUT, height=500)
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                st.dataframe(
                    state_df[["STATE", "AVG_EXCESS_PER_PROVIDER", "TOTAL_EXCESS_REVENUE"]]
                    .style.format({"AVG_EXCESS_PER_PROVIDER": "${:,.0f}", "TOTAL_EXCESS_REVENUE": "${:,.0f}"}),
                    use_container_width=True,
                    height=500
                )

    with tab3:
        st.markdown("### Provider Search")
        
        # Filters
        c1, c2 = st.columns(2)
        states = sorted(prov_df["STATE"].unique())
        selected_state = c1.selectbox("Filter State", ["All"] + states)
        
        specs = sorted(prov_df["SPECIALTY"].unique())
        selected_spec = c2.selectbox("Filter Specialty", ["All"] + specs)
        
        filtered_df = prov_df.copy()
        if selected_state != "All": filtered_df = filtered_df[filtered_df["STATE"] == selected_state]
        if selected_spec != "All": filtered_df = filtered_df[filtered_df["SPECIALTY"] == selected_spec]
        
        st.dataframe(
            filtered_df.head(1000).style.format({
                "UPCODING_INDEX": "{:.2f}",
                "MEDIAN_INDEX": "{:.2f}",
                "LEVEL_5_RATIO": "{:.1%}",
                "EST_EXCESS_REVENUE": "${:,.0f}"
            }),
            use_container_width=True,
            height=600
        )

    with st.expander("Methodology: How is Excess Revenue Calculated?"):
        st.markdown("""
        **Formula:** `(Provider Index - Peer Median Index) / Peer Median Index * Total Revenue`
        
        We calculate an **Upcoding Index** (weighted average of Level 1-5 codes) for every provider. 
        We then compare this to the **Median Index** of their exact specialty.
        The percentage difference represents the "Upcoding Premium" — the portion of revenue attributable 
        solely to billing higher complexity codes than their peers.
        
        *Note: This isolates complexity creep. It does not penalize providers who are expensive for other reasons (like geography).*
        """)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: GHOST PROVIDERS
# ═══════════════════════════════════════════════════════════════════════════════
def page_ghost_providers():
    st.markdown("# Ghost Providers")
    st.markdown("""
    <div class="custom-alert alert-warning">
    <b>Detection Logic:</b> 
    1. <b>Impossible Volume:</b> >704 claims/month (22 days * 8 hours * 4 units/hr).
    2. <b>Address Clustering:</b> Multiple unrelated NPIs billing from the same location.
    </div>
    """, unsafe_allow_html=True)

    imp = load_investigation_csv("ghost_providers_impossible_volume.csv")
    addr = load_investigation_csv("ghost_providers_address_clustering.csv")

    tab1, tab2 = st.tabs(["Impossible Volume", "Address Clustering"])

    with tab1:
        if imp is not None:
            c1, c2 = st.columns(2)
            c1.metric("Flagged Providers", len(imp))
            c2.metric("Max Capacity Ratio", f"{imp['MAX_CAPACITY_RATIO'].max():.1f}x")
            
            st.markdown("### Top Offenders (Exceeding Physical Capacity)")
            st.dataframe(
                imp.style.format({
                    "TOTAL_PAID_OVER_CAPACITY": "${:,.0f}",
                    "MAX_CAPACITY_RATIO": "{:.1f}x",
                    "MAX_CLAIMS_PER_BENE": "{:.1f}"
                }),
                use_container_width=True
            )
            
    with tab2:
        if addr is not None:
            min_npi = st.slider("Min Providers at Address", 2, 50, 5)
            filtered = addr[addr["NPI_COUNT"] >= min_npi]
            
            st.markdown(f"### Addresses with {min_npi}+ Billing Providers")
            st.dataframe(
                filtered.style.format({
                    "TOTAL_PAID_AT_ADDRESS": "${:,.0f}",
                    "TOTAL_CLAIMS_AT_ADDRESS": "{:,.0f}"
                }),
                use_container_width=True
            )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: BROOKLYN T1019
# ═══════════════════════════════════════════════════════════════════════════════
def page_brooklyn():
    st.markdown("# Brooklyn T1019 Concentration")
    st.markdown("Investigation into the anomalous concentration of Personal Care Services (T1019) in Brooklyn, NY.")
    
    bk = load_investigation_csv("t1019_brooklyn_analysis.csv")
    shared = load_investigation_csv("t1019_shared_addresses.csv")
    
    if bk is not None:
        c1, c2, c3 = st.columns(3)
        c1.metric("Brooklyn Providers", fmt_num(len(bk)))
        c2.metric("Total Spending", fmt_dollars(bk["TOTAL_PAID"].sum()))
        c3.metric("Nat'l Top 20 Presence", f"{len(bk[bk['NATIONAL_RANK'] <= 20])} / 20")
        
        st.markdown("### Provider Ranking (National Context)")
        st.dataframe(
            bk.head(100).style.format({
                "TOTAL_PAID": "${:,.0f}",
                "COST_PER_CLAIM": "${:,.2f}",
                "COST_PER_BENE": "${:,.2f}"
            }),
            use_container_width=True
        )
        
    if shared is not None:
        st.markdown("### High-Risk Address Clusters")
        st.dataframe(
            shared.style.format({"COMBINED_PAID": "${:,.0f}"}),
            use_container_width=True
        )

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: MINNESOTA FRAUD
# ═══════════════════════════════════════════════════════════════════════════════
def page_minnesota():
    st.markdown("# Minnesota Behavioral Health")
    st.markdown("Replicating the DOJ/FBI findings regarding the $9B behavioral health fraud scheme.")
    
    anom = load_investigation_csv("minnesota_anomalies.csv")
    temp = load_investigation_csv("minnesota_temporal.csv")
    
    if anom is not None:
        st.markdown("### Flagged Providers")
        st.caption("Providers flagged for 'Explosive Growth' or 'Impossible Claims/Bene'")
        st.dataframe(
            anom[anom["ANOMALY_SCORE"] > 0].sort_values("ANOMALY_SCORE", ascending=False)
            .style.format({"TOTAL_PAID": "${:,.0f}"}),
            use_container_width=True
        )
        
    if temp is not None:
        st.markdown("### Growth Trajectories (Top Flagged)")
        top_ids = temp.groupby("PROVIDER_NAME")["MONTHLY_PAID"].max().nlargest(10).index
        fig = px.line(temp[temp["PROVIDER_NAME"].isin(top_ids)], x="CLAIM_FROM_MONTH", y="MONTHLY_PAID", color="PROVIDER_NAME")
        fig.update_layout(**PLOTLY_LAYOUT, height=400, yaxis_tickformat="$.2s")
        st.plotly_chart(fig, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════════════════
# OTHER PAGES (Simplified)
# ═══════════════════════════════════════════════════════════════════════════════
def page_service_analysis():
    st.markdown("# Service Analysis")
    df = load_csv("top_services.csv")
    if df is not None:
        fig = px.bar(df.head(20), y="SHORT_DESCRIPTION", x="TOTAL_SPENT", orientation="h")
        fig.update_layout(**PLOTLY_LAYOUT, height=600, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True)

def page_top_providers():
    st.markdown("# Top Providers")
    df = load_csv("top_organizations.csv")
    if df is not None:
        st.dataframe(df.style.format({"TOTAL_SPENT": "${:,.0f}"}), use_container_width=True)

def page_temporal():
    st.markdown("# Temporal Anomalies")
    st.info("Providers with >5x month-over-month billing spikes.")
    df = load_investigation_csv("temporal_spikes.csv")
    if df is not None:
        st.dataframe(df.style.format({"MAX_SPIKE_AMOUNT": "${:,.0f}"}), use_container_width=True)

def page_outliers():
    st.markdown("# Individual Outliers")
    st.info("Providers billing >5x their specialty median cost-per-beneficiary.")
    df = load_investigation_csv("individual_specialty_outliers.csv")
    if df is not None:
        st.dataframe(df.style.format({"TOTAL_SPENT": "${:,.0f}", "COST_RATIO": "{:.1f}x"}), use_container_width=True)

# ─── Router ──────────────────────────────────────────────────────────────────
PAGE_MAP = {
    "Overview": page_overview,
    "E&M Upcoding": page_em_upcoding,
    "Ghost Providers": page_ghost_providers,
    "Brooklyn T1019": page_brooklyn,
    "Minnesota Fraud": page_minnesota,
    "Service Analysis": page_service_analysis,
    "Top Providers": page_top_providers,
    "Temporal Anomalies": page_temporal,
    "Individual Outliers": page_outliers,
}

if page in PAGE_MAP:
    PAGE_MAP[page]()