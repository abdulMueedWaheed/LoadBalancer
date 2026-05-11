"""
Load Balancer Dashboard
=======================
Streamlit dashboard for visualizing load balancer experiment results.

Run:
    streamlit run dashboard.py
"""

import os
import glob
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Load Balancer Dashboard",
    page_icon="⚡",
    layout="wide",
)

LOG_DIR = "logs"

# ── Helpers ───────────────────────────────────────────────────────────────────
@st.cache_data
def load_client_csvs() -> dict[str, pd.DataFrame]:
    """Load all client_*_run*.csv files, keyed by label."""
    pattern = os.path.join(LOG_DIR, "client_*_run*.csv")
    files = sorted(glob.glob(pattern))
    frames = {}
    for f in files:
        basename = os.path.basename(f)            # client_rr_run1.csv
        label = basename.replace("client_", "").replace(".csv", "")  # rr_run1
        df = pd.read_csv(f)
        df["source_file"] = basename
        df["label"] = label
        # Extract algo name from label (e.g., lc_db_mixed_50_50...)
        if "_db_" in label:
            algo = label.split("_db_", 1)[0]
        else:
            algo = label.split("_")[0]
        df["algorithm"] = algo
        frames[label] = df
    return frames


@st.cache_data
def load_controller_csvs() -> dict[str, pd.DataFrame]:
    """Load all {rr,lc,ucb}_logs.csv files from the controller side."""
    frames = {}
    for prefix in ("rr", "lc", "ucb", "ma", "metric_aware"):
        path = os.path.join(LOG_DIR, f"{prefix}_logs.csv")
        if os.path.exists(path):
            df = pd.read_csv(path)
            df["algorithm"] = prefix
            frames[prefix] = df
    return frames


@st.cache_data
def load_failure_csvs() -> dict[str, pd.DataFrame]:
    """Load all {rr,lc,ucb}_failures.csv files."""
    frames = {}
    for prefix in ("rr", "lc", "ucb", "ma", "metric_aware"):
        path = os.path.join(LOG_DIR, f"{prefix}_failures.csv")
        if os.path.exists(path):
            df = pd.read_csv(path)
            if not df.empty:
                df["algorithm"] = prefix
                frames[prefix] = df
    return frames


def algo_label(code: str) -> str:
    return {
        "rr": "Round Robin", 
        "lc": "Least Connections", 
        "ucb": "UCB",
        "ma": "Metric-Aware",
        "metric_aware": "Metric-Aware"
    }.get(code, code)


# ── Load data ─────────────────────────────────────────────────────────────────
client_data = load_client_csvs()
controller_data = load_controller_csvs()
failure_data = load_failure_csvs()

has_client = len(client_data) > 0
has_controller = len(controller_data) > 0

if not has_client and not has_controller:
    st.error("No log files found in `logs/`. Run some experiments first!")
    st.stop()

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("⚡ Load Balancer Dashboard")
st.sidebar.markdown("---")

data_source = st.sidebar.radio(
    "Data source",
    ["Client-side logs", "Controller-side logs"],
    index=0 if has_client else 1,
)

if data_source == "Client-side logs" and has_client:
    selected_runs = st.sidebar.multiselect(
        "Select experiment runs",
        options=list(client_data.keys()),
        default=list(client_data.keys()),
    )
    if not selected_runs:
        st.warning("Select at least one run.")
        st.stop()
    df = pd.concat([client_data[r] for r in selected_runs], ignore_index=True)
    latency_col = "latency_ms"
    node_col = "node"
    algo_col = "algorithm"
    success_col = "success"

elif data_source == "Controller-side logs" and has_controller:
    selected_algos = st.sidebar.multiselect(
        "Select algorithms",
        options=list(controller_data.keys()),
        default=list(controller_data.keys()),
        format_func=algo_label,
    )
    if not selected_algos:
        st.warning("Select at least one algorithm.")
        st.stop()
    df = pd.concat([controller_data[a] for a in selected_algos], ignore_index=True)
    latency_col = "latency_ms"
    node_col = "node_selected"
    algo_col = "algorithm"
    success_col = "success"
else:
    st.error("Selected data source has no logs.")
    st.stop()

# Make sure latency is numeric
df[latency_col] = pd.to_numeric(df[latency_col], errors="coerce")
df = df.dropna(subset=[latency_col])

# Parse timestamps
if "timestamp" in df.columns:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

# ── Color palette ─────────────────────────────────────────────────────────────
ALGO_COLORS = {
    "rr": "#636EFA",
    "lc": "#EF553B",
    "ucb": "#00CC96",
    "ma": "#AB63FA",
    "metric_aware": "#AB63FA",
}
NODE_COLORS = px.colors.qualitative.Set2

# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════
st.title("⚡ Load Balancer Performance Dashboard")
st.caption(f"Data source: **{data_source}** · {len(df)} requests loaded")

# ── KPI row ───────────────────────────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)

total_reqs = len(df)
if success_col in df.columns:
    success_count = df[df[success_col].astype(str).str.lower().isin(["true", "success", "1"])].shape[0]
else:
    success_count = total_reqs
fail_count = total_reqs - success_count

avg_lat = df[latency_col].mean()
p95_lat = df[latency_col].quantile(0.95)
max_lat = df[latency_col].max()

col1.metric("Total Requests", f"{total_reqs:,}")
col2.metric("Success Rate", f"{success_count / total_reqs * 100:.1f}%")
col3.metric("Avg Latency", f"{avg_lat:.1f} ms")
col4.metric("P95 Latency", f"{p95_lat:.1f} ms")
col5.metric("Max Latency", f"{max_lat:.1f} ms")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# 1. LATENCY CHARTS
# ══════════════════════════════════════════════════════════════════════════════
st.header("📊 Latency Analysis")

lat_tab1, lat_tab2, lat_tab3 = st.tabs(["Distribution", "Per Request", "Box Plot"])

with lat_tab1:
    fig = px.histogram(
        df, x=latency_col, color=algo_col,
        nbins=40,
        color_discrete_map=ALGO_COLORS,
        labels={latency_col: "Latency (ms)", algo_col: "Algorithm"},
        title="Latency Distribution",
        barmode="overlay",
        opacity=0.7,
    )
    fig.update_layout(template="plotly_dark", height=450)
    st.plotly_chart(fig, use_container_width=True)

with lat_tab2:
    if "req_id" in df.columns:
        x_col = "req_id"
    else:
        x_col = df.index.name or "index"
        df = df.reset_index()

    fig = px.scatter(
        df, x=x_col if x_col in df.columns else df.index,
        y=latency_col, color=algo_col,
        color_discrete_map=ALGO_COLORS,
        labels={latency_col: "Latency (ms)", "req_id": "Request #", algo_col: "Algorithm"},
        title="Latency per Request",
        opacity=0.7,
    )
    # Add avg line per algorithm
    for algo in df[algo_col].unique():
        avg = df[df[algo_col] == algo][latency_col].mean()
        fig.add_hline(
            y=avg,
            line_dash="dash",
            line_color=ALGO_COLORS.get(algo, "gray"),
            annotation_text=f"{algo_label(algo)} avg: {avg:.0f}ms",
        )
    fig.update_layout(template="plotly_dark", height=450)
    st.plotly_chart(fig, use_container_width=True)

with lat_tab3:
    fig = px.box(
        df, x=algo_col, y=latency_col, color=algo_col,
        color_discrete_map=ALGO_COLORS,
        labels={latency_col: "Latency (ms)", algo_col: "Algorithm"},
        title="Latency Box Plot by Algorithm",
        points="outliers",
    )
    fig.update_layout(template="plotly_dark", height=450)
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# 2. THROUGHPUT
# ══════════════════════════════════════════════════════════════════════════════
st.header("🚀 Throughput Analysis")

if "timestamp" in df.columns and df["timestamp"].notna().any():
    tp_tab1, tp_tab2 = st.tabs(["Over Time", "Summary"])

    with tp_tab1:
        # Compute throughput per second
        tp_df = df.copy()
        tp_df["second"] = tp_df["timestamp"].dt.floor("s")
        throughput_ts = tp_df.groupby(["second", algo_col]).size().reset_index(name="requests_per_sec")

        fig = px.line(
            throughput_ts, x="second", y="requests_per_sec", color=algo_col,
            color_discrete_map=ALGO_COLORS,
            markers=True,
            labels={"requests_per_sec": "Requests / sec", "second": "Time", algo_col: "Algorithm"},
            title="Throughput Over Time (requests per second)",
        )
        fig.update_layout(template="plotly_dark", height=400)
        st.plotly_chart(fig, use_container_width=True)

    with tp_tab2:
        # Summary bar chart
        summary_rows = []
        for algo in df[algo_col].unique():
            adf = df[df[algo_col] == algo]
            ts = adf["timestamp"]
            if ts.notna().sum() >= 2:
                duration = (ts.max() - ts.min()).total_seconds()
                duration = max(duration, 0.001)
                tp = len(adf) / duration
            else:
                tp = len(adf)
            summary_rows.append({"Algorithm": algo_label(algo), "algo_code": algo, "Throughput (req/s)": round(tp, 2)})

        tp_summary = pd.DataFrame(summary_rows)
        fig = px.bar(
            tp_summary, x="Algorithm", y="Throughput (req/s)",
            color="algo_code",
            color_discrete_map=ALGO_COLORS,
            title="Average Throughput by Algorithm",
            text="Throughput (req/s)",
        )
        fig.update_layout(template="plotly_dark", height=400, showlegend=False)
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Timestamp data not available for throughput-over-time analysis.")
    # Still show a summary
    summary_rows = []
    for algo in df[algo_col].unique():
        adf = df[df[algo_col] == algo]
        summary_rows.append({"Algorithm": algo_label(algo), "algo_code": algo, "Requests": len(adf)})
    st.dataframe(pd.DataFrame(summary_rows))

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# 3. LOAD DISTRIBUTION
# ══════════════════════════════════════════════════════════════════════════════
st.header("🏗️ Load Distribution")

dist_tab1, dist_tab2, dist_tab3 = st.tabs(["Per Algorithm", "Overall", "Latency by Node"])

with dist_tab1:
    for algo in df[algo_col].unique():
        adf = df[df[algo_col] == algo]
        node_counts = adf[node_col].astype(str).value_counts().reset_index()
        node_counts.columns = ["Node", "Requests"]
        # Clean node names
        node_counts["Node"] = node_counts["Node"].str.replace("http://", "").str.replace(":8000", "")

        fig = px.pie(
            node_counts, names="Node", values="Requests",
            title=f"{algo_label(algo)} — Node Distribution",
            color_discrete_sequence=NODE_COLORS,
            hole=0.4,
        )
        fig.update_layout(template="plotly_dark", height=350)
        st.plotly_chart(fig, use_container_width=True)

with dist_tab2:
    # Grouped bar chart
    dist_df = df.copy()
    dist_df["node_clean"] = dist_df[node_col].astype(str).str.replace("http://", "").str.replace(":8000", "")
    grouped = dist_df.groupby([algo_col, "node_clean"]).size().reset_index(name="count")

    fig = px.bar(
        grouped, x="node_clean", y="count", color=algo_col,
        color_discrete_map=ALGO_COLORS,
        barmode="group",
        labels={"node_clean": "Node", "count": "Requests", algo_col: "Algorithm"},
        title="Request Count by Node (All Algorithms)",
        text="count",
    )
    fig.update_layout(template="plotly_dark", height=400)
    fig.update_traces(textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

with dist_tab3:
    dist_df2 = df.copy()
    dist_df2["node_clean"] = dist_df2[node_col].astype(str).str.replace("http://", "").str.replace(":8000", "")

    fig = px.box(
        dist_df2, x="node_clean", y=latency_col, color=algo_col,
        color_discrete_map=ALGO_COLORS,
        labels={"node_clean": "Node", latency_col: "Latency (ms)", algo_col: "Algorithm"},
        title="Latency Distribution by Node",
        points="outliers",
    )
    fig.update_layout(template="plotly_dark", height=450)
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# 4. FAILURE ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
if failure_data:
    st.header("⚠️ Failure Analysis")
    fail_df = pd.concat(failure_data.values(), ignore_index=True)

    fcol1, fcol2 = st.columns(2)
    with fcol1:
        fig = px.histogram(
            fail_df, x="error_type", color="algorithm",
            color_discrete_map=ALGO_COLORS,
            title="Failure Types",
            labels={"error_type": "Error Type", "algorithm": "Algorithm"},
            barmode="group",
        )
        fig.update_layout(template="plotly_dark", height=350)
        st.plotly_chart(fig, use_container_width=True)

    with fcol2:
        node_fails = fail_df["node"].astype(str).str.replace("http://", "").str.replace(":8000", "")
        node_fail_counts = node_fails.value_counts().reset_index()
        node_fail_counts.columns = ["Node", "Failures"]
        fig = px.bar(
            node_fail_counts, x="Node", y="Failures",
            title="Failures by Node",
            text="Failures",
            color_discrete_sequence=["#EF553B"],
        )
        fig.update_layout(template="plotly_dark", height=350)
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# 5. SUMMARY TABLE
# ══════════════════════════════════════════════════════════════════════════════
st.header("📋 Summary Statistics")

summary_data = []
for algo in df[algo_col].unique():
    adf = df[df[algo_col] == algo]
    lats = adf[latency_col]

    # Node distribution string
    node_dist = adf[node_col].astype(str).value_counts()
    node_dist_clean = {
        k.replace("http://", "").replace(":8000", ""): v for k, v in node_dist.items()
    }

    summary_data.append({
        "Algorithm": algo_label(algo),
        "Total Requests": len(adf),
        "Avg Latency (ms)": round(lats.mean(), 1),
        "Max Latency (ms)": round(lats.max(), 1),
        "P95 Latency (ms)": round(lats.quantile(0.95), 1),
        "P99 Latency (ms)": round(lats.quantile(0.99), 1),
        "Node Distribution": str(node_dist_clean),
    })

summary_df = pd.DataFrame(summary_data)
st.dataframe(summary_df, use_container_width=True, hide_index=True)

# ── Raw data expander ─────────────────────────────────────────────────────────
with st.expander("🔍 View Raw Data"):
    st.dataframe(df, use_container_width=True, height=400)

st.sidebar.markdown("---")
st.sidebar.caption("Load Balancer Dashboard v1.0")
