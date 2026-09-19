import streamlit as st
import pandas as pd
import psycopg2

st.set_page_config(page_title="NYC 311 Complaints", layout="wide")
st.title("NYC 311 Service Requests Dashboard")
st.caption("Top complaint types by borough | Airflow + dbt + Postgres + dbt")

@st.cache_data(ttl=3600)
def load_data():
    # Read from CSV (works on Streamlit Cloud)
    df = pd.read_csv("data.csv", parse_dates=["request_month"])
    return df   

df = load_data()

if df.empty:
    st.warning("No data yet. Run the Airflow DAG first.")
    st.stop()

# --- Sidebar ---
st.sidebar.header("Filters")
boroughs = sorted(df["borough"].unique().tolist())
selected = st.sidebar.multiselect("Boroughs", boroughs, default=boroughs)
df_f = df[df["borough"].isin(selected)]

# --- KPIs ---
c1, c2, c3 = st.columns(3)
c1.metric("Total Complaints", f"{df_f['complaint_count'].sum():,}")
c2.metric("Boroughs Shown", len(selected))
c3.metric("Months Covered", df_f['request_month'].nunique())

# --- Chart 1: Top 10 complaint types (horizontal bar, easy to read) ---
st.subheader("Top 10 Complaint Types (All Time)")
top10 = df_f.groupby("complaint_type")["complaint_count"].sum().nlargest(10).reset_index()
top10 = top10.sort_values("complaint_count")
st.bar_chart(top10.set_index("complaint_type")["complaint_count"], horizontal=True)

# --- Chart 2: Complaints by Borough (grouped comparison) ---
st.subheader("Total Complaints by Borough")
boro_totals = df_f.groupby("borough")["complaint_count"].sum().reset_index()
st.bar_chart(boro_totals.set_index("borough")["complaint_count"])

# --- Chart 3: Monthly trend (line chart — this is where the story is) ---
st.subheader("Monthly Complaint Trend by Type")
monthly = df_f.pivot_table(
    index="request_month", columns="complaint_type",
    values="complaint_count", aggfunc="sum"
).fillna(0)
st.line_chart(monthly)

# --- Chart 4: Heatmap-style table (borough × type) ---
st.subheader("Complaints: Borough × Type (Latest Month)")
latest = df_f["request_month"].max()
latest_df = df_f[df_f["request_month"] == latest]
pivot = latest_df.pivot_table(
    index="borough", columns="complaint_type",
    values="complaint_count", aggfunc="sum"
).fillna(0)
st.dataframe(pivot, use_container_width=True)

# --- Raw table at bottom ---
with st.expander("Raw Data"):
    st.dataframe(df_f, use_container_width=True, hide_index=True)   
