import os
from typing import Optional

import pandas as pd
import streamlit as st
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

try:
    import plotly.express as px
except ImportError:  # plotly is optional for nicer charts
    px = None

st.set_page_config(
    page_title="UK Flight Punctuality",
    page_icon="✈️",
    layout="wide",
)

st.title("UK Flight Punctuality Dashboard")
st.write("Real-time insights into UK flight punctuality data from BigQuery.")


def get_bq_client(project: Optional[str] = None) -> bigquery.Client:
    return bigquery.Client(project=project) if project else bigquery.Client()


def load_dbt_model_table(project: Optional[str], dataset: str, table: str) -> pd.DataFrame:
    client = get_bq_client(project)
    table_ref = client.dataset(dataset).table(table)
    try:
        return client.list_rows(table_ref).to_dataframe(create_bqstorage_client=False)
    except NotFound as exc:
        raise ValueError(
            f"Table not found: {project or client.project}.{dataset}.{table}. "
            "Ensure dbt has built the model and the dataset/table exists in BigQuery."
        ) from exc


def build_avg_delay_chart(df: pd.DataFrame):
    if px is None:
        return None

    chart_data = df.copy()
    chart_data["year"] = chart_data["year"].astype(str)

    fig = px.line(
        chart_data,
        x="year",
        y="avg_delay_mins",
        title="Annual Average Delay Trend",
        labels={"year": "Year", "avg_delay_mins": "Average Delay (minutes)"},
        markers=True,
    )
    fig.update_traces(line=dict(width=3, color="#1f77b4"), marker=dict(size=8))
    fig.update_layout(
        xaxis=dict(tickmode="linear", showgrid=False),
        yaxis=dict(tickformat=",.0f", gridcolor="lightgrey"),
        margin=dict(l=40, r=20, t=60, b=40),
        title=dict(font=dict(size=18)),
    )
    fig.update_yaxes(zeroline=False)
    return fig


def build_monthly_delay_chart(df: pd.DataFrame):
    if px is None:
        return None

    # Ensure data is sorted by month
    df = df.sort_values('month')

    fig = px.bar(
        df,
        x="month_name",
        y="avg_delay_mins",
        title="Average Delay by Month (Last Year)",
        labels={"month_name": "Month", "avg_delay_mins": "Average Delay (minutes)"},
        color="avg_delay_mins",
        color_continuous_scale='Blues',
    )
    fig.update_layout(
        xaxis=dict(categoryorder='array', categoryarray=[
            'January', 'February', 'March', 'April', 'May', 'June',
            'July', 'August', 'September', 'October', 'November', 'December'
        ]),
        yaxis=dict(tickformat=",.0f", gridcolor="lightgrey"),
        margin=dict(l=40, r=20, t=60, b=40),
        title=dict(font=dict(size=18)),
    )
    fig.update_yaxes(zeroline=False)
    return fig


def build_airport_delays_treemap(df: pd.DataFrame):
    if px is None:
        return None

    # Sort by delay descending to prioritize larger delays in bigger boxes
    df_sorted = df.sort_values('avg_delay_mins', ascending=False)

    fig = px.treemap(
        df_sorted,
        path=['reporting_airport'],
        values='avg_delay_mins',
        title="Airport Delays in Most Recent Month",
        color='avg_delay_mins',
        color_continuous_scale='Reds',
    )
    fig.update_traces(
        textfont_size=12,
        textposition="middle center",
        textinfo="label+value"
    )
    fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
    return fig
try:
    # Try Streamlit secrets first (for production)
    project_id = st.secrets.get("bigquery", {}).get("project_id") or os.getenv("BIGQUERY_PROJECT", "")
    dataset = st.secrets.get("bigquery", {}).get("dataset") or os.getenv("BIGQUERY_DATASET", "flight_data")
except:
    # Fallback to environment variables
    project_id = os.getenv("BIGQUERY_PROJECT", "")
    dataset = os.getenv("BIGQUERY_DATASET", "flight_data")

# Load data automatically on startup
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_dashboard_data():
    try:
        # Load main delay data
        df = load_dbt_model_table(project_id or None, dataset, "fct_delay_over_years")

        # Load metrics
        recent_date_df = load_dbt_model_table(project_id or None, dataset, "fct_recent_published_date")
        tracked_flights_df = load_dbt_model_table(project_id or None, dataset, "fct_recent_number_tracked_flights")

        # Load monthly delay data
        monthly_delay_df = load_dbt_model_table(project_id or None, dataset, "fct_delay_vs_month_last_year")

        return df, recent_date_df, tracked_flights_df, monthly_delay_df
    except Exception as e:
        st.error(f"Failed to load data from BigQuery: {str(e)}")
        st.info("Please ensure your BigQuery credentials are properly configured.")
        return None, None, None, None

# Load data
df, recent_date_df, tracked_flights_df, monthly_delay_df = load_dashboard_data()

if df is not None:
    # Display metrics
    col1, col2 = st.columns(2)
    with col1:
        if recent_date_df is not None and not recent_date_df.empty and "recent_published_date" in recent_date_df.columns:
            st.metric("Most Recent Data", recent_date_df["recent_published_date"].iloc[0])
        else:
            st.metric("Most Recent Data", "N/A")

    with col2:
        if tracked_flights_df is not None and not tracked_flights_df.empty and "total_tracked_flights" in tracked_flights_df.columns:
            st.metric("Total Tracked Flights", f"{tracked_flights_df['total_tracked_flights'].iloc[0]:,}")
        else:
            st.metric("Total Tracked Flights", "N/A")

    st.divider()

    # Annual average delay chart
    st.subheader("Annual Average Delay Trend")

    if not df.empty and "year" in df.columns and "avg_delay_mins" in df.columns:
        chart = build_avg_delay_chart(df)
        if chart is not None:
            st.plotly_chart(chart, use_container_width=True)
        else:
            st.line_chart(df.set_index("year")["avg_delay_mins"])
    else:
        st.warning("No delay data available to display.")

    st.divider()

    # Monthly average delay chart for last year
    st.subheader("Average Delay by Month (Last Year)")

    if monthly_delay_df is not None and not monthly_delay_df.empty and "month_name" in monthly_delay_df.columns and "avg_delay_mins" in monthly_delay_df.columns:
        monthly_chart = build_monthly_delay_chart(monthly_delay_df)
        if monthly_chart is not None:
            st.plotly_chart(monthly_chart, use_container_width=True)
        else:
            st.bar_chart(monthly_delay_df.set_index("month_name")["avg_delay_mins"])
    else:
        st.warning("No monthly delay data available to display.")

    st.divider()
    st.subheader("Recent Airport Delays")

    try:
        df_treemap = load_dbt_model_table(project_id or None, dataset, "fct_recent_airport_delays")
        if not df_treemap.empty and "reporting_airport" in df_treemap.columns and "avg_delay_mins" in df_treemap.columns:
            treemap_chart = build_airport_delays_treemap(df_treemap)
            if treemap_chart is not None:
                st.plotly_chart(treemap_chart, use_container_width=True)
            else:
                st.write("Plotly not available for treemap visualization.")
        else:
            st.write("No airport delay data available.")
    except Exception as error:
        st.error("Unable to load airport delay data.")
        st.exception(error)

else:
    st.error("Unable to load dashboard data. Please check your BigQuery configuration.")

# Footer
st.divider()
st.caption("Dashboard automatically refreshes data every hour. Built with Streamlit and powered by BigQuery.")
