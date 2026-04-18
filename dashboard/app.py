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
st.write(
    "Display results from the dbt model `fct_delay_over_years` in BigQuery. "
    "This app loads the dbt-generated view and shows annual average delay."
)


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
        title="Annual average delay",
        labels={"year": "Year", "avg_delay_mins": "Average delay (mins)"},
        markers=True,
    )
    fig.update_traces(line=dict(width=3, color="#1f77b4"), marker=dict(size=8))
    fig.update_layout(
        xaxis=dict(tickmode="linear", showgrid=False),
        yaxis=dict(tickformat=",.0f", gridcolor="lightgrey"),
        margin=dict(l=40, r=20, t=60, b=40),
        # plot_bgcolor="white",
        # paper_bgcolor="white",
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


default_project = os.getenv("BIGQUERY_PROJECT", "")
default_dataset = os.getenv("BIGQUERY_DATASET", "flight_data")
default_table = os.getenv("BIGQUERY_MODEL_TABLE", "fct_delay_over_years")

gcp_project = st.sidebar.text_input("BigQuery project", value=default_project)

gcs_dataset = st.sidebar.text_input("Dataset", value=default_dataset)

st.sidebar.markdown(
    "This dashboard loads only the dbt model `fct_delay_over_years`.\n"
    "Leave the project blank to use ADC project, or set it explicitly."
)

if st.sidebar.button("Load dbt model results"):
    try:
        with st.spinner("Loading dbt model from BigQuery..."):
            df = load_dbt_model_table(gcp_project or None, gcs_dataset, default_table)

        st.success("Loaded `fct_delay_over_years` successfully")

        # Load and display metrics
        try:
            recent_date_df = load_dbt_model_table(gcp_project or None, gcs_dataset, "fct_recent_published_date")
            tracked_flights_df = load_dbt_model_table(gcp_project or None, gcs_dataset, "fct_recent_number_tracked_flights")

            col1, col2 = st.columns(2)
            with col1:
                if not recent_date_df.empty and "recent_published_date" in recent_date_df.columns:
                    st.metric("Most Recent Data", recent_date_df["recent_published_date"].iloc[0])
                else:
                    st.metric("Most Recent Data", "N/A")

            with col2:
                if not tracked_flights_df.empty and "total_tracked_flights" in tracked_flights_df.columns:
                    st.metric("Total Tracked Flights", f"{tracked_flights_df['total_tracked_flights'].iloc[0]:,}")
                else:
                    st.metric("Total Tracked Flights", "N/A")

        except Exception as error:
            st.warning("Unable to load metrics data. Continuing with main dashboard...")
            st.exception(error)

        st.subheader("Annual average delay")

        if not df.empty and "year" in df.columns and "avg_delay_mins" in df.columns:
            chart = build_avg_delay_chart(df)
            if chart is not None:
                st.plotly_chart(chart, use_container_width=True)
            else:
                st.line_chart(df.set_index("year")["avg_delay_mins"])

        st.markdown("---")
        st.subheader("Recent Airport Delays Treemap")

        try:
            df_treemap = load_dbt_model_table(gcp_project or None, gcs_dataset, "fct_recent_airport_delays")
            if not df_treemap.empty and "reporting_airport" in df_treemap.columns and "avg_delay_mins" in df_treemap.columns:
                treemap_chart = build_airport_delays_treemap(df_treemap)
                if treemap_chart is not None:
                    st.plotly_chart(treemap_chart, use_container_width=True)
                else:
                    st.write("Plotly not available for treemap visualization.")
            else:
                st.write("Required columns not found in fct_recent_airport_delays data.")
        except Exception as error:
            st.error("Unable to load fct_recent_airport_delays from BigQuery.")
            st.exception(error)

        st.markdown("---")
        st.write("Use the section above to review the dbt model output and add any extra visualizations.")
    except Exception as error:
        st.error("Unable to load the dbt model results from BigQuery.")
        st.exception(error)

st.sidebar.markdown(
    "---\n"
    "**Credentials**:\n"
    "- Set `GOOGLE_APPLICATION_CREDENTIALS` to your service account JSON file, or\n"
    "- run `gcloud auth application-default login` locally.\n"
    "- If deployed on GCP, application default credentials are used automatically."
)

st.header("How to use")
st.markdown(
    "1. Build the dbt model `fct_delay_over_years` with `dbt run` or `dbt build`.\n"
    "2. Confirm the model exists in your BigQuery dataset.\n"
    "3. Enter the project and dataset values in the sidebar.\n"
    "4. Click `Load dbt model results` to display the view."
)

with st.expander("dbt model SQL"):
    st.code(
        """
-- Table shows how average delay minutes change over the years
{{ config(materialized='view') }}
select
    year,
    round(sum(number_flights_matched * average_delay_mins) / nullif(sum(number_flights_matched), 0), 0) as avg_delay_mins
from {{ ref('int_scheduled_flights') }}
where number_flights_matched > 0
group by year
order by year
"""
    )

with st.expander("Recent Airport Delays Treemap SQL"):
    st.code(
        """
-- Implement recent airport delays table, which shows the average delay minutes for flights departing from each airport in the most recent month.
{{ config(materialized='view') }}
select
    reporting_airport,
    round(sum(number_flights_matched * average_delay_mins) / nullif(sum(number_flights_matched), 0), 0) as avg_delay_mins
from {{ ref('stg_punctuality_data') }}
where year = (select max(year) from {{ ref('stg_punctuality_data') }})
  and month = (
      select max(month)
      from {{ ref('stg_punctuality_data') }}
      where year = (select max(year) from {{ ref('stg_punctuality_data') }})
  )
group by reporting_airport
"""
    )

st.info("Run this app with: `streamlit run dashboard/app.py`")
