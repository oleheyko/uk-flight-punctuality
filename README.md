# UK Flight Punctuality Analytics

This repository is the capstone project for Data Engineering Zoomcamp by [DataTalks.Club](https://www.datatalks.club/). It contains a data pipeline and analytics project focused on UK flight punctuality. The project ingests flight data from the UK Civil Aviation Authority, processes it, and provides insights through a Streamlit dashboard.

# Problem Statement
As a frequent flyer, the question arises: which UK airlines, airports consistently delay their flights, impacting passenger experience and time? This analysis aims to identify patterns in UK flight delays by airline, airport, and time period. 

# Requirements
- Unix-based system (macOS, Linux). Alternatively, you can use GitHub Codespaces.
- A Google Cloud Platform account with the following APIs enabled: BigQuery API, Cloud Storage API, and Cloud Run API. 

# Technologies
- **Data Ingestion**: Python (pandas, BeautifulSoup, requests)
- **Data Warehouse**: Google BigQuery
- **Data Transformation**: dbt (data build tool)
- **Dashboard & Visualization**: Streamlit
- **Infrastructure as Code**: Terraform
- **Containerization**: Docker, Docker Compose
- **Cloud Platform**: Google Cloud Platform (BigQuery, Cloud Storage, Cloud Run, Container Registry)
- **Package Management**: uv
- **CI/CD**: GitHub Actions
- **Orchestration**: GitHub Workflows

# Project Structure
- `ingest/`: Data ingestion and processing code. Can be run locally or triggered in Cloud Run via GitHub workflow.
- `dbt/`: dbt project for data transformation and modeling. Runs locally or on Cloud Run via GitHub workflow.
- `dashboard/`: Streamlit app for data visualization and analytics. Runs locally or on Cloud Run via GitHub workflow.
- `infra/`: Terraform configuration for provisioning infrastructure on Google Cloud Platform.
- `set_up.py`: Setup script for the project environment and profiles. Configures environment variables, dbt profiles, and builds Docker images for the ingest, dbt, and dashboard applications, pushing them to Google Container Registry for use in Cloud Run.

# Getting Started
1. Create a Google Cloud project and enable the required APIs (BigQuery, Cloud Storage, Cloud Run). Reference: [DE Zoomcamp 1.3.2 - Terraform Basics](https://youtu.be/Y2ux7gq3Z0o?si=5r3IQlOst9R9p_sk). Note your project ID.
2. Create a service account with the following permissions: Artifact Registry Admin, BigQuery Admin, Storage Admin, Cloud Run Admin, and IAM Admin. Generate and download a JSON key, then place it in the `keys/` folder and name it `my-creds.json`.
3. Create a virtual environment and install dependencies using `uv sync` in the root of the repository.
4. Set the `GCP_PROJECT` environment variable to your Google Cloud project ID in the `.env` file. You can add environment variables to a `.env` file in the root of the repository. Please, only update the `GCP_PROJECT` variable in the provided `.env` template and do not change other variables, as they have default values that work with the provided Terraform configuration.
5. Export Google Application Credentials environment variable to point to your service account key file. This is required for local runs of the ingestion, dbt, and dashboard applications. For example:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keys/my-creds.json"
```
You can copy this path by right-clicking on the `my-creds.json` file in your file explorer and selecting "Copy Path".
6. Configure Terraform to use your Google Cloud project by setting the `project` variable in `infra/variables.tf` to your project ID.
7. Provision infrastructure using Terraform. Run `terraform init` and `terraform apply` in the `infra/` directory. This creates a BigQuery dataset, Cloud Storage bucket, and Cloud Run services.
8. Create github secrets for your repository. This is for github workflows to authenticate with Google Cloud and run ingestion/dbt/dashboard in Cloud Run. The required secrets are:
   - `GCP_PROJECT`: Your Google Cloud project ID
   - `GCP_REGION`: europe-west2
   - `GCP_SA_KEY`: The contents of your service account JSON key file
To find secrets, please, navigate to your repository on GitHub -> Settings -> Secrets and variables -> Actions -> New repository secret.
9. Run `uv run set_up.py --project <YOUR_GCP_PROJECT_ID>` to set up the environment. This creates dbt profiles for BigQuery authentication and builds Docker images for the ingest, dbt, and dashboard applications, pushing them to Google Container Registry.


After setup, you can run the ingestion, dbt, and Streamlit dashboard locally or trigger GitHub workflows to run them in Cloud Run.

## Running Locally / Codespaces
- Ingest data: `uv run ingest/main.py`. This will download the flight data, process it, and load it into BigQuery for the specified year(s).
After the ingestion, the Cloud Storage will look:
![image](readme_figs/cloud_storage_after_ingest.png)
The BigQuery dataset will contain tables like `punctuality_data_2000`, `punctuality_data_2001`, etc. with the raw ingested data under the configured dataset.
![image](readme_figs/bq_after_ingestion.png)
- Create a unioned view in BigQuery for all years using `uv run ingest/main.py --normalize-all-years`.
- Navigate to dbt folder through the terminal - `cd dbt`. Run dbt transformations: `uv run dbt run`.
- Run Streamlit dashboard: `uv run streamlit run dashboard/app.py`. This will start the Streamlit app locally, which you can access at `http://localhost:8501` or the link provided in the terminal output. In VS Code, you can also click on Ports in the bottom panel, find the forwarded port for Streamlit, and click "Open in Browser" to access the dashboard. If your browser does not open the dashboard, try to use the other browser. 

## Running in Cloud Run via GitHub Workflows
- Trigger the ingestion workflow: Go to the "Actions" tab in your GitHub repository, find the workflow named "Ingest Data to BigQuery", and click "Run workflow". This will execute the ingestion process in Cloud Run, which will download, process, and load the flight data into BigQuery. Go to Cloud Run in your Google Cloud Console to monitor the execution of the ingestion service, through 'View logs'. After the workflow completes, check BigQuery to see the newly ingested tables.
![image](readme_figs/cloud_run_ingestion.png)
- After completion of the ingestion workflow, trigger the dbt workflow: Go to the "Actions" tab, find the workflow named "Scheduled GCP dbt run", and click "Run workflow". This will execute the dbt transformations in Cloud Run, creating the necessary tables and views for the dashboard. Monitor the execution in Cloud Run and check BigQuery after completion to see the new tables/views created by dbt.
![image](readme_figs/cloud_run_dbt.png)
- Finally, trigger the dashboard workflow: Go to the "Actions" tab, find the workflow named "GCP dashboard run", and click "Run workflow". This will deploy the Streamlit dashboard to Cloud Run. After deployment, navigate to Cloud Run in your Google Cloud Console, find the service named "dashboard-service", and click on it. You will see a URL for the deployed dashboard. Click on the URL to access the Streamlit dashboard running in Cloud Run.
![image](readme_figs/cloud_run_dashboard_service.png)
![image](readme_figs/cloud_run_dashboard_service_status.png)

# Data Model
dbt models are defined in the `dbt/models/` directory. The repo stages a
year-by-year unioned source into dbt and builds a small set of final marts
consumed by the Streamlit dashboard.

The raw unioned source table (`punctuality_data_all_years`) is staged in dbt as
`stg_punctuality_data`. The staging model is materialized as a table and is
partitioned by `reporting_period_date` (date) and clustered by
`origin_destination` and `airline_name`:

- Partitioning: enables date-range pruning so queries that target recent months
   or years scan far less data (reducing BigQuery cost and improving latency).
- Clustering: colocates rows by origin/destination and airline, which speeds
   aggregations and group-by operations used by downstream models.

A lightweight intermediate view, `int_scheduled_flights`, filters the staged
data to scheduled flights only (`scheduled_charter = 'S'`) and serves as the
canonical source for mart-level aggregations.

Final mart models used by the Streamlit dashboard (only these are required by
the app):

- `fct_delay_over_years` — annual average delay (used for the "Annual Average
   Delay Trend" chart).
- `fct_delay_vs_month_last_year` — monthly average delays for the most recent
   year (used for the "Average Delay by Month (Last Year)" chart).
- `fct_recent_airport_delays` — average delay per reporting airport for the
   most recent month (used for the "Recent Airport Delays" treemap).
- `fct_recent_number_tracked_flights` — total tracked flights in the most
   recent period (dashboard metric).
- `fct_recent_published_date` — human-readable most-recent reporting month/year
   (dashboard metric).

Materialization summary:

- Staging: `stg_punctuality_data` is a partitioned & clustered table to
   optimize cost and query performance for time-based analytics.
- Intermediate: `int_scheduled_flights` is a view to keep transformations
   lightweight and avoid duplicating storage for filtered data.
- Marts: final models are materialized as tables to provide fast, stable read
   performance for the dashboard.

Only the models listed above are included in the deployed Streamlit app; other
dbt models in the project exist for exploration or auxiliary analyses and are
not required for dashboard rendering.

# Architecture

The high-level architecture of the project (ingest → storage → transformation → dashboard):

```mermaid
flowchart LR
  subgraph Ingest
    direction LR
    A["Ingest service<br/>(ingest/main.py)"]
    GCS[("Cloud Storage")]
    A -->|"Upload CSVs"| GCS
  end

   GCS -->|"BigQuery load jobs"| B["BigQuery<br/>raw dataset"]

  subgraph Transform
    direction LR
    C["dbt<br/>(Cloud Run job / CI)"]
    D["BigQuery<br/>Marts"]
    B --> C --> D
  end

  subgraph Serve
    direction LR
    E["Streamlit<br/>(dashboard/app.py on GCP)"]
    F["Browser"]
    D --> E -->|"Users"| F
  end

   O["GitHub Actions<br/>(Scheduler / Orchestrator)"] --> A
   O --> C
```
