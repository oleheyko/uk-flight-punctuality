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
- Run Streamlit dashboard: `uv run streamlit run dashboard/app.py`. This will start the Streamlit app locally, which you can access at `http://localhost:8501`.

## Running in Cloud Run via GitHub Workflows
