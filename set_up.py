#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict
import yaml

ROOT = Path(__file__).resolve().parent
INGEST_DIR = ROOT / "ingest"
DBT_DIR = ROOT / "dbt"
DASHBOARD_DIR = ROOT / "dashboard"
ENV_FILE = ROOT / ".env"


DEFAULT_ENV: Dict[str, str] = {
    "BUCKET_NAME": "uk-flight-punctuality-raw-data-lake",
    "CAA_BASE_URL": "https://www.caa.co.uk/data-and-analysis/uk-aviation-market/flight-punctuality/uk-flight-punctuality-statistics/",
    "CAA_YEAR_START": "2000",
    "CAA_YEAR_END": "2026",
    "GCS_PREFIX": "raw/",
    "OVERWRITE": "false",
    "REQUEST_TIMEOUT": "60",
    "BIGQUERY_DATASET": "flight_data",
    "BIGQUERY_TABLE_PREFIX": "punctuality_data_",
    "BIGQUERY_LOCATION": "EU",
    "GCP_PROJECT": ""  # No default for project since it's required
}


def check_program(name: str) -> None:
    if shutil.which(name) is None:
        print(f"Error: {name} is not installed or not on PATH.")
        sys.exit(1)


def write_env_file() -> None:
    if ENV_FILE.exists():
        print(f"Found existing {ENV_FILE}. Leaving it unchanged.")
        return

    ENV_FILE.parent.mkdir(parents=True, exist_ok=True)
    entries = [f"{key}={value}" for key, value in DEFAULT_ENV.items() if value]
    ENV_FILE.write_text("\n".join(entries) + ("\n" if entries else ""))
    print(f"Created {ENV_FILE} with default values.")
    print("Edit it before running the container if you want different bucket, year range, or BigQuery settings.")


def load_env_file() -> dict:
    """Read simple KEY=VALUE pairs from the repo `.env` file and inject them into os.environ.

    Returns a dict of the parsed values. Existing environment variables are preserved.
    """
    if not ENV_FILE.exists():
        return {}

    parsed: dict = {}
    for raw in ENV_FILE.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip()
        # Skip empty values so we don't overwrite existing env vars with blanks
        if val == "":
            continue
        parsed[key] = val
        # Do not overwrite existing environment variables
        if key not in os.environ:
            os.environ[key] = val

    return parsed


def run(cmd: list[str], cwd: Path | None = None, env: Dict[str, str] | None = None) -> None:
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, check=True, env=env or os.environ.copy())
    print(f"Finished: {' '.join(cmd)}")
    time.sleep(2)


def gcloud_auth_docker(region: str) -> None:
    host = f"{region}-docker.pkg.dev"
    credentials_path = Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", INGEST_DIR / "gcloud-key.json"))
    if not credentials_path.exists():
        print(
            "Error: Docker authentication is required to push to Artifact Registry. "
            "Set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON key file, or place the key at ingest/gcloud-key.json."
        )
        sys.exit(1)

    print(f"Using service account key {credentials_path} to log in to Docker host {host}.")
    time.sleep(2)
    subprocess.run(
        ["docker", "login", "-u", "_json_key", "--password-stdin", f"https://{host}"],
        cwd=ROOT,
        check=True,
        input=credentials_path.read_bytes(),
    )
    print("Docker login completed.")
    time.sleep(2)


def build_and_push(image: str, build_dir: Path) -> None:
    print(f"Building image {image} from {build_dir}")
    time.sleep(2)
    run(["docker", "build", "-t", image, "."], cwd=build_dir)
    print(f"Pushing image {image}")
    time.sleep(2)
    run(["docker", "push", image], cwd=build_dir)
    print(f"Completed build and push for {image}")
    time.sleep(2)


def artifact_repo_exists(project: str, region: str, repo: str) -> bool:
    try:
        from google.api_core.exceptions import NotFound
        from google.cloud import artifactregistry_v1
    except ImportError:
        print(
            "Error: google-cloud-artifact-registry is required for Artifact Registry operations. "
            "Install it with `pip install google-cloud-artifact-registry`."
        )
        sys.exit(1)

    client = artifactregistry_v1.ArtifactRegistryClient()
    name = f"projects/{project}/locations/{region}/repositories/{repo}"
    try:
        client.get_repository(name=name)
        return True
    except NotFound:
        return False


def ensure_artifact_repository(project: str, region: str, repo: str) -> None:
    try:
        from google.cloud import artifactregistry_v1
    except ImportError:
        print(
            "Error: google-cloud-artifact-registry is required for Artifact Registry operations. "
            "Install it with `pip install google-cloud-artifact-registry`."
        )
        sys.exit(1)

    if artifact_repo_exists(project, region, repo):
        print(f"Artifact Registry repository '{repo}' already exists in {region}.")
        return

    print(f"Creating Artifact Registry repository '{repo}' in {region}...")
    client = artifactregistry_v1.ArtifactRegistryClient()
    parent = f"projects/{project}/locations/{region}"
    repository = artifactregistry_v1.Repository(
        format_=artifactregistry_v1.Repository.Format.DOCKER,
        description="Docker repository for uk-flight-punctuality image",
    )
    operation = client.create_repository(
        parent=parent,
        repository_id=repo,
        repository=repository,
    )
    operation.result()
    print(f"Created Artifact Registry repository '{repo}' in {region}.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and push ingest and dbt Docker images to GCP Artifact Registry")
    parser.add_argument("--project", help="GCP project ID")
    parser.add_argument("--region", help="GCP region / Artifact Registry location")
    parser.add_argument("--repo", default="uk-flight-punctuality", help="Artifact Registry repository name")
    parser.add_argument("--image-name", default=None, help="Ingest Docker image name")
    parser.add_argument("--dbt-image-name", default=None, help="dbt Docker image name")
    parser.add_argument("--dashboard-image-name", default=None, help="Streamlit dashboard Docker image name")
    parser.add_argument("--tag", default="latest", help="Docker image tag")
    parser.add_argument(
        "--image",
        help="Full ingest container image URI. If provided, project/region/repo/image-name/tag are ignored.",
    )
    parser.add_argument(
        "--dbt-image",
        help="Full dbt container image URI.",
    )
    return parser.parse_args()


def create_streamlit_secrets_file(args: argparse.Namespace) -> None:
    secrets_dir = ROOT / ".streamlit"
    secrets_dir.mkdir(exist_ok=True)
    secrets_file = secrets_dir / "secrets.toml"
    if secrets_file.exists():
        print(f"Found existing {secrets_file}. Leaving it unchanged.")
        return
    
    bigquery_dataset = os.getenv("BIGQUERY_DATASET", "flight_data")
    if not bigquery_dataset:
        raise ValueError("BIGQUERY_DATASET environment variable is required to create Streamlit secrets file.")

    secrets_content = f"""[bigquery]\nproject_id = "{args.project}"\ndataset = "{bigquery_dataset}"\n"""

    with open(secrets_file, "w") as f:
        f.write(secrets_content)
    print(f"Created Streamlit secrets file at {secrets_file} with BigQuery project and dataset.")

    return


def create_dbt_profiles_yml_file(args: argparse.Namespace) -> None:
    profiles_dir = ROOT / "dbt"
    profiles_file = profiles_dir / "profiles_container.yml"
    if profiles_file.exists():
        print(f"Found existing {profiles_file}. Leaving it unchanged.")
        return

    profile_config = {
        "uk_flight_punctuality": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "bigquery",
                    "method": "oauth",
                    "project": args.project,
                    "dataset": "flight_data",
                    "threads": 1,
                    "timeout_seconds": 300,
                    "location": args.region,
                    "priority": "interactive",
                    "retries": 1
                }
            }
        }
    }

    with open(profiles_file, "w") as f:
        yaml.dump(profile_config, f, default_flow_style=False, sort_keys=False)
    print(f"Created dbt profiles.yml file at {profiles_file} with BigQuery project and dataset.")


def create_dbt_sources_file(args: argparse.Namespace) -> None:
    profiles_dir = ROOT / "dbt"    
    # path for dbt model sources: <repo>/dbt/models/staging/sources.yml
    sources_file = profiles_dir / "models" / "staging" / "sources.yml"
    
    # Prepare sources config and ensure the sources file exists (create if missing)
    sources_config = {
        "version": 2,
        "sources": [
            {
                "name": "source_data",
                "database": args.project,
                "schema": "flight_data",
                "description": "BigQuery dataset containing raw punctuality tables.",
                "tables": [
                    {
                        "name": "punctuality_data_all_years",
                        "description": "Raw BigQuery table imported from punctuality CSV data for all years."
                    }
                ]
            }
        ]
    }

    
    with open(sources_file, "w") as sf:
            yaml.dump(sources_config, sf, default_flow_style=False, sort_keys=False)
    print(f"Created dbt sources file at {sources_file} with table punctuality_data_all_years.")

    return


def main() -> None:
    
    write_env_file()
    print("Environment file ensured.")
    time.sleep(3)
    
    args = parse_args()

    # If project or region not provided via CLI, try reading them from .env
    if not args.project or not args.region:
        load_env_file()

    project = args.project or os.getenv("GCP_PROJECT")
    if not project:
        print("Error: GCP project ID is required. Provide --project or set GCP_PROJECT.")
        sys.exit(1)

    # Determine region: CLI arg > .env / env var > fallback default
    region = args.region or os.getenv("GCP_REGION") or "europe-west2"
    if not region:
        print("Error: GCP region is required. Provide --region or set GCP_REGION.")
        sys.exit(1)

    # Keep args in sync for downstream functions that expect them
    args.project = project
    args.region = region

    ingest_image_name = args.image_name or "uk-flight-ingest"
    ingest_image = f"{region}-docker.pkg.dev/{project}/{args.repo}/{ingest_image_name}:{args.tag}"

    dbt_image_name = args.dbt_image_name or "uk-flight-dbt"
    dbt_image = f"{region}-docker.pkg.dev/{project}/{args.repo}/{dbt_image_name}:{args.tag}"

    dashboard_image_name = args.dashboard_image_name or "uk-flight-dashboard"
    dashboard_image = f"{region}-docker.pkg.dev/{project}/{args.repo}/{dashboard_image_name}:{args.tag}"

    check_program("docker")
    print("Verified required programs are available.")
    time.sleep(2)

    create_streamlit_secrets_file(args)
    print("Streamlit secrets file ensured.")
    time.sleep(3)
    create_dbt_profiles_yml_file(args)
    print("dbt profiles file ensured.")
    create_dbt_sources_file(args)
    print("dbt sources file ensured.")
    time.sleep(3)
    ensure_artifact_repository(args.project, args.region, args.repo)
    print("Artifact Registry repository check complete.")
    time.sleep(3)
    gcloud_auth_docker(region)
    print("Authenticated Docker to Artifact Registry.")
    time.sleep(3)

    build_and_push(ingest_image, INGEST_DIR)
    print("Ingest image build/push finished.")
    time.sleep(3)
    build_and_push(dbt_image, DBT_DIR)
    print("DBT image build/push finished.")
    time.sleep(3)
    build_and_push(dashboard_image, DASHBOARD_DIR)
    print("Dashboard image build/push finished.")
    time.sleep(3)

    print("\nSuccess")
    print("Docker images pushed to:")
    print(f"  ingest: {ingest_image}")
    print(f"  dbt:    {dbt_image}")
    print(f"  dashboard:    {dashboard_image}")


if __name__ == "__main__":
    main()
