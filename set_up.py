#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parent
INGEST_DIR = ROOT / "ingest"
ENV_FILE = INGEST_DIR / ".env"

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
    ENV_FILE.write_text("\n".join(f"{key}={value}" for key, value in DEFAULT_ENV.items()) + "\n")
    print(f"Created {ENV_FILE} with default values.")
    print("Edit it before running the container if you want different bucket, year range, or BigQuery settings.")


def run(cmd: list[str], cwd: Path | None = None, env: Dict[str, str] | None = None) -> None:
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, check=True, env=env or os.environ.copy())


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
    subprocess.run(
        ["docker", "login", "-u", "_json_key", "--password-stdin", f"https://{host}"],
        cwd=ROOT,
        check=True,
        input=credentials_path.read_bytes(),
    )


def build_and_push(image: str) -> None:
    run(["docker", "build", "-t", image, "."], cwd=INGEST_DIR)
    run(["docker", "push", image], cwd=INGEST_DIR)


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
    parser = argparse.ArgumentParser(description="Build and push the ingest Docker image to GCP")
    parser.add_argument("--project", help="GCP project ID")
    parser.add_argument("--region", default="europe-west2", help="GCP region / Artifact Registry location")
    parser.add_argument("--repo", default="uk-flight-punctuality", help="Artifact Registry repository name")
    parser.add_argument("--image-name", default="uk-flight-ingest", help="Docker image name")
    parser.add_argument("--tag", default="latest", help="Docker image tag")
    parser.add_argument(
        "--image",
        help="Full container image URI. If provided, project/region/repo/image-name/tag are ignored.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    project = args.project or os.getenv("GCP_PROJECT")
    if not project:
        print("Error: GCP project ID is required. Provide --project or set GCP_PROJECT.")
        sys.exit(1)

    if args.image:
        image = args.image
    else:
        image = f"{args.region}-docker.pkg.dev/{project}/{args.repo}/{args.image_name}:{args.tag}"

    check_program("docker")

    write_env_file()
    ensure_artifact_repository(project, args.region, args.repo)
    gcloud_auth_docker(args.region)
    build_and_push(image)

    print("\nSuccess")
    print(f"Docker image pushed to: {image}")
    print("Next: update infra/terraform.tfvars container_image to this image and run terraform apply in infra/.")


if __name__ == "__main__":
    main()
