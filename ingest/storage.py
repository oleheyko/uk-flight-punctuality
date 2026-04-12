import logging

from google.cloud import storage


def blob_exists(bucket_name: str, blob_name: str, client: storage.Client) -> bool:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.exists()


def list_blob_names(bucket_name: str, prefix: str, client: storage.Client) -> set[str]:
    return {blob.name for blob in client.list_blobs(bucket_name, prefix=prefix)}


def upload_blob_if_missing(
    bucket_name: str,
    blob_name: str,
    source_url: str,
    session,
    overwrite: bool,
    timeout: int,
    client: storage.Client | None = None,
    skip_exists_check: bool = False,
) -> str:
    client = client or storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if not skip_exists_check and blob.exists() and not overwrite:
        logging.info("Skipping existing blob: %s", blob_name)
        return "skipped"

    response = session.get(source_url, timeout=timeout)
    response.raise_for_status()
    content = response.content

    blob.upload_from_string(content, content_type="text/csv")
    logging.info("Uploaded blob: %s", blob_name)
    return "uploaded"
