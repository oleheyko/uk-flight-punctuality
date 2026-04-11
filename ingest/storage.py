import logging

from google.cloud import storage


def blob_exists(bucket_name: str, blob_name: str, client: storage.Client) -> bool:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.exists()


def upload_blob_if_missing(
    bucket_name: str,
    blob_name: str,
    source_url: str,
    session,
    overwrite: bool,
    timeout: int,
) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if blob.exists() and not overwrite:
        logging.info("Skipping existing blob: %s", blob_name)
        return "skipped"

    response = session.get(source_url, timeout=timeout)
    response.raise_for_status()
    content = response.content

    blob.upload_from_string(content, content_type="text/csv")
    logging.info("Uploaded blob: %s", blob_name)
    return "uploaded"
