import requests
import os
import pandas as pd
import click
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict


BASE_URL = "https://datasets.imdbws.com/"
RAW_DATA_FOLDER = "data"
PARQUET_DATA_FOLDER = "parquet"
PROJECT_ID = "ensai-2026"
BUCKET_NAME = "christophe-2026"


def get_data(url, filename):
    print(f"\tDownloading {filename}...")

    response = requests.get(url)
    with open(filename, "wb") as f:
        f.write(response.content)

    print("\tDone.")


def convert_to_parquet(raw_filename, parquet_filename):
    print(f"\tConverting {raw_filename} to {parquet_filename}...")

    df = pd.read_csv(raw_filename, sep="\t", compression="gzip")
    df.to_parquet(parquet_filename)

    print("\tDone.")


def upload_to_gcs(filename, destination):
    print(f"\tUploading to GCS {filename} in {destination}...")

    credentials = service_account.Credentials.from_service_account_file(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    )
    client = storage.Client(project=PROJECT_ID, credentials=credentials)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination)
    blob.upload_from_filename(filename)

    print("\tDone.")


def ensure_dataset_exists(dataset_id):
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"

    try:
        client.get_dataset(dataset_ref)
    except Conflict:
        print(f"\tDataset {dataset_ref} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"  # or "US" depending on your preference
        client.create_dataset(dataset)
        print(f"\tCreated dataset {dataset_ref}")


def create_bigquery_table_from_gcs(gcs_uri, dataset_id, table_id):
    """
    Create a BigQuery table from a Parquet file in GCS.

    Args:
        gcs_uri: Full GCS path, e.g., "gs://bucket-name/path/to/file.parquet"
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
    """
    print(f"\tCreating BigQuery table {dataset_id}.{table_id} from {gcs_uri}...")

    client = bigquery.Client(project=PROJECT_ID)

    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"

    ensure_dataset_exists(dataset_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite if exists
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config,
    )

    load_job.result()  # Wait for the job to complete

    table = client.get_table(table_ref)
    print(f"\tLoaded {table.num_rows} rows to {table_ref}.")


def ingest(table, force_download=False):
    filename = f"{table}.tsv.gz"
    raw_filename = os.path.join(RAW_DATA_FOLDER, filename)
    parquet_filename = os.path.join(
        PARQUET_DATA_FOLDER, filename.replace(".tsv.gz", ".parquet")
    )
    gcs_file_uri = f"imdb/{table}/{table}.parquet"

    if force_download:
        get_data(os.path.join(BASE_URL, filename), raw_filename)
        convert_to_parquet(raw_filename, parquet_filename)
        upload_to_gcs(parquet_filename, gcs_file_uri)

    create_bigquery_table_from_gcs(
        os.path.join("gs://", BUCKET_NAME, gcs_file_uri),
        "bronze_christophe",
        table.replace(".", "_"),
    )


@click.command()
@click.option(
    "--force-download",
    is_flag=True,
    help="Force the download of the data",
)
def run(force_download):
    if not os.path.exists(RAW_DATA_FOLDER):
        os.mkdir(RAW_DATA_FOLDER)

    if not os.path.exists(PARQUET_DATA_FOLDER):
        os.mkdir(PARQUET_DATA_FOLDER)

    tables = [
        "name.basics",
        "title.akas",
        "title.basics",
        "title.crew",
        "title.episode",
        "title.principals",
        "title.ratings",
    ]

    for table in tables:
        print(f"Starting {table}")
        ingest(table, force_download)
        print("Done.")


if __name__ == "__main__":
    run()
