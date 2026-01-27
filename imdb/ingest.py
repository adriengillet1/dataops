import requests
import os
import pandas as pd
import click
from google.cloud import storage

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

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination)
    blob.upload_from_filename(filename)

    print("\tDone.")


def ingest(table, force_download=False):
    filename = f"{table}.tsv.gz"
    raw_filename = os.path.join(RAW_DATA_FOLDER, filename)
    parquet_filename = os.path.join(
        PARQUET_DATA_FOLDER, filename.replace(".tsv.gz", ".parquet")
    )

    if force_download:
        get_data(os.path.join(BASE_URL, filename), raw_filename)

    convert_to_parquet(raw_filename, parquet_filename)
    upload_to_gcs(parquet_filename, f"imdb/{table}/{table}.parquet")


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
