# /// script
# dependencies = [
#   "pandas",
#   "pyarrow",
#   "google-cloud-storage",
#   "google-cloud-bigquery",
#   "requests",
# ]
# ///

import datetime
import requests
from google.cloud import storage
from google.cloud import bigquery

client = storage.Client(project="ensai-2026")
bucket = client.bucket("christophe-2026")

start = datetime.datetime(2025, 1, 1)
while start < datetime.datetime(2025, 11, 30):
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{start.strftime('%Y-%m')}.parquet"
    print("Downloading from: ", url)
    response = requests.get(url)
    with open(f"yellow_tripdata_{start.strftime('%Y-%m')}.parquet", "wb") as f:
        f.write(response.content)

    blob = bucket.blob(f"taxi/month={start.strftime('%Y-%m')}/yellow_tripdata.parquet")
    blob.upload_from_filename(f"yellow_tripdata_{start.strftime('%Y-%m')}.parquet")

    start += datetime.timedelta(days=31)

client = bigquery.Client(project="ensai-2026")
sql = """
CREATE OR REPLACE EXTERNAL TABLE christophe.taxi
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://christophe-2026/taxi/month=*/yellow_tripdata.parquet']
);
"""

rows = client.query_and_wait(sql)
