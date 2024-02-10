# -*- coding: utf-8 -*-

# pip install requests google-cloud-storage google-cloud-bigquery

import requests
import os
import google.api_core.exceptions
from google.cloud import storage
from google.cloud import bigquery


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'keys/service-account-key.json'

# Configure these variables according to your GCP setup
BUCKET_NAME = 'bucket_name'
PROJECT_ID = 'project_id'
DATASET_ID = 'green_trip_records'
EXTERNAL_TABLE_ID = 'taxi_data_external'
MATERIALIZED_TABLE_ID = 'taxi_data_materialized'
OPTIMIZED_TABLE_ID = 'taxi_data_optimized'

# URLs of the Parquet files
parquet_urls = [
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet',
    'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet'
]
os.makedirs('tmp', exist_ok=True)

# Function to download and upload files to GCS
def download_and_upload_to_gcs(url, bucket):
    filename = url.split('/')[-1]
    local_path = 'tmp/' + filename
    blob = bucket.blob(filename)

    # Download file
    response = requests.get(url)
    with open(local_path, 'wb') as file:
        file.write(response.content)

    # Upload to GCS
    blob.upload_from_filename(local_path)

# Initialize GCP clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Download and upload each Parquet file
for url in parquet_urls:
    download_and_upload_to_gcs(url, BUCKET_NAME)

# Create a dataset
dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"

try:
    dataset = bigquery_client.create_dataset(dataset, timeout=30)
    print(f"Created dataset {bigquery_client.project}.{dataset.dataset_id}")
except google.api_core.exceptions.Conflict:
    print(f"Dataset {bigquery_client.project}.{dataset.dataset_id} already exists")

# Create an external table in BigQuery
uri = f"gs://{BUCKET_NAME}/*.parquet"
table_id = f"{PROJECT_ID}.{DATASET_ID}.{EXTERNAL_TABLE_ID}"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    autodetect=True,
)

table = bigquery.Table(table_id)
external_config = bigquery.ExternalConfig("PARQUET")
external_config.source_uris = [uri]
table.external_data_configuration = external_config

# Create the table
bigquery_client.create_table(table, exists_ok=True)
print(f"Table {table_id} created.")

# Define the schema of the materialized table

bigquery_schema = [
    bigquery.SchemaField("VendorID", "INTEGER"),
    bigquery.SchemaField("lpep_pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("lpep_dropoff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("store_and_fwd_flag", "STRING"),
    bigquery.SchemaField("RatecodeID", "FLOAT"),
    bigquery.SchemaField("PULocationID", "INTEGER"),
    bigquery.SchemaField("DOLocationID", "INTEGER"),
    bigquery.SchemaField("passenger_count", "FLOAT"),
    bigquery.SchemaField("trip_distance", "FLOAT"),
    bigquery.SchemaField("fare_amount", "FLOAT"),
    bigquery.SchemaField("extra", "FLOAT"),
    bigquery.SchemaField("mta_tax", "FLOAT"),
    bigquery.SchemaField("tip_amount", "FLOAT"),
    bigquery.SchemaField("tolls_amount", "FLOAT"),
    bigquery.SchemaField("ehail_fee", "INTEGER"),
    bigquery.SchemaField("improvement_surcharge", "FLOAT"),
    bigquery.SchemaField("total_amount", "FLOAT"),
    bigquery.SchemaField("payment_type", "FLOAT"),
    bigquery.SchemaField("trip_type", "FLOAT"),
    bigquery.SchemaField("congestion_surcharge", "FLOAT")
]

# Create a table in BQ
table_id = f"{PROJECT_ID}.{DATASET_ID}.{MATERIALIZED_TABLE_ID}"
table = bigquery.Table(table_id, schema=bigquery_schema)

try:
    table = bigquery_client.create_table(table)
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
except google.api_core.exceptions.Conflict:
    print(f"Table {table.project}.{table.dataset_id}.{table.table_id} already exists")

# Load data from GCS into the BigQuery table
uri = f"gs://{BUCKET_NAME}/*.parquet"
load_job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=load_job_config)

# Wait for the load job to complete
load_job.result()
print(f"Data loaded into table {table_id}")

# Creating optimized BQ table
table_id = f"{PROJECT_ID}.{DATASET_ID}.{OPTIMIZED_TABLE_ID}"
table = bigquery.Table(table_id, schema=bigquery_schema)
table.time_partitioning = bigquery.TimePartitioning(
    field="lpep_pickup_datetime",  # Partition by this field
)
table.clustering_fields = ["PULocationID"]  # Cluster by this field

# Create the table
bigquery_client.create_table(table)

# Load data from GCS into the BigQuery table
uri = f"gs://{BUCKET_NAME}/*.parquet"
load_job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET)
load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=load_job_config)

# Wait for the load job to complete
load_job.result()
print(f"Data loaded into table {table_id}")
