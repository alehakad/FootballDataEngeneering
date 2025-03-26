import logging

import functions_framework
import pandas as pd
from google.cloud import bigquery, storage

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

logging.getLogger().setLevel(logging.INFO)

storage_client = storage.Client()
bigquery_client = bigquery.Client()


def read_dataset_from_gcs(bucket_name, gcs_key):
    """Read dataset from Google Cloud Storage into a Pandas DataFrame."""
    logger.info(f"Reading dataset from gs://{bucket_name}/{gcs_key}")
    try:
        gcs_key_decoded = gcs_key

        # Get the bucket and blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_key_decoded)

        # Download blob to a temporary file
        temp_local_file = '/tmp/temp_dataset.parquet'
        blob.download_to_filename(temp_local_file)

        # Read the parquet file
        df = pd.read_parquet(temp_local_file, engine='pyarrow')
        logger.info(f"Dataset loaded successfully from {gcs_key_decoded}")
        return df
    except Exception as e:
        logger.error(f"Error reading dataset from gs://{bucket_name}/{gcs_key_decoded}: {e}")
        raise


@functions_framework.cloud_event
def load_parquet_to_bigquery(cloud_event):
    """Triggered on new file in silver bucket"""
    try:
        data = cloud_event.data
        bucket_name = data["bucket"]
        gcs_key = data["name"]

        dataset_name = gcs_key.split("/")[-1].split(".")[0]

        logger.info(f"Started processing file: {gcs_key}")

        if not gcs_key.endswith('.parquet'):
            logger.info(f"Received not parquet file")
            return

        # read from GCS
        df = read_dataset_from_gcs(bucket_name, gcs_key)

        df['source_file'] = dataset_name
        df['ingestion_timestamp'] = pd.Timestamp.now()

        # fix column names
        df.columns = [
            col.lower().replace("/", "_").replace(" ", "_") if not col[0].isalpha() else col.lower().replace("/",
                                                                                                             "_").replace(
                " ", "_")
            for col in df.columns
        ]

        dataset_id = 'players_stats'
        table_id = 'staging_stats_table'

        # load into BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
            autodetect=True
        )

        table_ref = f"{dataset_id}.{table_id}"
        # schema will be inferred automatically
        job = bigquery_client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )

        job.result()

        logger.info(f"Successfully loaded {dataset_name} to BigQuery")
    except Exception as e:
        logger.error(f"Error processing {dataset_name}: {str(e)}")
        raise
