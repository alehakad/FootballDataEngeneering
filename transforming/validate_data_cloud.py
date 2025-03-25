import logging
from urllib.parse import unquote

import functions_framework
import yaml
from cloudevents.pydantic import CloudEvent
from google.cloud import storage

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

logging.getLogger().setLevel(logging.INFO)

storage_client = storage.Client()


def read_yaml_from_gcs(bucket_name, gcs_key):
    """Read YAML configuration file from Google Cloud Storage."""
    logger.info(f"Reading YAML configuration from gs://{bucket_name}/{gcs_key}")
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_key)
        yaml_content = blob.download_as_text()
        return yaml.safe_load(yaml_content)
    except Exception as e:
        logger.error(f"Error reading YAML file from gs://{bucket_name}/{gcs_key}: {e}")
        raise


def read_dataset_from_gcs(bucket_name, gcs_key):
    """Read dataset from Google Cloud Storage into a Pandas DataFrame."""
    logger.info(f"Reading dataset from gs://{bucket_name}/{gcs_key}")
    try:
        # Decode the URL-encoded path
        gcs_key_decoded = unquote(gcs_key)

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


def clean_dataset(df, dataset_name, config):
    """Clean dataset based on YAML rules."""
    logger.info(f"Cleaning dataset: {dataset_name}")
    dataset_config = config["datasets"].get(dataset_name, {})

    # Get columns to keep and critical columns
    columns_to_keep = dataset_config.get("columns_to_keep", [])
    critical_columns = dataset_config.get("critical_columns", [])

    logger.info(f"Columns to keep: {columns_to_keep}")
    logger.info(f"Critical columns: {critical_columns}")

    # Drop duplicates, keeping first
    df = df.drop_duplicates()
    logger.info("Duplicates dropped.")

    # Convert column names
    df.columns = ["_".join(col.replace("'", "").strip("()").split(", ")) for col in df.columns]
    logger.info("Column names cleaned.")

    # Check for missing values in critical columns
    if critical_columns and df[critical_columns].isnull().any().any():
        missing_cols = df[critical_columns].isnull().sum()
        logger.error(f"Critical columns contain missing values:\n{missing_cols}")
        raise ValueError(f"Critical columns contain missing values:\n{missing_cols}")

    # Keep only specified columns
    df = df[columns_to_keep]
    logger.info(f"Kept only specified columns: {columns_to_keep}")

    # Fill nulls in kept columns with 0
    df = df.fillna(0)
    logger.info("Filled missing values with 0.")

    return df


def save_dataset_to_gcs(df, bucket_name, gcs_key):
    """Save cleaned dataset to Google Cloud Storage."""
    try:
        # Get the bucket and create a new blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_key)

        # Save to a temporary local file first
        temp_local_file = '/tmp/cleaned_dataset.parquet'
        df.to_parquet(temp_local_file, index=False, engine='pyarrow')

        # Upload the local file to GCS
        blob.upload_from_filename(temp_local_file, content_type='application/parquet')

        logger.info(f"File uploaded successfully to gs://{bucket_name}/{gcs_key}")
    except Exception as e:
        logger.error(f"Error saving dataset to GCS: {e}")
        raise


@functions_framework.cloud_event
def process_new_file(cloud_event: CloudEvent):
    """
       Cloud Function triggered by a new file in Cloud Storage.

       Args:
           cloud_event (google.events.cloud.storage.v1.StorageObjectData):
           Cloud Storage event payload
       """
    try:
        data = cloud_event.data
        bucket_name = data["bucket"]
        gcs_key = data["name"]
        logger.info(f"Successfully processed file: {gcs_key}")

        if not gcs_key.endswith('.parquet'):
            logger.info(f"Received nob parquet file")
            return

        # Read config yaml file
        config_bucket = "football-raw-data"
        config_key = "configs/validate_config.yml"
        config = read_yaml_from_gcs(config_bucket, config_key)

        # Extract dataset name from the file key
        dataset_name = gcs_key.split("/")[-1].split(".")[0]

        # Read dataset from GCS
        df = read_dataset_from_gcs(bucket_name, gcs_key)

        # Clean the dataset
        df_cleaned = clean_dataset(df, dataset_name, config)

        # Save cleaned dataset to Silver bucket
        silver_bucket = "football-cleaned-data"
        save_dataset_to_gcs(df_cleaned, silver_bucket, gcs_key)

        logger.info(f"Successfully processed file: {gcs_key}")

    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise
