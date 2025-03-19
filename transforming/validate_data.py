import logging
from io import BytesIO

import boto3
import pandas as pd
import yaml

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

s3_client = boto3.client("s3")


def read_yaml_from_local(config_path):
    """Read YAML configuration file from local disk."""
    logger.info(f"Reading YAML configuration from {config_path}")
    try:
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error reading YAML file {config_path}: {e}")
        raise


def read_dataset_from_s3(bucket_name, s3_key):
    """Read dataset from S3 into a Pandas DataFrame."""
    logger.info(f"Reading dataset from s3://{bucket_name}/{s3_key}")
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        buffer = BytesIO(obj['Body'].read())
        df = pd.read_parquet(buffer, engine="pyarrow")
        logger.info(f"Dataset loaded successfully from {s3_key}")
        return df
    except Exception as e:
        logger.error(f"Error reading dataset from s3://{bucket_name}/{s3_key}: {e}")
        raise


def clean_dataset(df: pd.DataFrame, dataset_name, config):
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


def save_dataset(df, output_path=None, s3_bucket=None, s3_key=None):
    """Save cleaned dataset either locally or to S3."""
    try:
        if s3_bucket and s3_key:
            # Save to S3 using BytesIO buffer
            buffer = BytesIO()
            df.to_parquet(buffer, index=False, engine="pyarrow")  # Use PyArrow to convert to Parquet
            buffer.seek(0)  # Rewind the buffer before sending to S3

            # Upload to S3
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=buffer.getvalue())
            logger.info(f"File uploaded successfully to s3://{s3_bucket}/{s3_key}")
        elif output_path:
            # Save locally if S3 details are not provided
            df.to_parquet(output_path, index=False)
            logger.info(f"Cleaned dataset saved locally to {output_path}")
        else:
            logger.error("Neither local path nor S3 details provided.")
            raise ValueError("Both local path and S3 path are missing.")
    except Exception as e:
        logger.error(f"Error saving dataset: {e}")
        raise


def main():
    # Configuration paths and S3 details
    CONFIG_PATH = "./validate_config.yml"
    BRONZE_BUCKET = "football-raw-data"
    SILVER_BUCKET = "football-cleaned-data"
    BUCKET_FILE_PATH = "match_stats/season=2024-25/league=ENG-Premier League/match_id=c0e3342a"
    BRONZE_FILE_KEY = f"{BUCKET_FILE_PATH}/summary.parquet"
    SILVER_FILE_KEY = f"{BUCKET_FILE_PATH}/summary.parquet"
    # LOCAL_OUTPUT_PATH = "./summary_cleaned.parquet"  # Path for local saving

    # Load YAML config from local disk
    try:
        config = read_yaml_from_local(CONFIG_PATH)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return

    # Read dataset from S3
    try:
        df = read_dataset_from_s3(BRONZE_BUCKET, BRONZE_FILE_KEY)
    except Exception as e:
        logger.error(f"Failed to load dataset from S3: {e}")
        return

    # Extract dataset name from the file key (assuming dataset name is the filename)
    dataset_name = BRONZE_FILE_KEY.split("/")[-1].split(".")[0]

    # Clean the dataset
    try:
        df_cleaned = clean_dataset(df, dataset_name, config)
    except Exception as e:
        logger.error(f"Error cleaning dataset: {e}")
        return

    # Save cleaned dataset (to S3 or locally)
    try:
        # Save to S3
        save_dataset(df_cleaned, s3_bucket=SILVER_BUCKET, s3_key=SILVER_FILE_KEY)

        # Optionally save to local as well for debugging
        # save_dataset(df_cleaned, output_path=LOCAL_OUTPUT_PATH)

    except Exception as e:
        logger.error(f"Error saving cleaned dataset: {e}")


if __name__ == "__main__":
    main()
