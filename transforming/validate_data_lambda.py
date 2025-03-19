import logging
from urllib.parse import unquote, quote

import awswrangler as wr
import boto3
import yaml

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

logging.getLogger().setLevel(logging.INFO)


s3_client = boto3.client('s3')


def read_yaml_from_s3(s3_bucket, s3_key):
    """Read YAML configuration file from S3 using awswrangler."""
    logger.info(f"Reading YAML configuration from s3://{s3_bucket}/{s3_key}")
    try:
        # Using boto3 to get the object from S3
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)

        # Read the body of the response and decode it as a string
        config_data = response["Body"].read().decode("utf-8")

        # Load the YAML data into a Python object
        config = yaml.safe_load(config_data)
        return config
    except Exception as e:
        logger.error(f"Error reading YAML file s3://{s3_bucket}/{s3_key}: {e}")
        raise


def read_dataset_from_s3(bucket_name, s3_key):
    """Read dataset from S3 into a Pandas DataFrame using awswrangler."""
    logger.info(f"Reading dataset from s3://{bucket_name}/{s3_key}")
    try:
        # Decode the URL-encoded path
        s3_key_decoded = unquote(s3_key)

        # Using awswrangler to read the Parquet file directly from S3 into a DataFrame
        df = wr.s3.read_parquet(path=f"s3://{bucket_name}/{s3_key_decoded}")
        logger.info(f"Dataset loaded successfully from {s3_key_decoded}")
        return df
    except Exception as e:
        logger.error(f"Error reading dataset from s3://{bucket_name}/{s3_key_decoded}: {e}")
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


def save_dataset(df, s3_bucket, s3_key):
    """Save cleaned dataset to S3 using awswrangler."""
    try:
        # URL encode the S3 path before saving
        s3_key_encoded = quote(s3_key)

        # Using awswrangler to write the cleaned DataFrame back to S3 as Parquet
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{s3_bucket}/{s3_key_encoded}",
            dataset=True,  # To allow appending to partitions
            mode="overwrite",  # Overwrite if file exists
        )
        logger.info(f"File uploaded successfully to s3://{s3_bucket}/{s3_key_encoded}")
    except Exception as e:
        logger.error(f"Error saving dataset to S3: {e}")
        raise


def lambda_handler(event, context):
    """Lambda handler triggered by S3 events."""
    try:
        # Event details: trigger details about the file uploaded to S3
        s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
        s3_key = event["Records"][0]["s3"]["object"]["key"]
        logger.info(f"Processing file: {s3_key} from bucket: {s3_bucket}")

        # Fetch the YAML config from S3
        config_bucket = "football-raw-data"
        config_key = "configs/validate_config.yml"
        config = read_yaml_from_s3(config_bucket, config_key)

        # Extract dataset name from the file key (assuming dataset name is the filename)
        dataset_name = s3_key.split("/")[-1].split(".")[0]

        # Read dataset from S3
        df = read_dataset_from_s3(s3_bucket, s3_key)

        # Clean the dataset
        df_cleaned = clean_dataset(df, dataset_name, config)

        # Generate cleaned file key
        silver_file_key = s3_key

        # Save cleaned dataset to Silver bucket
        silver_bucket = "football-cleaned-data"
        save_dataset(df_cleaned, silver_bucket, silver_file_key)

        logger.info(f"Successfully processed file: {s3_key}")

    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise
