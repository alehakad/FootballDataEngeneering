import json
import logging
from io import StringIO

import pandas as pd
from fuzzywuzzy import process  # pip install python-Levenshtein
from google.cloud import bigquery, storage

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# GCS paths
bucket_name = "football-raw-data"
team_json_path = "teams_json/premier_league_clubs_20250312_155944.json"
schedule_csv_path = 'game_schedule/season=2024-25/league=ENG-Premier%20League/game_schedule_2024-25_ENG-Premier%20League.csv'

# BigQuery table
dataset_id = "helpers"
table_id = "team_name_mapping"


def load_json_from_gcs(bucket_name, file_path):
    """Reads a JSON file from GCS and loads it into a dictionary."""
    logger.info(f"Loading JSON file from GCS: {file_path}")
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        content = blob.download_as_text()
        data = json.loads(content)
        logger.info(f"Successfully loaded {len(data)} records from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Error reading JSON file from GCS ({file_path}): {e}")
        raise


def load_csv_from_gcs(bucket_name, file_path):
    """Reads a CSV file from GCS and loads it into a Pandas DataFrame."""
    logger.info(f"Loading CSV file from GCS: {file_path}")
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        data = blob.download_as_text()
        df = pd.read_csv(StringIO(data))
        logger.info(f"Successfully loaded {df.shape[0]} rows and {df.shape[1]} columns from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV file from GCS ({file_path}): {e}")
        raise


def find_best_match(name, choices):
    """Finds the best matching team name using fuzzy matching."""
    if pd.isna(name) or not isinstance(name, str):
        return None  # Handle missing or invalid values
    match, score = process.extractOne(name, choices)
    logger.debug(f"Matched {name} to {match} with score {score}")
    return match


def upload_to_bigquery(df, dataset_id, table_id):
    """Uploads a DataFrame to a BigQuery table."""
    logger.info(f"Uploading {df.shape[0]} records to BigQuery table: {dataset_id}.{table_id}")
    try:
        table_ref = f"{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"Data successfully uploaded to {table_ref}")
    except Exception as e:
        logger.error(f"Error loading into BigQuery ({table_ref}): {str(e)}")
        raise


if __name__ == "__main__":
    try:
        logger.info("Starting team name matching pipeline...")

        # Read teams JSON
        team_data = load_json_from_gcs(bucket_name, team_json_path)
        team_df = pd.DataFrame(team_data)

        # Read schedule CSV
        timetable_df = load_csv_from_gcs(bucket_name, schedule_csv_path)

        # Ensure necessary columns exist
        if "name" not in team_df.columns or "home_team" not in timetable_df.columns:
            logger.error("Missing required columns in input data. Check JSON and CSV structures.")
            raise ValueError("Missing required columns in input data")

        # Rename columns for clarity
        team_df.rename(columns={"name": "team_name_tf"}, inplace=True)
        # convert id to int
        team_df["id"] = pd.to_numeric(team_df["id"], errors="coerce").astype("Int64")
        # Perform fuzzy matching
        logger.info("Performing fuzzy matching between team names...")
        team_df["team_name_fbref"] = team_df["team_name_tf"].apply(
            lambda x: find_best_match(x, timetable_df["home_team"].tolist())
        )

        # Upload to BigQuery
        upload_to_bigquery(team_df, dataset_id, table_id)

        # TODO: finds all matches except Wolves
        logger.info("✅ Team name mapping pipeline completed successfully!")
    except Exception as e:
        logger.critical(f"Pipeline failed: {str(e)}")
        raise
