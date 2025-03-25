import logging
from io import StringIO
from urllib.parse import quote

import pandas as pd
import soccerdata as sd
from google.cloud import storage

storage_client = storage.Client()
bucket_name = 'football-raw-data'

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_season_timetable(season, league):
    """
    Fetches season game schedule
    """
    fbref = sd.FBref(leagues=league, seasons=season)
    games_schedule = fbref.read_schedule()

    if games_schedule.empty:
        logger.warning(f"No schedule data found for {league} and season {season}")
        return

    return games_schedule


def save_to_gcs(games_schedule: pd.DataFrame, gcs_path: str):
    """
    Saves game schedule to Google Cloud Storage
    """
    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Convert df to csv
    csv_buffer = StringIO()
    games_schedule.to_csv(csv_buffer, index=False)

    # Create a blob (file) in the bucket
    blob = bucket.blob(gcs_path)

    # Upload the file
    try:
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        logger.info(f"File uploaded successfully to {gcs_path}")
    except Exception as e:
        logger.error(f"Error uploading file: {e}")


if __name__ == "__main__":
    league = "ENG-Premier League"
    season = '2024-25'

    games_schedule = fetch_season_timetable(season, league)

    # URL-encode the league and season to ensure valid S3 path
    encoded_season = quote(season)
    encoded_league = quote(league)

    gcs_path = f"game_schedule/season={encoded_season}/league={encoded_league}/game_schedule_{encoded_season}_{encoded_league}.csv"

    save_to_gcs(games_schedule, gcs_path)
