import logging
from io import StringIO
import boto3
import pandas as pd
import soccerdata as sd
from urllib.parse import quote

s3 = boto3.client('s3')
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


def save_to_s3(games_schedule: pd.DataFrame, s3_path: str):
    """
    Saves game schedule to s3
    """
    # convert df to csv
    csv_buffer = StringIO()
    games_schedule.to_csv(csv_buffer, index=False)

    # save the file to S3
    try:
        s3.put_object(Bucket=bucket_name, Key=s3_path, Body=csv_buffer.getvalue())
        logger.info(f"File uploaded successfully to {s3_path}")
    except Exception as e:
        logger.error(f"Error uploading file: {e}")


if __name__ == "__main__":
    league = "ENG-Premier League"
    season = '2024-25'

    games_schedule = fetch_season_timetable(season, league)

    # URL-encode the league and season to ensure valid S3 path
    encoded_season = quote(season)
    encoded_league = quote(league)

    s3_path = f"game_schedule/season={encoded_season}/league={encoded_league}/game_schedule_{encoded_season}_{encoded_league}.csv"

    save_to_s3(games_schedule, s3_path)
