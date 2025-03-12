from io import StringIO

import boto3
import pandas as pd
import soccerdata as sd

s3 = boto3.client('s3')
bucket_name = 'football-raw-data'


def fetch_season_timetable(season, league):
    """
    Fetches season game schedule
    """
    fbref = sd.FBref(leagues=league, seasons=season)
    games_schedule = fbref.read_schedule()

    if games_schedule.empty:
        print(f"No schedule data found for {league} and season {season}")
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
        print(f"File uploaded successfully to {s3_path}")
    except Exception as e:
        print(f"Error uploading file: {e}")


if __name__ == "__main__":
    league = "ENG-Premier League"
    season = '2024-25'

    games_schedule = fetch_season_timetable(season, league)

    s3_path = f"game_schedule/season={season}/league={league}/game_schedule_{season}_{league}.csv"

    save_to_s3(games_schedule, s3_path)
