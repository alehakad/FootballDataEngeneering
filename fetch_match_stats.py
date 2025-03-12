import datetime
from io import StringIO

import boto3
import pandas as pd
import soccerdata as sd

s3 = boto3.client('s3')
bucket_name = 'football-raw-data'


def read_game_schedule_from_s3():
    """
    Read the game schedule from S3 and return a pandas DataFrame.
    """
    schedule_key = 'game_schedule/season=2024-25/league=ENG-Premier League/game_schedule_2024-25_ENG-Premier League.csv'

    try:
        schedule_obj = s3.get_object(Bucket=bucket_name, Key=schedule_key)
        schedule_data = schedule_obj['Body'].read().decode('utf-8')
        schedule_df = pd.read_csv(StringIO(schedule_data))
        return schedule_df
    except Exception as e:
        print(f"Error reading game schedule from S3: {e}")
        return pd.DataFrame()  # Return empty DataFrame to prevent errors


def filter_played_matches(schedule_df):
    """
    Filter the matches that have already been played (based on current date and time).
    """
    if 'date' not in schedule_df.columns or 'time' not in schedule_df.columns:
        print("Error: Schedule DataFrame is missing 'date' or 'time' column.")
        return pd.DataFrame()

    schedule_df['date_day'] = pd.to_datetime(schedule_df['date'], errors='coerce').dt.date

    if schedule_df['date_day'].isnull().all():
        print("Error: Could not parse date and time. Check the format in the schedule file.")
        return pd.DataFrame()

    today_date = datetime.datetime.today().date()

    played_matches = schedule_df[schedule_df['date_day'] < today_date]

    if played_matches.empty:
        print("No played matches found.")

    return played_matches


def save_match_stats_to_s3(fbref, match_id, stat_type, season, league):
    """
    Fetch and save player match stats to an S3 bucket.
    """

    try:
        player_match_stats = fbref.read_player_match_stats(stat_type=stat_type, match_id=match_id)
    except Exception as e:
        print(f"Error fetching match stats for match_id {match_id}, stat_type {stat_type}: {e}")
        return

    if player_match_stats.empty:
        print(f"No data found for match_id {match_id} and stat_type {stat_type}")
        return

    s3_path = f"match_stats/season={season}/league={league}/match_id={match_id}/{stat_type}.csv"

    player_match_stats.reset_index(inplace=True)

    csv_buffer = StringIO()
    player_match_stats.to_csv(csv_buffer, index=False)

    try:
        s3.put_object(Bucket=bucket_name, Key=s3_path, Body=csv_buffer.getvalue())
        print(f"File uploaded successfully to {s3_path}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")


def process_and_save_stats(league, season, stats_names_list):
    """
    Main function to process the schedule and save stats for each played match.
    """
    fbref = sd.FBref(leagues=league, seasons=season)
    schedule_df = read_game_schedule_from_s3()

    if schedule_df.empty:
        print("No schedule data found. Exiting...")
        return

    played_matches = filter_played_matches(schedule_df)

    if played_matches.empty:
        print("No matches played yet. Exiting...")
        return

    for _, match in played_matches.iterrows():
        match_id = match['game_id']
        print(f"Fetching stats for match {match_id}...")

        for stat_name in stats_names_list:
            save_match_stats_to_s3(fbref, match_id, stat_name, season, league)


if __name__ == "__main__":
    league = "ENG-Premier League"
    season = '2024-25'
    stats_names_list = ['summary', 'keepers', 'passing', 'passing_types', 'defense', 'possession', 'misc']

    process_and_save_stats(league, season, stats_names_list)
