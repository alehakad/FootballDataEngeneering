import io

import boto3
import pandas as pd

# AWS Config
S3_BUCKET_NAME = "football-clean-data"
S3_FILE_PATH = "/match_stats/season%3D2024-25/league%3DENG-Premier%2520League/match_id%3Dcc5b4244/possession.parquet/"

# DynamoDB Tables
dynamodb = boto3.resource("dynamodb")
teams_table = dynamodb.Table("Teams")
players_table = dynamodb.Table("Players")

# S3 Client
s3 = boto3.client("s3")


def get_team_id(team_name):
    response = teams_table.get_item(Key={"team_name": team_name})
    return response.get("Item", {}).get("team_id")


def process_parquet_from_s3():
    # Read Parquet file from S3
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_FILE_PATH)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    # Expected columns: 'team_name', 'player_name', 'jersey_number'
    for _, row in df.iterrows():
        team_id = get_team_id(row["team_name"])
        game_time = row["game_time"]

        if team_id:
            # Insert into Players table
            players_table.put_item(
                Item={
                    "team_id": team_id,
                    "player_name": row["player_name"],
                    "jersey_number": int(row["jersey_number"]),
                    "game_time": game_time,
                }
            )
            print(f"Inserted {row['player_name']} into Players table.")
        else:
            print(f"⚠️ Warning: Team '{row['team_name']}' not found in Teams table.")


if __name__ == "__main__":
    process_parquet_from_s3()
