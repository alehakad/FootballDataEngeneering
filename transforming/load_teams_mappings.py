"""
Loads all teams names with generated ids into DynamoDB table
"""

import hashlib
import io
import logging

import boto3
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

logging.getLogger().setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
teams_table = dynamodb.Table("Teams")

S3_BUCKET = "football-raw-data"
S3_KEY = "game_schedule/season=2024-25/league=ENG-Premier%20League/game_schedule_2024-25_ENG-Premier%20League.csv"


# Load CSV from S3
def load_csv_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(response["Body"].read()))
    return df


# Generate unique team_id
def generate_team_id(team_name):  # Shorten ID
    return int(hashlib.md5(team_name.encode()).hexdigest(), 16) % 10 ** 6


# Insert team into DynamoDB
def insert_team(team_name):
    team_id = generate_team_id(team_name)

    # Check if team already exists
    response = teams_table.get_item(Key={"team_id": team_id})
    if "Item" in response:
        return f"Team {team_name} already exists."

    # Insert new team
    teams_table.put_item(
        Item={
            "team_id": team_id,
            "team_name": team_name,
        }
    )
    return f"Inserted {team_name} with ID {team_id}"


# Lambda Handler
def lambda_handler(event, context):
    df = load_csv_from_s3(S3_BUCKET, S3_KEY)

    for team_name in df["home_team"].unique():
        result = insert_team(team_name)
        logger.info(result)

    return {"statusCode": 200, "body": "Teams processed"}


if __name__ == "__main__":
    lambda_handler(None, None)
