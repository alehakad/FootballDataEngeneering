#!/usr/bin/env python3
"""
Script to collect Premier League players and their market values using the Transfermarkt fly.dev API
"""

import json
import logging
import time
import os
from datetime import datetime
from pathlib import Path
import random
import requests

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('transfermarkt-flydev')

# Configuration
BASE_URL = "https://transfermarkt-api.fly.dev"
LEAGUE_ID = "GB1"  # Premier League
SEASON_ID = "2024"  # Current season
OUTPUT_DIR = "./transfermarkt_data"

# Headers
HEADERS = {
    'accept': 'application/json'
}

def get_premier_league_clubs():
    """
    Get all Premier League clubs for the current season
    
    Returns:
        list: List of club data
    """
    url = f"{BASE_URL}/competitions/{LEAGUE_ID}/clubs"
    params = {"season_id": SEASON_ID}
    
    logger.info(f"Fetching Premier League clubs for season {SEASON_ID}...")
    
    try:
        response = requests.get(url, params=params, headers=HEADERS)
        response.raise_for_status()
        
        data = response.json()
        clubs = data.get("clubs", [])
        
        logger.info(f"Found {len(clubs)} Premier League clubs")
        return clubs
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching clubs: {str(e)}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Status code: {e.response.status_code}")
            logger.error(f"Response: {e.response.text[:200]}")
        return []

def get_club_players(club_id, club_name):
    """
    Get all players for a specific club
    
    Args:
        club_id (str): The club ID
        club_name (str): The club name (for logging)
    
    Returns:
        list: List of player data
    """
    url = f"{BASE_URL}/clubs/{club_id}/players"
    
    logger.info(f"Fetching players for {club_name} (ID: {club_id})...")
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        data = response.json()
        players = data.get("players", [])
        
        # Add the club info to each player record
        for player in players:
            player["club_id"] = club_id
            player["club_name"] = club_name
        
        logger.info(f"Found {len(players)} players for {club_name}")
        return players
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching players for {club_name}: {str(e)}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Status code: {e.response.status_code}")
            logger.error(f"Response: {e.response.text[:200]}")
        return []

def get_player_market_value(player_id, player_name):
    """
    Get market value history for a specific player
    
    Args:
        player_id (str): The player ID
        player_name (str): The player name (for logging)
    
    Returns:
        dict: Market value data
    """
    url = f"{BASE_URL}/players/{player_id}/market_value"
    
    logger.info(f"Fetching market value for {player_name} (ID: {player_id})...")
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Successfully fetched market value data for {player_name}")
        return data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching market value for {player_name}: {str(e)}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Status code: {e.response.status_code}")
            logger.error(f"Response: {e.response.text[:200]}")
        return {}

def save_to_json(data, filename):
    """
    Save data to a JSON file
    
    Args:
        data: The data to save
        filename (str): The filename
    """
    # Create output directory if it doesn't exist
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # Full file path
    file_path = output_dir / filename
    
    # Save the data
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Data saved to {file_path}")

def main():
    """
    Main function to collect all Premier League players with market values
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Get all Premier League clubs
        clubs = get_premier_league_clubs()
        
        if not clubs:
            logger.error("No clubs found. Exiting.")
            return
        
        # Save the clubs data
        save_to_json(clubs, f"premier_league_clubs_{timestamp}.json")
        
        # 2. Get players for each club and their market values
        all_players = []
        failed_market_values = []
        
        for i, club in enumerate(clubs):
            club_id = club.get("id")
            club_name = club.get("name", f"Club #{i+1}")
            
            # Skip if no club ID
            if not club_id:
                logger.warning(f"No ID found for {club_name}. Skipping.")
                continue
            
            # Get players for this club
            players = get_club_players(club_id, club_name)
            
            # Get market values for each player
            for player in players:
                player_id = player.get("id")
                player_name = player.get("name", f"Player {player_id}")
                
                if player_id:
                    # Add a small delay to avoid overwhelming the API
                    time.sleep(random.uniform(0.5, 1.5))
                    
                    # Get market value data
                    market_value_data = get_player_market_value(player_id, player_name)
                    
                    if market_value_data:
                        # Add market value data to the player record
                        player["market_value_data"] = market_value_data
                        
                        # Extract current market value for easier access
                        market_values = market_value_data.get("market_values", [])
                        if market_values:
                            # Latest market value is typically the first one in the list
                            latest_value = market_values[0]
                            player["current_market_value"] = latest_value.get("value")
                            player["current_market_value_currency"] = latest_value.get("currency")
                            player["current_market_value_date"] = latest_value.get("date")
                    else:
                        failed_market_values.append({
                            "player_id": player_id,
                            "player_name": player_name,
                            "club_name": club_name
                        })
            
            # Add players to the master list
            all_players.extend(players)
            
            # Save progress after each club
            save_to_json(all_players, f"premier_league_players_with_market_values_{timestamp}.json")
            
            # Also save the list of failed market value lookups
            if failed_market_values:
                save_to_json(failed_market_values, f"failed_market_value_lookups_{timestamp}.json")
            
            # Add a small delay before the next club
            time.sleep(random.uniform(1.0, 2.0))
        
        # 3. Final save and summary
        player_count = len(all_players)
        failed_count = len(failed_market_values)
        
        logger.info(f"Collection complete. Found {player_count} players across {len(clubs)} Premier League clubs")
        logger.info(f"Successfully retrieved market values for {player_count - failed_count} players")
        logger.info(f"Failed to retrieve market values for {failed_count} players")
        
        print(f"\nCollection complete!")
        print(f"Found {player_count} players across {len(clubs)} Premier League clubs")
        print(f"Successfully retrieved market values for {player_count - failed_count} players")
        print(f"Failed to retrieve market values for {failed_count} players")
        print(f"Data saved to: {OUTPUT_DIR}/premier_league_players_with_market_values_{timestamp}.json")
    
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        print(f"\nAn error occurred: {str(e)}")

if __name__ == "__main__":
    main()