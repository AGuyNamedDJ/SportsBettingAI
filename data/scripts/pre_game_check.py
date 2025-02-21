#!/usr/bin/env python3
"""
pre_game_check.py

Pre-Game Validation Script:
- Fetches updated game statuses for games scheduled tomorrow.
- Fetches starting lineup information for tomorrow’s games.
- Updates the database accordingly.
"""

import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from loguru import logger
from data.database.database import update_game_status, update_lineup  # Ensure these exist

# Load environment variables
load_dotenv()

# API Configuration (adjust endpoints as necessary for your provider)
SPORTSDATAIO_API_KEY = os.getenv("SPORTSDATAIO_API_KEY")
# Endpoint for fetching game statuses
GAME_STATUS_URL = "https://api.sportsdata.io/v3/mlb/scores/json/GamesByDate"
# Hypothetical endpoint for starting lineups (adjust if your provider uses a different URL)
STARTING_LINEUPS_URL = "https://api.sportsdata.io/v3/mlb/scores/json/StartingLineupsByDate"


def fetch_game_status(date_str):
    """Fetch updated game statuses for a given date."""
    url = f"{GAME_STATUS_URL}/{date_str}"
    headers = {"Ocp-Apim-Subscription-Key": SPORTSDATAIO_API_KEY}
    logger.info(f"Fetching game statuses for {date_str}...")
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Error fetching game statuses: {response.status_code} Response: {response.text}")
        return None


def fetch_starting_lineups(date_str):
    """Fetch starting lineup information for a given date."""
    url = f"{STARTING_LINEUPS_URL}/{date_str}"
    headers = {"Ocp-Apim-Subscription-Key": SPORTSDATAIO_API_KEY}
    logger.info(f"Fetching starting lineups for {date_str}...")
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Error fetching starting lineups: {response.status_code} Response: {response.text}")
        return None


def pre_game_validation():
    """
    Perform pre-game validation:
      - Update game statuses.
      - Update starting lineups.
    Assumes pre-game data is for tomorrow.
    """
    tomorrow = datetime.now() + timedelta(days=1)
    date_str = tomorrow.strftime("%Y-%m-%d")
    logger.info(f"Starting pre-game validation for {date_str}")

    # Fetch game statuses
    game_status_data = fetch_game_status(date_str)
    if game_status_data:
        for game in game_status_data:
            game_id = game.get("GameID")
            new_status = game.get("Status")
            # Convert game_id to string if needed (depending on your DB column type)
            update_game_status(str(game_id), new_status)
            logger.info(f"Updated game {game_id} status to {new_status}")
    else:
        logger.warning("No game status data available.")

    # Fetch starting lineups
    lineup_data = fetch_starting_lineups(date_str)
    if lineup_data:
        for game in lineup_data:
            game_id = game.get("GameID")
            # Depending on your data structure, extract the starting lineup info (e.g., list of starting pitchers)
            starting_pitchers = game.get("StartingPitchers")
            update_lineup(str(game_id), starting_pitchers)
            logger.info(f"Updated starting lineup for game {game_id}")
    else:
        logger.warning("No starting lineup data available.")


def main():
    logger.info("Pre-game validation process initiated.")
    pre_game_validation()
    logger.info("Pre-game validation completed.")


if __name__ == "__main__":
    main()
