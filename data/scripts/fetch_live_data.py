#!/usr/bin/env python3
"""
fetch_live_data.py

Fetches live MLB game data including scores, player stats, and play-by-play events.
"""

import os
import time
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
from data.database.insert_live_data import insert_live_game

# Load environment variables
load_dotenv()

# API Configuration
SPORTSDATAIO_API_KEY = os.getenv("SPORTSDATAIO_API_KEY")
LIVE_API_URL = "https://api.sportsdata.io/v3/mlb/scores/json/LiveGameStatsByDate"

def fetch_live_games():
    """
    Fetch live MLB games and their current stats.
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    url = f"{LIVE_API_URL}/{date_str}"
    headers = {"Ocp-Apim-Subscription-Key": SPORTSDATAIO_API_KEY}

    logger.info(f"Fetching live game data for {date_str}...")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        live_data = response.json()

        # Debugging: Save a copy locally
        with open("data/live/live_scores.json", "w") as f:
            json.dump(live_data, f, indent=2)

        logger.info(f"✅ Fetched {len(live_data)} live games.")
        return live_data
    else:
        logger.error(f"❌ Failed to fetch live game data. Status: {response.status_code} Response: {response.text}")
        return None

def process_live_data():
    """
    Process live data and insert into the database.
    """
    live_games = fetch_live_games()
    if not live_games:
        logger.warning("⚠ No live games available.")
        return

    for game in live_games:
        insert_live_game(
            game_id=game.get("GameID"),
            date=game.get("Day"),
            home_team=game.get("HomeTeam"),
            away_team=game.get("AwayTeam"),
            home_score=game.get("HomeTeamRuns"),
            away_score=game.get("AwayTeamRuns"),
            inning=game.get("Inning"),
            status=game.get("Status"),
        )

def main():
    while True:
        process_live_data()
        logger.info("Sleeping for 60 seconds before next update...")
        time.sleep(60)  # Adjust polling interval if needed

if __name__ == "__main__":
    main()