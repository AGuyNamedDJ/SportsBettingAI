#!/usr/bin/env python3
"""
preprocess_data.py

This script reads raw MLB data files from the data folder, cleans and standardizes the data,
and writes the processed output back to the data folder.
"""

import os
import json
from loguru import logger

# Define input and output file paths (all in the data folder)
SCHEDULE_INPUT_FILE = os.path.join("data", "schedule.json")
SCHEDULE_OUTPUT_FILE = os.path.join("data", "schedule_processed.json")
ROSTERS_INPUT_FILE = os.path.join("data", "rosters.json")
ROSTERS_OUTPUT_FILE = os.path.join("data", "rosters_processed.json")

def preprocess_schedule():
    """Preprocess the raw schedule data."""
    try:
        with open(SCHEDULE_INPUT_FILE, "r") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Error reading schedule data from {SCHEDULE_INPUT_FILE}: {e}")
        return

    processed = []
    for game in data:
        # Normalize date format if needed, and extract key fields
        processed_game = {
            "game_id": game.get("GameID"),
            "date": game.get("Day"),
            "home_team": game.get("HomeTeam"),
            "away_team": game.get("AwayTeam"),
            "venue": game.get("Stadium", {}).get("Name", "Unknown"),
            "status": game.get("Status")
        }
        processed.append(processed_game)

    try:
        with open(SCHEDULE_OUTPUT_FILE, "w") as f:
            json.dump(processed, f, indent=2)
        logger.info(f"Processed schedule data saved to {SCHEDULE_OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"Error writing processed schedule data to {SCHEDULE_OUTPUT_FILE}: {e}")

def preprocess_rosters():
    """Preprocess the raw rosters data."""
    try:
        with open(ROSTERS_INPUT_FILE, "r") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Error reading rosters data from {ROSTERS_INPUT_FILE}: {e}")
        return

    processed = []
    for player in data:
        first_name = player.get("FirstName") or ""
        last_name = player.get("LastName") or ""
        name = f"{first_name} {last_name}".strip()
        if not name:
            logger.warning(f"Skipping player ID {player.get('PlayerID')} due to missing name.")
            continue
        processed_player = {
            "player_id": player.get("PlayerID"),
            "team_id": player.get("TeamID"),
            "name": name,
            "position": player.get("Position"),
            "status": player.get("Status") or "Unknown"
        }
        processed.append(processed_player)

    try:
        with open(ROSTERS_OUTPUT_FILE, "w") as f:
            json.dump(processed, f, indent=2)
        logger.info(f"Processed roster data saved to {ROSTERS_OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"Error writing processed roster data to {ROSTERS_OUTPUT_FILE}: {e}")

def main():
    logger.info("Starting data preprocessing...")
    preprocess_schedule()
    preprocess_rosters()
    logger.info("Data preprocessing completed.")

if __name__ == "__main__":
    main()
