#!/usr/bin/env python3
"""
fetch_data.py

Script to fetch MLB data (schedule, rosters, injuries, odds) from configured APIs
and save them directly to PostgreSQL.
"""

import os
import requests
from datetime import datetime
from loguru import logger
import argparse
from dotenv import load_dotenv
from data.database.database import insert_game, insert_odds, insert_roster, insert_injury
load_dotenv()

# Fetch API keys from .env
SPORTSDATAIO_API_KEY = os.getenv("SPORTSDATAIO_API_KEY")
ODDS_API_KEY = os.getenv("ODDS_API_KEY")


def fetch_schedule(date_str):
    """
    Fetch the MLB game schedule for a specific date and store in PostgreSQL.
    """
    url = f"https://api.sportsdata.io/v3/mlb/scores/json/GamesByDate/{date_str}"
    headers = {"Ocp-Apim-Subscription-Key": SPORTSDATAIO_API_KEY}
    logger.info(f"Fetching schedule for date: {date_str}")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        schedule_data = response.json()

        # Debug: Print API response
        if not schedule_data:
            logger.warning(f"No games found for date: {date_str}")
        else:
            logger.info(f"Fetched {len(schedule_data)} games.")

        for game in schedule_data:
            logger.info(f"Inserting game: {game}")
            insert_game(
                game_id=game.get("GameID"),
                date=game.get("Day"),
                home_team=game.get("HomeTeam"),
                away_team=game.get("AwayTeam"),
                venue=game.get("Stadium", {}).get("Name", "Unknown"),
                status=game.get("Status"),
            )

        logger.info("Schedule data successfully saved to PostgreSQL.")
        return schedule_data
    else:
        logger.error(f"Failed to fetch schedule data. Status code: {response.status_code} Response: {response.text}")
        return None

def fetch_rosters():
    """
    Fetch MLB player roster data from the roster API and store in PostgreSQL.
    """
    url = "https://api.sportsdata.io/v3/mlb/scores/json/Players"
    headers = {"Ocp-Apim-Subscription-Key": SPORTSDATAIO_API_KEY}
    logger.info("Fetching rosters...")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        roster_data = response.json()
        for player in roster_data:
            first_name = player.get("FirstName") or ""
            last_name = player.get("LastName") or ""
            name = f"{first_name} {last_name}".strip()

            if not name or name == "":
                logger.warning(f"Skipping player ID {player.get('PlayerID')} due to missing name. Full data: {player}")
                continue  # Skip inserting this player

            insert_roster(
                player_id=player.get("PlayerID"),
                team_id=player.get("TeamID"),
                name=name,
                position=player.get("Position"),
                status=player.get("Status") or "Unknown",
            )
        logger.info("Roster data successfully saved to PostgreSQL.")
        return roster_data
    else:
        logger.error(f"Failed to fetch roster data. Status code: {response.status_code}")
        return None


def fetch_injuries():
    """
    Fetch current MLB injury reports and store in PostgreSQL.
    """
    url = "https://api.sportsdata.io/v3/mlb/scores/json/Injuries"
    headers = {"Ocp-Apim-Subscription-Key": SPORTSDATAIO_API_KEY}
    logger.info("Fetching injury reports...")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        injury_data = response.json()

        if not injury_data:
            logger.info("No injuries reported. Everything is good!")
            return None  

        for injury in injury_data:
            insert_injury(
                player_id=injury.get("PlayerID"),
                team_id=injury.get("TeamID"),
                name=f"{injury.get('FirstName', '')} {injury.get('LastName', '')}".strip(),
                position=injury.get("Position"),
                status=injury.get("Injury"),
            )

        logger.info("Injury data successfully saved to PostgreSQL.")
        return injury_data

    elif response.status_code == 404:
        logger.warning("⚠ No injury data available (404 response). This is normal if no players are injured.")
        return None

    else:
        logger.error(f"Failed to fetch injury data. Status code: {response.status_code} Response: {response.text}")
        return None

def fetch_odds():
    """
    Fetch MLB betting odds from the configured odds API and store in PostgreSQL.
    """
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    params = {"apiKey": ODDS_API_KEY, "regions": "us", "markets": "h2h,spreads,totals"}
    logger.info("Fetching betting odds...")
    response = requests.get(url, params=params)

    if response.status_code == 200:
        odds_data = response.json()

        # Debug: Print API response
        if not odds_data:
            logger.warning("No odds data received from API.")
        else:
            logger.info(f"Fetched {len(odds_data)} odds entries.")

        for game in odds_data:
            for bookmaker in game.get("bookmakers", []):
                for market in bookmaker.get("markets", []):
                    if market.get("key") == "h2h":
                        odds = market.get("outcomes", [])
                        moneyline_home = odds[0]["price"] if len(odds) > 0 else None
                        moneyline_away = odds[1]["price"] if len(odds) > 1 else None
                    else:
                        moneyline_home, moneyline_away = None, None

                    insert_odds(
                        game_id=game.get("id"),
                        sportsbook=bookmaker.get("title"),
                        moneyline_home=moneyline_home,
                        moneyline_away=moneyline_away,
                        spread=None,
                        total=None,
                        timestamp=datetime.now(),
                    )

        logger.info("Odds data successfully saved to PostgreSQL.")
        return odds_data
    else:
        logger.error(f"Failed to fetch odds data. Status code: {response.status_code} Response: {response.text}")
        return None
    
def parse_arguments():
    """
    Parse command-line arguments for fetching MLB data.
    """
    parser = argparse.ArgumentParser(description="Fetch MLB data from configured APIs.")
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Date (YYYY-MM-DD) for which to fetch the schedule.",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    date_str = args.date

    # 1. Fetch schedule for the specified date
    fetch_schedule(date_str)

    # 2. Fetch rosters
    fetch_rosters()

    # 3. Fetch injuries
    fetch_injuries()

    # 4. Fetch betting odds
    fetch_odds()


if __name__ == "__main__":
    main()