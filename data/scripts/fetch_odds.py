#!/usr/bin/env python3
"""
fetch_odds.py

Fetches MLB betting odds including Moneyline, Spreads, and Totals,
and inserts them into PostgreSQL.
"""

import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger
from data.database.database import insert_odds

# Load environment variables
load_dotenv()
ODDS_API_KEY = os.getenv("ODDS_API_KEY")

# API endpoint configuration
ODDS_API_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"

def fetch_odds():
    """
    Fetch MLB betting odds and insert them into PostgreSQL.
    """
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us",
        "markets": "h2h,spreads,totals",  # Ensure only these valid markets are included
        "oddsFormat": "decimal"
    }
    
    # Debug: print out the parameters to verify
    logger.info(f"Request parameters: {params}")

    logger.info("Fetching MLB betting odds...")
    response = requests.get(ODDS_API_URL, params=params)

    if response.status_code == 200:
        odds_data = response.json()

        if not odds_data:
            logger.warning("⚠ No odds data available at this time.")
            return None

        logger.info(f"✅ Fetched odds for {len(odds_data)} games.")

        for game in odds_data:
            game_id = game.get("id")
            for bookmaker in game.get("bookmakers", []):
                sportsbook = bookmaker.get("title")

                # Initialize odds fields
                moneyline_home = None
                moneyline_away = None
                spread = None
                spread_home_odds = None
                spread_away_odds = None
                total = None
                over_odds = None
                under_odds = None
                parlay = {}

                for market in bookmaker.get("markets", []):
                    key = market.get("key")
                    outcomes = market.get("outcomes", [])
                    if key == "h2h":  # Moneyline odds
                        if len(outcomes) > 0:
                            moneyline_home = outcomes[0].get("price")
                        if len(outcomes) > 1:
                            moneyline_away = outcomes[1].get("price")
                    elif key == "spreads":  # Spread betting
                        if len(outcomes) > 0:
                            spread = outcomes[0].get("point")
                            spread_home_odds = outcomes[0].get("price")
                        if len(outcomes) > 1:
                            spread_away_odds = outcomes[1].get("price")
                    elif key == "totals":  # Over/Under
                        if len(outcomes) > 0:
                            total = outcomes[0].get("point")
                            over_odds = outcomes[0].get("price")
                        if len(outcomes) > 1:
                            under_odds = outcomes[1].get("price")
                    # No processing for "player_props" as it is not valid for MLB

                insert_odds(
                    game_id=game_id,
                    sportsbook=sportsbook,
                    moneyline_home=moneyline_home,
                    moneyline_away=moneyline_away,
                    spread=spread,
                    spread_home_odds=spread_home_odds,
                    spread_away_odds=spread_away_odds,
                    total=total,
                    over_odds=over_odds,
                    under_odds=under_odds,
                    prop_bet={},  # Empty, since "player_props" market is removed
                    parlay=parlay,
                    timestamp=datetime.now()
                )

        logger.info("Odds data successfully saved to PostgreSQL.")
        return odds_data

    else:
        logger.error(f"Failed to fetch odds data. Status code: {response.status_code} Response: {response.text}")
        return None

if __name__ == "__main__":
    fetch_odds()
