import psycopg2
import os
import json
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

DB_PARAMS = {
    "dbname": os.getenv("PG_DATABASE"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
}

def connect_db():
    """Establish connection to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def insert_game(game_id, date, home_team, away_team, venue, status):
    """Insert game details into the database."""
    conn = connect_db()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO games (game_id, date, home_team, away_team, venue, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO NOTHING;
            """
            cursor.execute(query, (game_id, date, home_team, away_team, venue, status))
            conn.commit()
            logger.info(f"Inserted game: {game_id} | {home_team} vs {away_team}")
    except Exception as e:
        logger.error(f"Error inserting game {game_id}: {e}")
    finally:
        conn.close()

def insert_odds(game_id, sportsbook, moneyline_home, moneyline_away, spread, spread_home_odds,
                spread_away_odds, total, over_odds, under_odds, prop_bet, parlay, timestamp):
    """Insert betting odds into the database with expanded columns."""
    conn = connect_db()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO odds (
                    game_id, sportsbook, moneyline_home, moneyline_away, spread, spread_home_odds,
                    spread_away_odds, total, over_odds, under_odds, prop_bet, parlay, timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id, sportsbook) DO UPDATE SET 
                    moneyline_home = EXCLUDED.moneyline_home,
                    moneyline_away = EXCLUDED.moneyline_away,
                    spread = EXCLUDED.spread,
                    spread_home_odds = EXCLUDED.spread_home_odds,
                    spread_away_odds = EXCLUDED.spread_away_odds,
                    total = EXCLUDED.total,
                    over_odds = EXCLUDED.over_odds,
                    under_odds = EXCLUDED.under_odds,
                    prop_bet = EXCLUDED.prop_bet,
                    parlay = EXCLUDED.parlay,
                    timestamp = EXCLUDED.timestamp;
            """
            cursor.execute(query, (
                game_id, sportsbook, moneyline_home, moneyline_away, spread, spread_home_odds,
                spread_away_odds, total, over_odds, under_odds, json.dumps(prop_bet), json.dumps(parlay),
                timestamp
            ))
            conn.commit()
    except Exception as e:
        logger.error(f"Error inserting odds: {e}")
    finally:
        conn.close()

def insert_roster(player_id, team_id, name, position, status):
    """Insert player roster data into the database, skipping records with missing names."""
    if not name:
        logger.warning(f"Skipping player ID {player_id} due to missing name.")
        return

    conn = connect_db()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO players (player_id, team_id, name, position, status)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO NOTHING;
            """
            cursor.execute(query, (player_id, team_id, name, position, status))
            conn.commit()
    except Exception as e:
        logger.error(f"Error inserting roster for player {player_id}: {e}")
    finally:
        conn.close()

def insert_injury(player_id, team_id, name, position, status):
    """Insert injury data into the database; update player's status if they already exist."""
    conn = connect_db()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO players (player_id, team_id, name, position, status)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO UPDATE SET status = EXCLUDED.status;
            """
            cursor.execute(query, (player_id, team_id, name, position, status))
            conn.commit()
    except Exception as e:
        logger.error(f"Error inserting injury data for player {player_id}: {e}")
    finally:
        conn.close()

def update_game_status(game_id, new_status):
    """Update the status of a game in the 'games' table."""
    conn = connect_db()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            query = """
                UPDATE games 
                SET status = %s, last_updated = CURRENT_TIMESTAMP 
                WHERE game_id = %s;
            """
            cursor.execute(query, (new_status, game_id))
            conn.commit()
            logger.info(f"Updated game {game_id} status to {new_status}")
    except Exception as e:
        logger.error(f"Error updating game status for game {game_id}: {e}")
    finally:
        conn.close()

def update_lineup(game_id, starting_pitchers):
    """Update starting lineup info for a game.
       Adjust the SQL based on your schema; this example assumes a 'starting_pitchers' column exists.
    """
    conn = connect_db()
    if not conn:
        return

    try:
        with conn.cursor() as cursor:
            query = """
                UPDATE games 
                SET starting_pitchers = %s, last_updated = CURRENT_TIMESTAMP 
                WHERE game_id = %s;
            """
            cursor.execute(query, (str(starting_pitchers), game_id))
            conn.commit()
            logger.info(f"Updated lineup for game {game_id}")
    except Exception as e:
        logger.error(f"Error updating lineup for game {game_id}: {e}")
    finally:
        conn.close()
