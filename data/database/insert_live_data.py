import psycopg2
import os
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

def insert_live_game(game_id, date, home_team, away_team, home_score, away_score, inning, status):
    """
    Insert or update live game data into the database.
    Assumes a table `live_games` exists with columns:
    game_id, date, home_team, away_team, home_score, away_score, inning, status, last_updated.
    """
    conn = connect_db()
    if not conn:
        return

    cursor = conn.cursor()
    query = """
        INSERT INTO live_games (game_id, date, home_team, away_team, home_score, away_score, inning, status, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (game_id) DO UPDATE SET
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            inning = EXCLUDED.inning,
            status = EXCLUDED.status,
            last_updated = CURRENT_TIMESTAMP;
    """
    try:
        cursor.execute(query, (game_id, date, home_team, away_team, home_score, away_score, inning, status))
        conn.commit()
        logger.info(f"Inserted/Updated live game: {game_id} | {home_team} vs {away_team}")
    except Exception as e:
        logger.error(f"Error inserting live game {game_id}: {e}")
    finally:
        cursor.close()
        conn.close()
