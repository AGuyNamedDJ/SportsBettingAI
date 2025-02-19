import psycopg2
import os
from dotenv import load_dotenv

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
        print(f"Database connection error: {e}")
        return None

def insert_game(game_id, date, home_team, away_team, venue, status):
    """Insert game details into the database."""
    conn = connect_db()
    if not conn:
        return

    cursor = conn.cursor()
    query = """
        INSERT INTO games (game_id, date, home_team, away_team, venue, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id) DO NOTHING;
    """

    try:
        cursor.execute(query, (game_id, date, home_team, away_team, venue, status))
        conn.commit()
        print(f"Inserted game: {game_id} | {home_team} vs {away_team}")
    except Exception as e:
        print(f"Error inserting game {game_id}: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_odds(game_id, sportsbook, moneyline_home, moneyline_away, spread, total, timestamp):
    """Insert betting odds into the database."""
    conn = connect_db()
    if not conn:
        return

    cursor = conn.cursor()
    query = """
        INSERT INTO odds (game_id, sportsbook, moneyline_home, moneyline_away, spread, total, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cursor.execute(query, (game_id, sportsbook, moneyline_home, moneyline_away, spread, total, timestamp))
        conn.commit()
    except Exception as e:
        print(f"Error inserting odds: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_roster(player_id, team_id, name, position, status):
    """Insert player roster data into the database, skipping records with missing names."""
    if not name:  # Skip if name is None or empty
        print(f"Skipping player ID {player_id} due to missing name.")
        return

    conn = connect_db()
    if not conn:
        return

    cursor = conn.cursor()
    query = """
        INSERT INTO players (player_id, team_id, name, position, status)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (player_id) DO NOTHING;
    """
    try:
        cursor.execute(query, (player_id, team_id, name, position, status))
        conn.commit()
    except Exception as e:
        print(f"Error inserting roster: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_injury(player_id, team_id, name, position, status):
    """Insert injury data into the database."""
    conn = connect_db()
    if not conn:
        return

    cursor = conn.cursor()
    query = """
        INSERT INTO players (player_id, team_id, name, position, status)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (player_id) DO UPDATE SET status = EXCLUDED.status;
    """
    try:
        cursor.execute(query, (player_id, team_id, name, position, status))
        conn.commit()
    except Exception as e:
        print(f"Error inserting injury data: {e}")
    finally:
        cursor.close()
        conn.close()
