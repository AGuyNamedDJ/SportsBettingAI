import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL connection parameters
DB_PARAMS = {
    "dbname": os.getenv("PG_DATABASE"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
}

# SQL script to create tables
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS games (
    id SERIAL PRIMARY KEY,
    game_id TEXT UNIQUE NOT NULL,
    date DATE NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    venue TEXT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS teams (
    id SERIAL PRIMARY KEY,
    team_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    abbreviation TEXT NOT NULL,
    city TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS players (
    id SERIAL PRIMARY KEY,
    player_id TEXT UNIQUE NOT NULL,
    team_id TEXT,
    name TEXT NOT NULL,
    position TEXT NOT NULL,
    status TEXT
);

CREATE TABLE IF NOT EXISTS odds (
    id SERIAL PRIMARY KEY,
    game_id TEXT NOT NULL,
    sportsbook TEXT NOT NULL,
    moneyline_home REAL,
    moneyline_away REAL,
    spread REAL,
    total REAL,
    timestamp TIMESTAMPTZ DEFAULT now()
);
"""

def create_tables():
    """Create PostgreSQL tables from SQL script."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        cursor.execute(CREATE_TABLES_SQL)
        conn.commit()
        cursor.close()
        conn.close()
        print("PostgreSQL database tables created successfully!")
    except Exception as e:
        print(f" Error creating tables: {e}")

if __name__ == "__main__":
    create_tables()
