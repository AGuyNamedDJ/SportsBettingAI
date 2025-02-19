import os
import psycopg2
from data.database.database import connect_db
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_FILE = os.path.join(BASE_DIR, "../../database/schema.sql")

from loguru import logger

def execute_sql_file(sql_file):
    """
    Executes SQL commands from a file.
    """
    conn = connect_db()
    if not conn:
        logger.error("Database connection failed.")
        return

    try:
        with conn.cursor() as cursor:
            with open(sql_file, "r") as file:
                sql_script = file.read()
            cursor.execute(sql_script)
            conn.commit()
            logger.info(f"Successfully executed {sql_file}")
    except Exception as e:
        logger.error(f"Error executing {sql_file}: {e}")
    finally:
        conn.close()

execute_sql_file(SQL_FILE)

if __name__ == "__main__":
    execute_sql_file("database/schema.sql")