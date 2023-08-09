# db_functions.py

import psycopg2
import mysql.connector
from .utils import Config, setup_logging

# Sensitive information in external file to avoid Git tracking
config = Config()
logger = setup_logging()

def mysql_connect_db():
    logger.info("Connecting to the MySQL database...")
    conn = mysql.connector.connect(
        host=config.MYSQL_HOST_URL,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USERNAME,
        password=config.MYSQL_PASSWORD
    )
    cur = conn.cursor()
    logger.info(f"Creating the {config.MYSQL_DB_NAME} database if not exists...")
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {config.MYSQL_DB_NAME}")
    cur.fetchall()
    cur.execute(f"USE {config.MYSQL_DB_NAME}")
    cur.fetchall()
    cur.close()
    conn.database = config.MYSQL_DB_NAME
    logger.info(f"Connected to the MySQL database. id: {conn.connection_id}")
    return conn

def mysql_database_drop(conn):
    logger.info(f"Dropping the {config.MYSQL_DB_NAME} database if exists...")
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {config.MYSQL_DB_NAME}")
    cur.fetchall()
    cur.close()
    logger.info(f"{config.MYSQL_DB_NAME} database dropped if it existed.")

def pg_connect_db():
    logger.info("Connecting to the Postgres database...")
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=config.PG_HOST_URL,
        user=config.PG_USERNAME,
        password=config.PG_PASSWORD,
        dbname=config.PG_DATABASE
    )
    logger.info("Connected to the Postgres database.")
    return conn

def test_db_connection(db_type):
    try:
        if db_type == 'PG':
            conn = pg_connect_db()
            query = """
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """
        elif db_type == 'MYSQL':
            conn = mysql_connect_db()
            query = """
                SELECT COUNT(*)
                FROM information_schema.tables 
                WHERE table_schema = DATABASE()
            """
        else:
            logger.info(f'Invalid database type: {db_type}')
        cur = conn.cursor()
        cur.execute(query)
        num_tables = cur.fetchone()[0]
        cur.close()
        logger.info(f'Number of tables in {db_type} database: {num_tables}')
    except Exception as e:
        logger.info(f'Error connecting to {db_type}: {e}')

def table_create(conn, table_name, query):
    try:
        logger.info(f"Creating the {table_name} table if not exists...")
        cur = conn.cursor()
        cur.execute(query)
        
        # Check if the connection is a MySQL connection
        if isinstance(conn, mysql.connector.MySQLConnection):
            cur.fetchall()  # Ensure all results are read for MySQL

        conn.commit()
        cur.close()
        logger.info(f"{table_name} table created successfully.")
    except Exception as e:
        logger.info(f"Error creating table: {e}")
        conn.rollback()


def table_fetch(conn, table_name):
    logger.info("Fetching the current list of users...")
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table_name}")
        users = cur.fetchall() or []
    logger.info(f"Fetched {len(users)} users.")
    return users

def table_print(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name} ORDER BY id ASC")
    users = cur.fetchall()
    logger.info(f"{table_name}:")
    for user in users:
        logger.info(user)
    cur.close()

def table_drop(conn, table_name):
    logger.info(f"Dropping the {table_name} table if exists...")
    cur = conn.cursor()
    cur.execute(f'DROP TABLE IF EXISTS {table_name}')
    conn.commit()
    cur.close()
    logger.info(f"{table_name} table dropped if it existed.")

def table_column_names(conn, table_name):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table_name} LIMIT 0')
    column_names = [desc[0] for desc in cur.description]
    cur.fetchall()
    cur.close()
    return column_names
