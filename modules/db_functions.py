# db_functions.py

import psycopg2
from psycopg2 import sql
import mysql.connector

# Sensitive information in external file to avoid Git tracking
import conf

def mysql_connect_db():
    print("Connecting to the MySQL database...")
    conn = mysql.connector.connect(
        host=conf.MYSQL_HOST_URL,
        port=conf.MYSQL_PORT,
        user=conf.MYSQL_USERNAME,
        password=conf.MYSQL_PASSWORD
    )
    cur = conn.cursor()
    print(f"Creating the {conf.MYSQL_DB_NAME} database if not exists...")
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {conf.MYSQL_DB_NAME}")
    cur.fetchall()
    cur.close()
    conn.database = conf.MYSQL_DB_NAME
    print(f"Connected to the MySQL database. id: {conn.connection_id}")
    return conn

def mysql_table_create(conn, table_name):
    try:
        print(f"Creating the {table_name} table if not exists...")
        cur = conn.cursor()

        # Create Users table if not exists
        # Note that this schema matches the Tinybird handling for this table, so changing the schema without updating the Tinybird files will break the overall pipeline.
        cur.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            address VARCHAR(100),
            phone_number VARCHAR(50),
            email_verified BOOLEAN DEFAULT FALSE,
            onboarded BOOLEAN DEFAULT FALSE,
            deleted BOOLEAN DEFAULT FALSE,
            lang CHAR(2),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        ''')
        cur.fetchall()  # Ensure all results are read
        conn.commit()

        cur.close()
        print("Users table created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()

def pg_connect_db():
    print("Connecting to the Postgres database...")
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=conf.PG_HOST_URL,
        user=conf.PG_USERNAME,
        password=conf.PG_PASSWORD,
        dbname=conf.PG_DATABASE
    )
    print("Connected to the Postgres database.")
    return conn

def pg_table_create(conn, table_name):
    try:
        print(f"Creating the {table_name} table if not exists...")
        cur = conn.cursor()

        # Create Users table if not exists
        # Note that this schema matches the Tinybird handling for this table, so changing the schema without updating the Tinybird files will break the overall pipeline.
        create_table_query = sql.SQL('''
        CREATE TABLE IF NOT EXISTS {} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            address VARCHAR(100),
            phone_number VARCHAR(50),
            email_verified BOOLEAN DEFAULT FALSE,
            onboarded BOOLEAN DEFAULT FALSE,
            deleted BOOLEAN DEFAULT FALSE,
            lang CHAR(2),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        ''').format(sql.Identifier(table_name))
        cur.execute(create_table_query)
        conn.commit()

        cur.close()
        print("Users table created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()

def table_fetch(conn, table_name):
    print("Fetching the current list of users...")
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table_name}")
        users = cur.fetchall() or []
    print(f"Fetched {len(users)} users.")
    return users

def table_print(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name} ORDER BY id ASC")
    users = cur.fetchall()
    print(f"{table_name}:")
    for user in users:
        print(user)
    cur.close()

def table_drop(conn, table_name):
    print(f"Dropping the {table_name} table if exists...")
    cur = conn.cursor()
    cur.execute(f'DROP TABLE IF EXISTS {table_name}')
    conn.commit()
    cur.close()
    print(f"{table_name} table dropped if it existed.")

def get_column_names(conn, table_name):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table_name} LIMIT 0')
    column_names = [desc[0] for desc in cur.description]
    cur.close()
    return column_names

