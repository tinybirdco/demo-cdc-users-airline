#! /usr/bin/env python3

from modules import tb_functions
from modules import cc_functions
from modules import db_functions

import os
import random
import glob
import requests
import time
import faker
import click
import json
from datetime import datetime

# Sensitive information in external file to avoid Git tracking
import conf

# TINYBIRD CONSTANTS
TB_BASE_URL = f"https://{conf.TINYBIRD_API_URI}.tinybird.co/v0/"

# Datagen Constants
INSERT_WEIGHT = 30
UPDATE_WEIGHT = 60
DELETE_WEIGHT = 10
ADDRESS_UPDATE_PROBABILITY = 0.1
NUM_EVENTS = 10
LANGUAGES = ['EN', 'ES', 'FR', 'DE', 'IT']

PROPAGATION_TIMEOUT = 15  # seconds
PROPAGATION_SLEEP_INTERVAL = 1  # seconds

# Fake data generator
fake = faker.Faker()

def generate_events(conn, num_events, table_name):
    print("Generating user events...")
    cur = conn.cursor()

    # Get schema information
    cur.execute(f'SELECT * FROM {table_name} LIMIT 0')
    #column_names = db_get_column_names(conn, table_name)
    column_names = cur.column_names
    cur.fetchall()
    deleted_index = column_names.index('deleted')
    email_verified_index = column_names.index('email_verified')
    onboarded_index = column_names.index('onboarded')

    # Generate random user events
    for _ in range(num_events):
        users = db_functions.table_fetch(conn, table_name)
        if len(users) == 0:
            event_type = 'insert'
        else:
            event_type = random.choices(
                ['insert', 'update', 'delete'],
                weights=[INSERT_WEIGHT, UPDATE_WEIGHT, DELETE_WEIGHT],
                k=1
            )[0]
        
        print(f"Picked event type: {event_type}")
        if event_type == 'insert':
            # Insert new user
            insert_query = f'''
                INSERT INTO {table_name} (name, email, address, phone_number, lang, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''
            cur.execute(insert_query, (fake.name(), fake.email(), fake.address(), fake.phone_number(), random.choice(LANGUAGES), datetime.now(), datetime.now()))
            print("New user inserted.")
            
        elif event_type == 'update':
            # Update existing user
            user = random.choice(users)
            user_id = user[0]
            cur.execute(f"SELECT * FROM {table_name} WHERE id = %s", (user_id,))
            user_info = cur.fetchone()
            if user_info[deleted_index]:  # skip if user is deleted
                print("Skipping update for deleted user...")
                continue
            print("Updating an existing user...")
            if random.random() < ADDRESS_UPDATE_PROBABILITY:  # chance to update address or phone number
                update_query = f'''
                    UPDATE {table_name}
                    SET address = %s, phone_number = %s, updated_at = %s
                    WHERE id = %s
                '''
                cur.execute(update_query, (fake.address(), fake.phone_number(), datetime.now(), user_id))
            else:  # progress through onboarding process
                if not user_info[email_verified_index]:  # if not email_verified, verify
                    update_query = f'''
                        UPDATE {table_name}
                        SET email_verified = TRUE, updated_at = %s
                        WHERE id = %s
                    '''
                    cur.execute(update_query, (datetime.now(), user_id,))
                elif not user_info[onboarded_index]:  # if not onboarded, onboard
                    update_query = f'''
                        UPDATE {table_name}
                        SET onboarded = TRUE, updated_at = %s
                        WHERE id = %s
                    '''
                    cur.execute(update_query, (datetime.now(), user_id,))
            print("Existing user updated.")

        elif event_type == 'delete':
            # Mark user as deleted
            print("Deleting a user...")
            user = random.choice(users)
            user_id = user[0]
            cur.execute(f"SELECT * FROM {table_name} WHERE id = %s", (user_id,))
            user_info = cur.fetchone()
            if user_info[deleted_index]:  # skip if user is deleted
                print("Skipping delete for deleted user...")
                continue
            delete_query = f'''
                UPDATE {table_name}
                SET deleted = TRUE, updated_at = %s
                WHERE id = %s
            '''
            cur.execute(delete_query, (datetime.now(), user_id,))
            print("User marked as deleted.")

        # Commit the transaction
        conn.commit()

    cur.close()
    print("User events generated.")

def test_connectivity(db_type):
    # Test PostgreSQL connection
    if db_type == 'PG':
        try:
            pg_conn = db_functions.pg_connect_db()
            print('PostgreSQL connection successful.')
            cur = pg_conn.cursor()

            # Query to count the number of tables in the current database
            query = """
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """
            cur.execute(query)
            num_tables = cur.fetchone()[0]

            cur.close()
            print(f'Number of tables in the database: {num_tables}')
        except Exception as e:
            print(f'Error connecting to PostgreSQL: {e}')
    if db_type == 'MYSQL':
        # Test MySQL connection
        try:
            mysql_conn = db_functions.mysql_connect_db()
            print('MySQL connection successful.')
            cur = mysql_conn.cursor()

            # Query to count the number of tables in the current database
            # This SQL statement works with MySQL
            query = """
                SELECT COUNT(*)
                FROM information_schema.tables 
                WHERE table_schema = DATABASE()
            """
            cur.execute(query)
            num_tables = cur.fetchone()[0]

            cur.close()
            print(f'Number of tables in the MySQL database: {num_tables}')
        except Exception as e:
            print(f'Error connecting to MySQL: {e}')

    # Test Confluent Kafka connection
    try:
        topics = k_topic_list()
        print(f'Available topics: {len(topics)}')
    except Exception as e:
        print(f'Error connecting to Confluent Kafka: {e}')
    # Test Tinybird connection
    try:
        tb_functions.connection_test()
    except Exception as e:
        print(f'Error connecting to Tinybird: {e}')

def bool_to_int(row):
    """Converts boolean values in a row to integers."""
    return {k: int(v) if isinstance(v, bool) else v for k, v in row.items()}

def compare_source_to_dest(source_conn, dest_endpoint):
    dest_rest = tb_functions.endpoint_fetch(dest_endpoint)
    dest_data = dest_rest['data'] or []
    if not dest_data:
        print(f"Destination endpoint {dest_endpoint} is empty.")
        return False

    source_data = db_functions.table_fetch(source_conn, conf.USERS_TABLE_NAME)
    if not source_data:
        print(f"Source table {conf.USERS_TABLE_NAME} is empty.")
        return False
    
    # Convert to dicts for comparison
    column_names = column_names = db_functions.get_column_names(source_conn, conf.USERS_TABLE_NAME)
    source_mapped = [dict(zip(column_names, tup)) for tup in source_data]
    source_sorted = sorted(source_mapped, key=lambda x: x['id'])
    dest_sorted = sorted(dest_data, key=lambda x: x['id'])

    if len(source_sorted) != len(dest_sorted):
        print(f"Source table {conf.USERS_TABLE_NAME} has {len(source_sorted)} rows, but destination endpoint {dest_endpoint} has {len(dest_sorted)} rows.")
        return False
    
    for i in range(len(source_sorted)):
        # Convert boolean values to integers
        source_row_int = bool_to_int(source_sorted[i])
        # Convert all values in source and dest to their string representation
        source_row_str = {k: str(v) for k, v in source_row_int.items()}
        dest_row_str = {k: str(v) for k, v in dest_sorted[i].items()}

        if source_row_str != dest_row_str:
            # Check by field
            for field in source_row_str:
                if source_row_str[field] != dest_row_str[field]:
                    print(f"Row {i} differs in field {field}: {source_row_str[field]} (Type: {type(source_sorted[i][field])}) != {dest_row_str[field]} (Type: {type(dest_sorted[i][field])})")
                    return False
        
    print(f"Source table {conf.USERS_TABLE_NAME} and destination endpoint {dest_endpoint} are identical.")
    return True

@click.command()
@click.option('--test-connection', is_flag=True, help='Test connections only.')
@click.option('--source-db', type=click.Choice(['PG', 'MYSQL']), default='PG', help='Source database type. Defaults to PG.')
@click.option('--tb-connect-kafka', is_flag=True, help='Create a Kafka connection in Tinybird.')
@click.option('--fetch-users', is_flag=True, help='Fetch and print the user table from source Database.')
@click.option('--drop-table', is_flag=True, help='Drop the Users table from the source Database.')
@click.option('--tb-clean', is_flag=True, help='Clean out the Tinybird Workspace of Pipeline resources.')
@click.option('--tb-include-connector', is_flag=True, help='Also remove the shared Tinybird Confluent connector. Affects all source databases.')
@click.option('--remove-pipeline', is_flag=True, help='Reset the pipeline. Will Drop source table, remove debezium connector, drop the topic, and clean the Tinybird workspace')
@click.option('--create-pipeline', is_flag=True, help='Create the Pipeline. Will create the table, a few initial user events, create debezium connector and topic, and the Tinybird Confluent connection.')
@click.option('--compare-tables', is_flag=True, help='Compare the source table to the destination endpoint.')
def main(test_connection, source_db, tb_connect_kafka, fetch_users, drop_table, tb_clean, tb_include_connector, remove_pipeline, create_pipeline, compare_tables):
    if source_db in ['PG', 'pg']:
        source_db = 'PG'
        debezium_connector_name = conf.PG_CONFLUENT_CONNECTOR_NAME
        # The Kafka Topic Name is generated by the Debezium connector using a fixed structure.
        # See https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-debezium.html
        kafka_topic_name = f"{conf.PG_DATABASE}.public.{conf.USERS_TABLE_NAME}"
        conn = db_functions.pg_connect_db()
        users_api_endpoint = 'pg_users_api_rmt.json'
        db_table_create_func = db_functions.pg_table_create
    elif source_db in ['MYSQL', 'mysql']:
        source_db = 'MYSQL'
        debezium_connector_name = conf.MYSQL_CONFLUENT_CONNECTOR_NAME
        # The Kafka Topic Name is generated by the Debezium connector using a fixed structure.
        # See https://docs.confluent.io/cloud/current/connectors/cc-mysql-source-cdc-debezium.html
        # Note that the documentation says 'schemaName' but that is actually just the database name in these configurations.
        kafka_topic_name = f"{conf.MYSQL_DB_NAME}.{conf.MYSQL_DB_NAME}.{conf.USERS_TABLE_NAME}"
        conn = db_functions.mysql_connect_db()
        users_api_endpoint = 'mysql_users_api_pipe.json' # TODO: elevate to conf.py.
        db_table_create_func = db_functions.mysql_table_create
    else:
        raise Exception(f"Invalid source_db: {source_db}")
    if compare_tables:
        if compare_source_to_dest(conn, users_api_endpoint):
            timer_start = time.time()
            generate_events(conn, num_events=NUM_EVENTS, table_name=conf.USERS_TABLE_NAME)
            print(f'{NUM_EVENTS} events generated in {time.time() - timer_start} seconds.')

            # Wait for events to propagate or until timeout
            wait_start = time.time()
            while not compare_source_to_dest(conn, users_api_endpoint):
                if time.time() - wait_start > PROPAGATION_TIMEOUT:
                    raise Exception("Timeout reached waiting for events to propagate.")
                time.sleep(PROPAGATION_SLEEP_INTERVAL)
            
            total_elapsed = time.time() - timer_start
            print(f'Events propagated in {round(total_elapsed, 2)} seconds.')
        else:
            raise Exception(f"Source table {conf.USERS_TABLE_NAME} and destination endpoint {users_api_endpoint} are not identical to start test.")
    elif remove_pipeline:
        print(f"Resetting the Tinybird pipeline from {source_db}...")
        cc_functions.connector_delete(name=debezium_connector_name, env_name=conf.CONFLUENT_ENV_NAME, cluster_name=conf.CONFLUENT_CLUSTER_NAME)
        cc_functions.k_topic_delete(kafka_topic_name)
        db_functions.table_drop(conn, table_name=conf.USERS_TABLE_NAME)
        tb_functions.clean_workspace(source_db=source_db, include_connector=tb_include_connector)
        print("Pipeline Removed.")
    elif tb_connect_kafka:
        tb_functions.connection_create_kafka(
            kafka_bootstrap_servers=conf.CONFLUENT_BOOTSTRAP_SERVERS,
            kafka_key=conf.CONFLUENT_UNAME,
            kafka_secret=conf.CONFLUENT_SECRET,
            kafka_connection_name=conf.TINYBIRD_CONFLUENT_CONNECTION_NAME,
        )
    elif test_connection:
        test_connectivity(source_db)
    elif fetch_users:
        db_functions.table_print(conn, table_name=conf.USERS_TABLE_NAME)
    elif drop_table:
        db_functions.table_drop(conn, table_name=conf.USERS_TABLE_NAME)
    elif tb_clean:
        tb_functions.clean_workspace(source_db, include_connector=tb_functions.include_connector)
    elif create_pipeline:
        db_table_create_func(conn, table_name=conf.USERS_TABLE_NAME)
        cc_functions.connector_create(name=debezium_connector_name, source_db=source_db, env_name=conf.CONFLUENT_ENV_NAME, cluster_name=conf.CONFLUENT_CLUSTER_NAME)
        tb_functions.ensure_kafka_connection()
        generate_events(conn, num_events=NUM_EVENTS, table_name=conf.USERS_TABLE_NAME)
        # Get listing of definition files
        files_to_upload = tb_functions.get_def_files_for_db(source_db)
        print(f"Updating local Tinybird definition files for {source_db}...")
        tb_functions.update_datasource_info(files_to_upload, kafka_topic_name)
        print(f"Uploading Tinybird definition files for {source_db}...")
        tb_functions.upload_def_for_db(files_to_upload)       
    else:
        try:
            generate_events(conn, num_events=NUM_EVENTS, table_name=conf.USERS_TABLE_NAME)
            print(f'{NUM_EVENTS} events generated.')
        except Exception as e:
            print(f'Error: {e}')
        finally:
            if conn:
                conn.close()

if __name__ == '__main__':
    main()
