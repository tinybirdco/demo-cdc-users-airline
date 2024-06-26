#! /usr/bin/env python3

from modules import tb_functions # A collection of Tinybird function for managing resources. 
from modules import cc_functions # A wrapper for the Confluent APIs. List environemnts and clusters, and creates the CDC Connector. 
from modules import db_functions # Contains vanilla SQL queries and Postgres/MySQL-specific methods. 
from modules import utils # Passes the conf.py details to the other modules. 

import random
import time
import faker
import click
from datetime import datetime

config = utils.Config()

# Demo Constants
INSERT_WEIGHT = 30
UPDATE_WEIGHT = 60
DELETE_WEIGHT = 10
ADDRESS_UPDATE_PROBABILITY = 0.1

MYSQL_ENDPOINT_NAME = 'users_api.json'
PG_ENDPOINT_NAME = 'users_api_batch.json'
LANGUAGES = ['EN', 'ES', 'FR', 'DE', 'IT']

# These should be create if not exists statements for the table
PG_USERS_TABLE_CREATE = f'''
    CREATE TABLE IF NOT EXISTS {config.USERS_TABLE_NAME} (
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
    '''

MYSQL_USERS_TABLE_CREATE = f'''
    CREATE TABLE IF NOT EXISTS {config.USERS_TABLE_NAME} (
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
    '''

TABLES_TO_REPLICATE = [config.USERS_TABLE_NAME, ]

# Fake data generator
fake = faker.Faker()

def generate_events(conn, num_events, table_name):
    print("Generating user events...")
    cur = conn.cursor()
    column_names = db_functions.table_column_names(conn, table_name)
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
    # Test Database connection
    db_functions.test_db_connection(db_type)
    # Test Confluent Kafka connection
    try:
        topics = cc_functions.k_topic_list()
        print(f'Available topics: {len(topics)}')
    except Exception as e:
        print(f'Error connecting to Confluent Kafka: {e}')
    # Test Tinybird connection
    try:
        tb_functions.connection_test()
    except Exception as e:
        print(f'Error connecting to Tinybird: {e}')

def compare_source_to_dest(source_conn, dest_endpoint):
    dest_rest = tb_functions.endpoint_fetch(dest_endpoint)
    dest_data = dest_rest['data'] or []
    if not dest_data:
        print(f"Destination endpoint {dest_endpoint} is empty.")
        return False

    source_data = db_functions.table_fetch(source_conn, config.USERS_TABLE_NAME)
    if not source_data:
        print(f"Source table {config.USERS_TABLE_NAME} is empty.")
        return False
    
    # Convert to dicts for comparison
    column_names = column_names = db_functions.table_column_names(source_conn, config.USERS_TABLE_NAME)
    source_mapped = [dict(zip(column_names, tup)) for tup in source_data]
    source_sorted = sorted(source_mapped, key=lambda x: x['id'])
    dest_sorted = sorted(dest_data, key=lambda x: x['id'])

    if len(source_sorted) != len(dest_sorted):
        print(f"Source table {config.USERS_TABLE_NAME} has {len(source_sorted)} rows, but destination endpoint {dest_endpoint} has {len(dest_sorted)} rows.")
        return False
    
    for i in range(len(source_sorted)):
        # Convert boolean values to integers
        source_row_int = utils.bool_to_int(source_sorted[i])
        # Convert all values in source and dest to their string representation
        source_row_str = {k: str(v) for k, v in source_row_int.items()}
        dest_row_str = {k: str(v) for k, v in dest_sorted[i].items()}

        if source_row_str != dest_row_str:
            # Check by field
            for field in source_row_str:
                if source_row_str[field] != dest_row_str[field]:
                    print(f"Row {i} differs in field {field}: {source_row_str[field]} (Type: {type(source_sorted[i][field])}) != {dest_row_str[field]} (Type: {type(dest_sorted[i][field])})")
                    return False
        
    print(f"Source table {config.USERS_TABLE_NAME} and destination endpoint {dest_endpoint} are identical.")
    return True

@click.command()
@click.option('--source-db', type=click.Choice(['PG', 'MYSQL']), default='PG', help='Source database type. Defaults to PG.')
@click.option('--num-events', '-n', type=int, default=10)
@click.option('--test-connection', is_flag=True, help='Test connections only.')
@click.option('--tb-connect-kafka', is_flag=True, help='Create a Kafka connection in Tinybird.')
@click.option('--fetch-users', is_flag=True, help='Fetch and print the site table from source Database.') # [] Object-type-specific.
@click.option('--drop-table', is_flag=True, help='Drop the Users table from the source Database.')
@click.option('--tb-clean', is_flag=True, help='Clean out the Tinybird Workspace of Pipeline resources.')
@click.option('--tb-include-connector', is_flag=True, help='Also remove the shared Tinybird Confluent connector. Affects all source databases.')
@click.option('--remove-pipeline', is_flag=True, help='Reset the pipeline. Will Drop source table, remove debezium connector, drop the topic, and clean the Tinybird workspace')
@click.option('--create-pipeline', is_flag=True, help='Create the Pipeline. Will create the table, a few initial user events, create debezium connector and topic, and the Tinybird Confluent connection.')
@click.option('--compare-tables', is_flag=True, help='Compare the source table to the destination endpoint.')
def main(source_db, num_events, test_connection, tb_connect_kafka, fetch_users, drop_table, tb_clean, tb_include_connector, remove_pipeline, create_pipeline, compare_tables):
    if source_db in ['PG', 'pg']:
        source_db = 'PG'
        debezium_connector_name = config.PG_CONFLUENT_CONNECTOR_NAME
        conn = db_functions.pg_connect_db()
        users_api_endpoint = PG_ENDPOINT_NAME
        db_table_create_query = PG_USERS_TABLE_CREATE
    elif source_db in ['MYSQL', 'mysql']:
        source_db = 'MYSQL'
        debezium_connector_name = config.MYSQL_CONFLUENT_CONNECTOR_NAME
        conn = db_functions.mysql_connect_db()
        users_api_endpoint = MYSQL_ENDPOINT_NAME
        db_table_create_query = MYSQL_USERS_TABLE_CREATE
    else:
        raise Exception(f"Invalid source_db: {source_db}")
    project_kit_path = 'kits/users/' + source_db.lower()
    config.set_source_db(source_db)
    config.set_kafka_topics(table_names=TABLES_TO_REPLICATE)
    config.set_include_tables(TABLES_TO_REPLICATE)
    def_files = utils.get_all_files_in_directory(directory=project_kit_path)

    if compare_tables:
        if compare_source_to_dest(conn, users_api_endpoint):
            # Mark start time.
            start_time = time.time()
            generate_events(conn, num_events, table_name=config.USERS_TABLE_NAME)
            
            # Mark event generation finish time.
            generated_time = time.time()
            generate_duration = generated_time - start_time
            print(f'{num_events} events generated in {round(generate_duration,2)} seconds.')

            # Wait for events to propagate or until timeout
            while not compare_source_to_dest(conn, users_api_endpoint):
                if time.time() - generated_time > config.TIMEOUT_WAIT:
                    raise Exception("Timeout reached waiting for events to propagate.")
                time.sleep(config.SLEEP_WAIT)

            # Mark propagation finish time.
            progagated_time = time.time()
            prograte_duration = progagated_time - generated_time 
            total_duration = progagated_time - start_time
            
            print(f"{num_events} events created and propagated in {round(total_duration, 2)} seconds. Generation took {round(generate_duration,2)} seconds.")
            print(f"Time from last database update to API Endpoint providing finalized data: {round(prograte_duration,2)} seconds.")
        else:
            print(f"Source table {config.USERS_TABLE_NAME} and destination endpoint {users_api_endpoint} are either empty or not identical.")
    elif remove_pipeline:
        print(f"Resetting the Tinybird pipeline from {config.SOURCE_DB}...")
        cc_functions.connector_delete(name=debezium_connector_name, env_name=config.CONFLUENT_ENV_NAME, cluster_name=config.CONFLUENT_CLUSTER_NAME)
        cc_functions.k_topic_cleanup()
        db_functions.table_drop(conn, table_name=config.USERS_TABLE_NAME)
        tb_functions.clean_workspace(files=def_files, include_connector=tb_include_connector)
        print("Pipeline Removed.")
    elif tb_connect_kafka:
        tb_functions.connection_create_kafka(
            kafka_bootstrap_servers=config.CONFLUENT_BOOTSTRAP_SERVERS,
            kafka_key=config.CONFLUENT_UNAME,
            kafka_secret=config.CONFLUENT_SECRET,
            kafka_connection_name=config.TINYBIRD_CONFLUENT_CONNECTION_NAME,
        )
    elif test_connection:
        test_connectivity(config.SOURCE_DB)
    elif fetch_users:
        db_functions.table_print(conn, table_name=config.USERS_TABLE_NAME)
    elif drop_table:
        db_functions.table_drop(conn, table_name=config.USERS_TABLE_NAME)
    elif tb_clean:
        tb_functions.clean_workspace(files=def_files, include_connector=tb_include_connector)
    elif create_pipeline:
        db_functions.table_create(
            conn,
            table_name=config.USERS_TABLE_NAME,
            query=db_table_create_query)
        cc_functions.connector_create(
            name=debezium_connector_name, source_db=config.SOURCE_DB, env_name=config.CONFLUENT_ENV_NAME, cluster_name=config.CONFLUENT_CLUSTER_NAME,
            table_include_list=config.INCLUDE_TABLES)
        tb_functions.ensure_kafka_connection()
        print(f"Updating local Tinybird definition files for {config.SOURCE_DB}...")
        tb_functions.update_datasource_info(def_files)
        print(f"Uploading Tinybird definition files for {config.SOURCE_DB}...")
        tb_functions.upload_def_for_db(def_files)
        time.sleep(3)  # Give it a few seconds to warm up
        generate_events(conn, num_events=num_events, table_name=config.USERS_TABLE_NAME)
    else:
        try:
            generate_events(conn, num_events, table_name=config.USERS_TABLE_NAME)
            print(f'{num_events} events generated.')
        except Exception as e:
            print(f'Error: {e}')
        finally:
            if conn:
                conn.close()

if __name__ == '__main__':
    main()
