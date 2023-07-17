#! /usr/bin/env python3

import psycopg2
from psycopg2 import sql
import mysql.connector
import os
from confluent_kafka.admin import AdminClient
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
CFLT_BASE_URL = "https://api.confluent.cloud/"

# Datagen Constants
INSERT_WEIGHT = 30
UPDATE_WEIGHT = 60
DELETE_WEIGHT = 10
ADDRESS_UPDATE_PROBABILITY = 0.1
NUM_EVENTS = 10
LANGUAGES = ['EN', 'ES', 'FR', 'DE', 'IT']

# Fake data generator
fake = faker.Faker()

# Simple cache for discovered identities
cache = {}

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
    print("Connected to the MySQL database.")
    return conn

def mysql_table_create(conn, table_name=conf.USERS_TABLE_NAME):
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

def pg_table_create(conn, table_name=conf.USERS_TABLE_NAME):
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

def db_table_fetch(conn, table_name=conf.USERS_TABLE_NAME):
    print("Fetching the current list of users...")
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table_name}")
        users = cur.fetchall() or []
    print(f"Fetched {len(users)} users.")
    return users

def db_table_print(conn, table_name=conf.USERS_TABLE_NAME):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name} ORDER BY id ASC")
    users = cur.fetchall()
    print(f"{table_name}:")
    for user in users:
        print(user)
    cur.close()

def db_table_drop(conn, table_name=conf.USERS_TABLE_NAME):
    print(f"Dropping the {table_name} table if exists...")
    cur = conn.cursor()
    cur.execute(f'DROP TABLE IF EXISTS {table_name}')
    conn.commit()
    cur.close()
    print(f"{table_name} table dropped if it existed.")

def k_connect_kadmin():
    print("Connecting to Confluent Cloud Kafka with Admin Client...")
    return AdminClient({
        'bootstrap.servers': conf.CONFLUENT_BOOTSTRAP_SERVERS,
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': conf.CONFLUENT_UNAME,
        'sasl.password': conf.CONFLUENT_SECRET
    })

def k_topic_list():
    kadmin = k_connect_kadmin()
    topics_metadata = kadmin.list_topics(timeout=5)
    if not topics_metadata:
        raise Exception("No topics found.")
    return topics_metadata.topics

def k_topic_delete(topic_name):
    if topic_name not in k_topic_list():
        print(f"Kafka topic {topic_name} not found.")
        return
    print(f"Deleting Kafka topic {topic_name}...")
    kadmin = k_connect_kadmin()
    kadmin.delete_topics([topic_name])
    print(f"Deleted Kafka topic {topic_name}.")

def tb_connection_create_kafka(
    kafka_bootstrap_servers=conf.CONFLUENT_BOOTSTRAP_SERVERS,
    kafka_key=conf.CONFLUENT_UNAME,
    kafka_secret=conf.CONFLUENT_SECRET,
    kafka_connection_name=conf.TINYBIRD_CONFLUENT_CONNECTION_NAME,
    kafka_auto_offset_reset="earliest",
    kafka_schema_registry_url=None,
    kafka_sasl_mechanism="PLAIN"):

    params = {
        'service': 'kafka',
        'kafka_security_protocol': 'SASL_SSL',
        'kafka_sasl_mechanism': kafka_sasl_mechanism,
        'kafka_bootstrap_servers': kafka_bootstrap_servers,
        'kafka_sasl_plain_username': kafka_key,
        'kafka_sasl_plain_password': kafka_secret,
        'name': kafka_connection_name
    }

    if kafka_schema_registry_url:
        params['kafka_schema_registry_url'] = kafka_schema_registry_url
    if kafka_auto_offset_reset:
        params['kafka_auto_offset_reset'] = kafka_auto_offset_reset

    connection_params = {
        key: value
        for key, value in params.items() if value is not None
    }
    resp = requests.post(
        TB_BASE_URL + "connectors",
        headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {conf.TINYBIRD_API_KEY}'},
        data=json.dumps(connection_params)
    )
    resp.raise_for_status()
    print(f"Created Kafka Connector named {kafka_connection_name} with id {resp.json()['id']}")

def tb_connection_list():
    print("Listing Tinybird Connections...")
    resp = requests.get(
        TB_BASE_URL + "connectors",
        headers={'Authorization': f'Bearer {conf.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Found {len(resp.json()['connectors'])} Tinybird Connections.")
    return resp.json()['connectors']

def tb_connection_delete(connection_id):
    print(f"Deleting Kafka Connector with id {connection_id}")
    resp = requests.delete(
        TB_BASE_URL + "connectors/" + connection_id,
        headers={'Authorization': f'Bearer {conf.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Deleted Kafka Connector with id {connection_id}")

def tb_connection_get(name=conf.TINYBIRD_CONFLUENT_CONNECTION_NAME):
    print(f"Getting Tinybird Connection with name {name}...")
    connectors = tb_connection_list()
    for connector in connectors:
        if connector['name'] == name:
            print(f"Found Tinybird Connection with name {name}.")
            return connector
    print(f"Tinybird Connection with name {name} not found.")
    return None

def tb_connection_test():
    connector = tb_connection_get(conf.TINYBIRD_CONFLUENT_CONNECTION_NAME)
    if not connector:
        print(f"Tinybird Confluent Connection not found for name: {conf.TINYBIRD_CONFLUENT_CONNECTION_NAME}")
        return 0
    resp = requests.get(
        TB_BASE_URL + f"connectors/{connector['id']}/preview?preview_activity=false",
        headers={'Authorization': f'Bearer {conf.TINYBIRD_API_KEY}'}
        )
    if not resp.ok:
        print(f"Tinybird Confluent Connection {conf.TINYBIRD_CONFLUENT_CONNECTION_NAME} not working: {resp.text}")
        return -1
    
    print(f"Tinybird Confluent Connection has {len(resp.json()['preview'])} Topics.")
    return 1

def tb_ensure_kafka_connection():
    status = tb_connection_test()
    if status == 0:
        print("Tinybird Confluent Connection not found. Creating...")
        tb_connection_create_kafka()
    elif status == -1:
        print("Tinybird Confluent Connection not working. Deleting and creating...")
        connector = tb_connection_get(conf.TINYBIRD_CONFLUENT_CONNECTION_NAME)
        tb_connection_delete(connector['id'])
        time.sleep(5)
        tb_connection_create_kafka()
        if not tb_connection_test():
            raise Exception("Tinybird Confluent Connection not working after recreation.")
    else:
        print(f"Tinybird Confluent Connection {conf.TINYBIRD_CONFLUENT_CONNECTION_NAME} found.")

def tb_datasources_list():
    print(f"Listing Tinybird Datasources...")
    resp = requests.get(
        TB_BASE_URL + "datasources",
        headers={'Authorization': f'Bearer {conf.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Found {len(resp.json()['datasources'])} Tinybird Datasources.")
    return resp.json()['datasources']

def tb_datasources_truncate(names, include_quarantine=True):
    ds_list = tb_datasources_list()
    for name in names:
        if include_quarantine:
            ds_names = [name, name + "_quarantine"]
        else:
            ds_names = [name]
    for ds_name in ds_names:
        if ds_name not in [x['name'] for x in ds_list]:
            print(f"Tinybird Datasource {ds_name} not found.")
        else:
            print(f"Truncating Tinybird Datasource {ds_name}")
            resp = requests.post(
                TB_BASE_URL + f"datasources/{ds_name}/truncate",
                headers={'Authorization': f'Bearer {conf.TINYBIRD_API_KEY}'}
            )
            resp.raise_for_status()
            print(f"Truncated Tinybird Datasource {ds_name}")

def tb_datasources_delete(names):
    ds_list = tb_datasources_list()
    for name in names:
        if name not in [x['name'] for x in ds_list]:
            print(f"Tinybird Datasource {name} not found.")
        else:
            print(f"Deleting Tinybird Datasource {name}")
            resp = requests.delete(
                TB_BASE_URL + f"datasources/{name}",
                headers={'Authorization': f'Bearer {conf.TINYBIRD_API_KEY}'}
            )
            resp.raise_for_status()
            print(f"Deleted Tinybird Datasource {name}")

def tb_replace_kafka_group_id(filename):
    print(f"Generating new Kafka Group ID in {filename}...")
    with open(filename, 'r') as file:
        file_content = file.read()

    # Replace the KAFKA_GROUP_ID line
    start_index = file_content.find("KAFKA_GROUP_ID '") + len("KAFKA_GROUP_ID '")
    end_index = file_content.find("'", start_index)
    old_group_id = file_content[start_index:end_index]
    
    # Generate new group_id with current unix time
    new_group_id = old_group_id[:old_group_id.rfind("_")+1] + str(int(time.time()))

    file_content = file_content.replace(old_group_id, new_group_id)

    # Write the modified content back to the file
    with open(filename, 'w') as file:
        file.write(file_content)

    print(f"Generated new Kafka Group ID: {new_group_id}")

def tb_upload_def_file(filepath):
    url = TB_BASE_URL + "datafiles"
    filename = os.path.basename(filepath)
    print(f"Reading in file {filepath}...")
    file_data = {filename: open(filepath, 'rb')}
    headers = {
        "Authorization": f"Bearer {conf.TINYBIRD_API_KEY}",
    }
    print(f"Uploading {filepath} to Tinybird...")
    resp = requests.post(
        url + f"?filenames={filename}",
        headers=headers,
        files=file_data,
    )
    if resp.ok:
        print(f"Successfully uploaded {filename} to Tinybird.")
    elif resp.status_code == 400:
        print(f"Tinybird returned an Error: {resp.text}")
    else:
        resp.raise_for_status()

def tb_get_def_files_for_db(source_db):
    print(f"Getting Tinybird definitions for {source_db}...")
    files_to_get = [
        filepath 
        for directory in ["./datasources", "./pipes"]
        for filepath in glob.glob(os.path.join(directory, f"{source_db.lower()}_*"))
    ]
    print(f"Got {len(files_to_get)} Tinybird definitions for {source_db}.")
    return files_to_get

def tb_upload_def_for_db(source_db):
    # We deliberately use the Tinybird definition files here to replicate what a user would typically do in the CLI.
    # It avoids the hassle of converting the schemas and definitions into plain python objects to POST with requests.
    print(f"Uploading Tinybird definitions for {source_db}...")
    # Get listing of definition files
    files_to_upload = tb_get_def_files_for_db(source_db)
    # Replace the Kafka Group ID in the definition files
    [tb_replace_kafka_group_id(filepath) for filepath in files_to_upload if 'raw.datasource' in filepath]
    # Upload files
    _ = [tb_upload_def_file(filepath) for filepath in files_to_upload]
    print(f"Uploaded {len(files_to_upload)} Tinybird definitions for {source_db}.")

def tb_pipes_list():
    print(f"Listing Tinybird Pipes...")
    resp = requests.get(
        TB_BASE_URL + "pipes",
        headers={'Authorization': f'Bearer {conf.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Found {len(resp.json()['pipes'])} Tinybird Pipes.")
    return resp.json()['pipes']

def tb_pipes_delete(names):
    pipes_list = tb_pipes_list()
    for name in names:
        if name not in [x['name'] for x in pipes_list]:
            print(f"Tinybird Pipe {name} not found.")
        else:
            print(f"Deleting Tinybird Pipe {name}")
            resp = requests.delete(
                TB_BASE_URL + f"pipes/{name}",
                headers={'Authorization': f'Bearer {conf.TINYBIRD_API_KEY}'}
            )
            resp.raise_for_status()
            print(f"Deleted Tinybird Pipe {name}")

def tb_clean_workspace(source_db):
    print(f"Cleaning Tinybird workspace for {source_db}...")
    files = tb_get_def_files_for_db(source_db)
    pipes = [file for file in files if os.path.dirname(file) == './pipes']
    datasources = [file for file in files if os.path.dirname(file) == './datasources']
    # Do Pipes first
    tb_pipes_delete([os.path.basename(file).split('.')[0] for file in pipes])
    # Then do Datasources
    tb_datasources_delete([os.path.basename(file).split('.')[0] for file in datasources])
    # Then do Dataspace Tokens
    current_tokens = tb_tokens_list()
    current_token_names = [x['name'] for x in current_tokens]
    kafka_token_patten = "_".join([
        conf.TINYBIRD_CONFLUENT_CONNECTION_NAME,
        source_db.lower(),
        conf.USERS_TABLE_NAME
        ])
    tokens_to_remove = [
        token_name for token_name in current_token_names if token_name.startswith(kafka_token_patten)
        ]
    # Then do Pipe Tokens
    tokens_to_remove += [
        x for x in tb_get_token_names_from_pipes(pipes) if x in current_token_names
        ]
    print(f"Found {len(tokens_to_remove)} Tinybird Tokens to remove for {source_db}.")
    # Do token removal
    _ = [tb_tokens_delete(token_name) for token_name in tokens_to_remove]
    
    
def tb_get_token_names_from_pipes(pipes):
    token_names = []
    token_pattern = "TOKEN"
    for pipe_file in pipes:
        with open(pipe_file, 'r') as file:
            for line in file:
                if line.startswith(token_pattern):
                    token_name = line.split()[1].strip('\"')  # remove quotes from token name
                    token_names.append(token_name)
    return token_names

def tb_tokens_list():
    print(f"Listing Tinybird Tokens...")
    resp = requests.get(
        TB_BASE_URL + "tokens",
        headers={'Authorization': f'Bearer {conf.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Found {len(resp.json()['tokens'])} Tinybird Tokens.")
    return resp.json()['tokens']

def tb_tokens_delete(name):
    print(f"Deleting Tinybird Token {name}")
    resp = requests.delete(
        TB_BASE_URL + f"tokens/{name}",
        headers={'Authorization': f'Bearer {conf.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Deleted Tinybird Token {name}")

def cflt_environment_list():
    print("Listing Confluent Cloud Environments...")
    resp = requests.get(
        CFLT_BASE_URL + "org/v2/environments",
        auth=(conf.CONFLUENT_CLOUD_KEY, conf.CONFLUENT_CLOUD_SECRET)
    )
    resp.raise_for_status()
    return resp.json()['data']

def cflt_environment_get(name=conf.CONFLUENT_ENV_NAME):
    print(f"Getting Confluent Cloud Environment with name {name}...")
    envs = cflt_environment_list()
    for env in envs:
        if env['display_name'] == name:
            return env
    return None

def cflt_cluster_list(environment_name=conf.CONFLUENT_ENV_NAME):
    env_id = cache_cflt_env_id(environment_name)
    print("Listing Confluent Cloud Clusters...")
    resp = requests.get(
        CFLT_BASE_URL + f"cmk/v2/clusters?environment={env_id}",
        auth=(conf.CONFLUENT_CLOUD_KEY, conf.CONFLUENT_CLOUD_SECRET)
    )
    resp.raise_for_status()
    return resp.json()['data']

def cflt_cluster_get(name=conf.CONFLUENT_CLUSTER_NAME, environment_name=conf.CONFLUENT_ENV_NAME):
    print(f"Getting Confluent Cloud Cluster with name {name}...")
    clusters = cflt_cluster_list(environment_name)
    for cluster in clusters:
        if cluster['spec']['display_name'] == name:
            return cluster
    return None

def cache_cflt_env_id(name=conf.CONFLUENT_ENV_NAME):
    if 'clft_env_id' in cache and cache['clft_env_id'] is not None:
        return cache['clft_env_id']
    environment = cflt_environment_get(name)
    if not environment:
        raise Exception(f"Environment {name} not found.")
    cache['clft_env_id'] = environment['id']
    return environment['id']

def cache_cflt_cluster_id(name=conf.CONFLUENT_CLUSTER_NAME):
    if 'clft_cluster_id' in cache and cache['clft_cluster_id'] is not None:
        return cache['clft_cluster_id']
    cluster = cflt_cluster_get(name)
    if not cluster:
        raise Exception(f"Cluster {name} not found.")
    cache['clft_cluster_id'] = cluster['id']
    return cluster['id']

def cflt_connector_list(environment_name=conf.CONFLUENT_ENV_NAME, cluster_name=conf.CONFLUENT_CLUSTER_NAME):
    print(f"Listing Confluent Cloud Connectors for Cluster {cluster_name}...")
    cluster_id = cache_cflt_cluster_id(cluster_name)
    env_id = cache_cflt_env_id(environment_name)
    resp = requests.get(
        CFLT_BASE_URL + f"connect/v1/environments/{env_id}/clusters/{cluster_id}/connectors",
        auth=(conf.CONFLUENT_CLOUD_KEY, conf.CONFLUENT_CLOUD_SECRET)
    )
    resp.raise_for_status()
    return resp.json()

def cflt_connector_delete(name, environment_name=conf.CONFLUENT_ENV_NAME, cluster_name=conf.CONFLUENT_CLUSTER_NAME):
    if name not in cflt_connector_list():
        print(f"Confluent Cloud Connector with name {name} not found.")
        return
    print(f"Deleting Confluent Cloud Connector with name {name}...")
    cluster_id = cache_cflt_cluster_id(cluster_name)
    env_id = cache_cflt_env_id(environment_name)
    resp = requests.delete(
        CFLT_BASE_URL + f"connect/v1/environments/{env_id}/clusters/{cluster_id}/connectors/{name}",
        auth=(conf.CONFLUENT_CLOUD_KEY, conf.CONFLUENT_CLOUD_SECRET)
    )
    resp.raise_for_status()
    print(f"Deleted Confluent Cloud Connector with name {name}")

def cflt_connector_create(name, source_db, environment_name=conf.CONFLUENT_ENV_NAME, cluster_name=conf.CONFLUENT_CLUSTER_NAME):
    # The API response for this call can be obtuse, sending 500 for minor misconfigurations.
    # Therefore change carefully and test thoroughly.
    if name in cflt_connector_list():
        print(f"Confluent Cloud Connector with name {name} already exists.")
        return
    print(f"Creating Confluent Cloud Connector with name {name}...")
    cluster_id = cache_cflt_cluster_id(cluster_name)
    env_id = cache_cflt_env_id(environment_name)

    base_config = {
        "name": name,
        "kafka.auth.mode": "KAFKA_API_KEY",
        "kafka.api.key": conf.CONFLUENT_UNAME,
        "kafka.api.secret": conf.CONFLUENT_SECRET,
        "tasks.max": "1",
        "output.data.format": "JSON",
        "output.key.format": "JSON",
        "cleanup.policy": "delete"
    }
    mysql_config = {
        "connector.class": "MySqlCdcSource",
        "database.hostname": conf.MYSQL_HOST_URL,
        "database.port": str(conf.MYSQL_PORT),
        "database.user": conf.MYSQL_USERNAME,
        "database.password": conf.MYSQL_PASSWORD,
        "database.server.name": conf.MYSQL_DB_NAME,
        "database.whitelist": conf.MYSQL_DB_NAME,
        "table.include.list": '.'.join([conf.MYSQL_DB_NAME, conf.USERS_TABLE_NAME]),
        "database.include.list": "mysql_cdc_demo",
        "snapshot.mode": "when_needed",
        "database.ssl.mode": "preferred"
    }
    pg_config = {
        "connector.class": "PostgresCdcSource",
        "database.hostname": conf.PG_HOST_URL,  # This is the RDS endpoint
        "database.port": str(conf.PG_PORT),
        "database.user": conf.PG_USERNAME,
        "database.password": conf.PG_PASSWORD,
        "database.dbname": conf.PG_DATABASE,
        "database.server.name": conf.PG_DATABASE,  # This is a logical name used for Confluent topics
        "database.sslmode": "require",
        "table.include.list":f"public.{conf.USERS_TABLE_NAME}", 
        "plugin.name": "pgoutput",
        "snapshot.mode": "exported"
    }
    if source_db == 'MYSQL':
        config_sub = {**base_config, **mysql_config}
    elif source_db == 'PG':
        config_sub = {**base_config, **pg_config}
    else:
        raise Exception(f"Invalid source_db: {source_db}")
    json_sub = {
            "name": name,
            "config": config_sub
        }
    # print(f"Using config: {json_sub}")  # Prints full config for debugging. Contains security info.
    resp = requests.post(
        CFLT_BASE_URL + f"connect/v1/environments/{env_id}/clusters/{cluster_id}/connectors",
        headers={'Content-Type': 'application/json'},
        auth=(conf.CONFLUENT_CLOUD_KEY, conf.CONFLUENT_CLOUD_SECRET),
        json=json_sub
    )
    resp.raise_for_status()
    print(f"Created Confluent Cloud Connector with name {name}")

def generate_events(conn, num_events, table_name=conf.USERS_TABLE_NAME):
    print("Generating user events...")
    cur = conn.cursor()

    # Get schema information
    cur.execute(f'SELECT * FROM {table_name} LIMIT 0')
    column_names = [desc[0] for desc in cur.description]
    cur.fetchall()
    deleted_index = column_names.index('deleted')
    email_verified_index = column_names.index('email_verified')
    onboarded_index = column_names.index('onboarded')

    # Generate random user events
    for _ in range(num_events):
        users = db_table_fetch(conn, table_name)
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
            pg_conn = pg_connect_db()
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
            mysql_conn = mysql_connect_db()
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
        tb_connection_test()
    except Exception as e:
        print(f'Error connecting to Tinybird: {e}')

@click.command()
@click.option('--test-connection', is_flag=True, help='Test connections only.')
@click.option('--source-db', type=click.Choice(['PG', 'MYSQL']), default='PG', help='Source database type. Defaults to PG.')
@click.option('--tb-connect-kafka', is_flag=True, help='Create a Kafka connection in Tinybird.')
@click.option('--fetch-users', is_flag=True, help='Fetch and print the user table from source Database.')
@click.option('--drop-table', is_flag=True, help='Drop the Users table from the source Database.')
@click.option('--tb-clean', is_flag=True, help='Clean out the Tinybird Workspace of Pipeline resources.')
@click.option('--remove-pipeline', is_flag=True, help='Reset the pipeline. Will Drop source table, remove debezium connector, drop the topic, and clean the Tinybird workspace')
@click.option('--create-pipeline', is_flag=True, help='Create the Pipeline. Will create the table, a few initial user events, create debezium connector and topic, and the Tinybird Confluent connection.')
def main(test_connection, source_db, tb_connect_kafka, fetch_users, drop_table, tb_clean, remove_pipeline, create_pipeline):
    if source_db in ['PG', 'pg']:
        source_db = 'PG'
        confluent_connection_name = 'PostgresCdcSourceConnector_0'
        kafka_topic_name = f"{conf.PG_DATABASE}.public.{conf.USERS_TABLE_NAME}"
        conn = pg_connect_db()
        db_table_create_func = pg_table_create
    elif source_db in ['MYSQL', 'mysql']:
        source_db = 'MYSQL'
        confluent_connection_name = 'MySqlCdcSourceConnector_0'
        kafka_topic_name = f"{conf.MYSQL_DB_NAME}.public.{conf.USERS_TABLE_NAME}"
        conn = mysql_connect_db()
        db_table_create_func = mysql_table_create
    else:
        raise Exception(f"Invalid source_db: {source_db}")
    if remove_pipeline:
        print(f"Resetting the Tinybird pipeline from {source_db}...")
        cflt_connector_delete(confluent_connection_name)
        k_topic_delete(kafka_topic_name)
        db_table_drop(conn)
        tb_clean_workspace(source_db)
        print("Pipeline Removed.")
    elif tb_connect_kafka:
        tb_connection_create_kafka()
    elif test_connection:
        test_connectivity(source_db)
    elif fetch_users:
        db_table_print(conn)
    elif drop_table:
        db_table_drop(conn)
    elif tb_clean:
        tb_clean_workspace(source_db)
    elif create_pipeline:
        db_table_create_func(conn)
        cflt_connector_create(name=confluent_connection_name, source_db=source_db)
        tb_ensure_kafka_connection()
        generate_events(conn, NUM_EVENTS)
        tb_upload_def_for_db(source_db)       
    else:
        try:
            generate_events(conn, NUM_EVENTS)
            print(f'{NUM_EVENTS} events generated.')
        except Exception as e:
            print(f'Error: {e}')
        finally:
            if conn:
                conn.close()

if __name__ == '__main__':
    main()
