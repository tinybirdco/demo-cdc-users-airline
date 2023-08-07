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

PROPAGATION_TIMEOUT = 15  # seconds
PROPAGATION_SLEEP_INTERVAL = 1  # seconds

# Fake data generator
fake = faker.Faker()

# Simple cache for discovered identities
cache = {}

def mysql_connect_db():
    print("Connecting to the MySQL database...")

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
    print("Users table created successfully.")
   
def db_table_fetch(conn, table_name):
    print("Fetching the current list of users...")

def db_table_print(conn, table_name):
    cur = conn.cursor()
    cur.close()

def db_table_drop(conn, table_name):
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
    print(f"Found {len(topics_metadata.topics)} Kafka topics.")
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
    kafka_bootstrap_servers,
    kafka_key,
    kafka_secret,
    kafka_connection_name,
    kafka_auto_offset_reset=conf.CONFLUENT_OFFSET_RESET,
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

def tb_connection_get(name):
    print(f"Getting Tinybird Connection with name {name}...")
    connectors = tb_connection_list()
    for connector in connectors:
        if connector['name'] == name:
            print(f"Found Tinybird Connection with name {name}.")
            return connector
    print(f"Tinybird Connection with name {name} not found.")
    return None

def tb_connection_test():
    connector = tb_connection_get(name=conf.TINYBIRD_CONFLUENT_CONNECTION_NAME)
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


def cflt_environment_list():
    print("Listing Confluent Cloud Environments...")
    resp = requests.get(
        CFLT_BASE_URL + "org/v2/environments",
        auth=(conf.CONFLUENT_CLOUD_KEY, conf.CONFLUENT_CLOUD_SECRET)
    )
    resp.raise_for_status()
    return resp.json()['data']

def cflt_environment_get(env_name):
    print(f"Getting Confluent Cloud Environment with name {env_name}...")
    envs = cflt_environment_list()
    for env in envs:
        if env['display_name'] == env_name:
            return env
    return None

def cflt_cluster_list(env_name):
    env_id = cache_cflt_env_id(env_name)
    print("Listing Confluent Cloud Clusters...")
    resp = requests.get(
        CFLT_BASE_URL + f"cmk/v2/clusters?environment={env_id}",
        auth=(conf.CONFLUENT_CLOUD_KEY, conf.CONFLUENT_CLOUD_SECRET)
    )
    resp.raise_for_status()
    return resp.json()['data']

def cflt_cluster_get(cluster_name, env_name):
    print(f"Getting Confluent Cloud Cluster with name {cluster_name}...")
    clusters = cflt_cluster_list(env_name)
    for cluster in clusters:
        if cluster['spec']['display_name'] == cluster_name:
            return cluster
    return None


def db_get_column_names(conn, table_name):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table_name} LIMIT 0')
    column_names = [desc[0] for desc in cur.description]
    cur.close()
    return column_names

def generate_events(conn, num_events, table_name):
    print("Generating user events...")
    cur = conn.cursor()
    print("User events generated.")

def test_connectivity(db_type):
    # Test PostgreSQL connection
    pass

def compare_source_to_dest(source_conn, dest_endpoint):
    dest_rest = tb_endpoint_fetch(dest_endpoint)

def main(test_connection, source_db, tb_connect_kafka, fetch_users, drop_table, tb_clean, tb_include_connector, remove_pipeline, create_pipeline, compare_tables):
    pass
if __name__ == '__main__':
    main()
