# tb_functions.py

import requests
import json
import time
import os
import glob

from .utils import Config, setup_logging
from . import cc_functions

config = Config()
logger = setup_logging()

def connection_create_kafka(
    kafka_bootstrap_servers,
    kafka_key,
    kafka_secret,
    kafka_connection_name,
    kafka_auto_offset_reset=config.CONFLUENT_OFFSET_RESET,
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
        config.TB_BASE_URL + "connectors",  # TODO: Note: Undocumented.
        headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'},
        data=json.dumps(connection_params)
    )
    resp.raise_for_status()
    logger.info(f"Created Kafka Connector named {kafka_connection_name} with id {resp.json()['id']}")

def connection_list():
    logger.info("Listing Tinybird Connections...")
    resp = requests.get(
        config.TB_BASE_URL + "connectors",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    logger.info(f"Found {len(resp.json()['connectors'])} Tinybird Connections.")
    return resp.json()['connectors']

def connection_delete(connection_id):
    logger.info(f"Deleting Kafka Connector with id {connection_id}")
    resp = requests.delete(
        config.TB_BASE_URL + "connectors/" + connection_id,
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    logger.info(f"Deleted Kafka Connector with id {connection_id}")

def connectors_get(name, enforce_unique=True):
    # Note that connection names are not enforced unique by the service
    logger.info(f"Getting Tinybird Connectors with name {name}...")
    connectors = connection_list()
    out = [connector for connector in connectors if connector['name'] == name]
    if enforce_unique:
        if len(out) > 1:
            raise Exception(f"Found {len(out)} Tinybird Connectors with name {name}, expected 1.")
    if out:
        logger.info(f"Found {len(out)} Tinybird Connectors with name {name}.")
        return out
    logger.info(f"Tinybird Connector(s) with name {name} not found.")
    return []

def connection_test():
    connectors = connectors_get(name=config.TINYBIRD_CONFLUENT_CONNECTION_NAME)
    if not connectors:
        logger.info(f"Tinybird Confluent Connection {config.TINYBIRD_CONFLUENT_CONNECTION_NAME} not found.")
        return 0
    resp = requests.get(
        config.TB_BASE_URL + f"connectors/{connectors[0]['id']}/preview?preview_activity=false",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
        )
    if not resp.ok:
        logger.info(f"Tinybird Confluent Connection {config.TINYBIRD_CONFLUENT_CONNECTION_NAME} not working: {resp.text}")
        return -1
    
    logger.info(f"Tinybird Confluent Connection has {len(resp.json()['preview'])} Topics.")
    return 1

def ensure_kafka_connection():
    status = connection_test()
    if status == 0:
        logger.info("Tinybird Confluent Connection not found. Creating...")
        connection_create_kafka(
            kafka_bootstrap_servers=config.CONFLUENT_BOOTSTRAP_SERVERS,
            kafka_key=config.CONFLUENT_UNAME,
            kafka_secret=config.CONFLUENT_SECRET,
            kafka_connection_name=config.TINYBIRD_CONFLUENT_CONNECTION_NAME,
        )
    elif status == -1:
        logger.info("Tinybird Confluent Connection not working. Deleting and creating...")
        connector = connectors_get(name=config.TINYBIRD_CONFLUENT_CONNECTION_NAME)
        connection_delete(connector[0]['id'])
        time.sleep(5)
        connection_create_kafka(
            kafka_bootstrap_servers=config.CONFLUENT_BOOTSTRAP_SERVERS,
            kafka_key=config.CONFLUENT_UNAME,
            kafka_secret=config.CONFLUENT_SECRET,
            kafka_connection_name=config.TINYBIRD_CONFLUENT_CONNECTION_NAME,
        )
        
        if not connection_test():
            raise Exception("Tinybird Confluent Connection not working after recreation.")
    else:
        logger.info(f"Tinybird Confluent Connection {config.TINYBIRD_CONFLUENT_CONNECTION_NAME} found.")

def datasources_list():
    logger.info(f"Listing Tinybird Datasources...")
    resp = requests.get(
        config.TB_BASE_URL + "datasources",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    logger.info(f"Found {len(resp.json()['datasources'])} Tinybird Datasources.")
    return resp.json()['datasources']

def datasources_truncate(names, include_quarantine=True):
    ds_list = datasources_list()
    for name in names:
        if include_quarantine:
            ds_names = [name, name + "_quarantine"]
        else:
            ds_names = [name]
    for ds_name in ds_names:
        if ds_name not in [x['name'] for x in ds_list]:
            logger.info(f"Tinybird Datasource {ds_name} not found.")
        else:
            logger.info(f"Truncating Tinybird Datasource {ds_name}")
            resp = requests.post(
                config.TB_BASE_URL + f"datasources/{ds_name}/truncate",
                headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
            )
            resp.raise_for_status()
            logger.info(f"Truncated Tinybird Datasource {ds_name}")

def datasources_delete(names):
    ds_list = datasources_list()
    for name in names:
        if name not in [x['name'] for x in ds_list]:
            logger.info(f"Tinybird Datasource {name} not found.")
        else:
            logger.info(f"Deleting Tinybird Datasource {name}")
            resp = requests.delete(
                config.TB_BASE_URL + f"datasources/{name}",
                headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
            )
            resp.raise_for_status()
            logger.info(f"Deleted Tinybird Datasource {name}")

def update_datasource_info(files):
    for filepath in files:
        if 'raw.datasource' in filepath:
            # Replace the Kafka Group ID in the definition files
            filename = os.path.basename(filepath)
            with open(filepath, 'r') as file:
                file_content = file.read()

            # Ensure KAFKA_TOPIC is correct for database and table name
            # Expecting file to be named in the format <table_name>_raw.datasource
            table_name = filename.split('_')[0]
            kafka_topic = config.KAFKA_CDC_TOPICS[table_name]
            logger.info(f"Updating Kafka Topic to {kafka_topic}...")
            topic_start_index = file_content.find("KAFKA_TOPIC '") + len("KAFKA_TOPIC '")
            topic_end_index = file_content.find("'", topic_start_index)
            old_topic = file_content[topic_start_index:topic_end_index]
            topics = [x for x in cc_functions.k_topic_list() if kafka_topic in x]
            if len(topics) != 1:
                raise Exception(f"Found {len(topics)} topics matching {kafka_topic}, expected 1.")
            else:
                logger.info(f"Found Kafka Topic {topics[0]} matching {kafka_topic}")
                new_topic = topics[0]
            file_content = file_content.replace(old_topic, new_topic)
            logger.info(f"Updated Kafka Topic to {new_topic}")

            logger.info(f"Generating new Kafka Group ID in {filename}...")
            # Replace the KAFKA_GROUP_ID line
            start_index = file_content.find("KAFKA_GROUP_ID '") + len("KAFKA_GROUP_ID '")
            end_index = file_content.find("'", start_index)
            old_group_id = file_content[start_index:end_index]
            # Generate new group_id with current unix time
            new_group_id = new_topic + '_' + str(int(time.time()))
            file_content = file_content.replace(old_group_id, new_group_id)
            logger.info(f"Generated new Kafka Group ID: {new_group_id}")
            
            # Fix KAFKA_AUTO_OFFSET_RESET as well
            logger.info(f"Updating Kafka Auto Offset Reset to {config.CONFLUENT_OFFSET_RESET}...")
            offset_start_index = file_content.find("KAFKA_AUTO_OFFSET_RESET '") + len("KAFKA_AUTO_OFFSET_RESET '")
            offset_end_index = file_content.find("'", offset_start_index)
            old_offset = file_content[offset_start_index:offset_end_index]
            file_content = file_content.replace(old_offset, config.CONFLUENT_OFFSET_RESET)
            logger.info(f"Updated Kafka Auto Offset Reset to {config.CONFLUENT_OFFSET_RESET}")

            # Fix KAFKA_CONNECTION_NAME
            logger.info(f"Updating Kafka Connection Name to {config.TINYBIRD_CONFLUENT_CONNECTION_NAME}...")
            connection_start_index = file_content.find("KAFKA_CONNECTION_NAME '") + len("KAFKA_CONNECTION_NAME '")
            connection_end_index = file_content.find("'", connection_start_index)
            old_connection = file_content[connection_start_index:connection_end_index]
            file_content = file_content.replace(old_connection, config.TINYBIRD_CONFLUENT_CONNECTION_NAME)
            logger.info(f"Updated Kafka Connection Name to {config.TINYBIRD_CONFLUENT_CONNECTION_NAME}")

            # Write the modified content back to the file
            with open(filepath, 'w') as file:
                file.write(file_content)

def upload_def_file(filepath):
    url = config.TB_BASE_URL + "datafiles"
    filename = os.path.basename(filepath)
    logger.info(f"Reading in file {filepath}...")
    file_data = {filename: open(filepath, 'rb')}
    headers = {
        "Authorization": f"Bearer {config.TINYBIRD_API_KEY}",
    }
    logger.info(f"Uploading {filepath} to Tinybird...")
    resp = requests.post(
        url + f"?filenames={filename}",
        headers=headers,
        files=file_data,
    )
    if resp.ok:
        logger.info(f"Successfully uploaded {filename} to Tinybird.")
    elif resp.status_code == 400:
        logger.info(f"Tinybird returned an Error: {resp.text}")
        resp.raise_for_status()
    else:
        resp.raise_for_status()

def upload_def_for_db(files):
    # We deliberately use the Tinybird definition files here to replicate what a user would typically do in the CLI.
    # It avoids the hassle of converting the schemas and definitions into plain python objects to POST with requests.
    # Upload files
    _ = [upload_def_file(filepath) for filepath in files]
    logger.info(f"Uploaded {len(files)} Tinybird definitions.")

def pipes_list():
    logger.info(f"Listing Tinybird Pipes...")
    resp = requests.get(
        config.TB_BASE_URL + "pipes",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    logger.info(f"Found {len(resp.json()['pipes'])} Tinybird Pipes.")
    return resp.json()['pipes']

def endpoint_fetch(endpoint_name, strict=True):
    # e.g. https://api.tinybird.co/v0/pipes/pg_users_api_rmt.json
    logger.info(f"Fetching Tinybird Endpoint {endpoint_name}...")
    resp = requests.get(
        config.TB_BASE_URL + f"pipes/{endpoint_name}",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    if strict:
        resp.raise_for_status()
    return resp.json()

def pipes_delete(names):
    pipes_listing = pipes_list()
    for name in names:
        if name not in [x['name'] for x in pipes_listing]:
            logger.info(f"Tinybird Pipe {name} not found.")
        else:
            logger.info(f"Deleting Tinybird Pipe {name}")
            resp = requests.delete(
                config.TB_BASE_URL + f"pipes/{name}",
                headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
            )
            resp.raise_for_status()
            logger.info(f"Deleted Tinybird Pipe {name}")

def clean_workspace(files, include_connector=False):
    pipes_to_delete = [file for file in files if '/pipes' in os.path.dirname(file)]
    datasources_to_delete = [file for file in files if '/datasources' in os.path.dirname(file)]
    # Do Pipes first
    pipenames_to_delete = [os.path.basename(file).split('.')[0] for file in pipes_to_delete]
    logger.info(f"Requesting Delete of Pipes: {pipenames_to_delete}")
    pipes_delete(pipenames_to_delete)
    # Then do Datasources
    datasourcenames_to_delete = [os.path.basename(file).split('.')[0] for file in datasources_to_delete]
    logger.info(f"Requesting Delete of Datasources: {datasourcenames_to_delete}")
    datasources_delete(datasourcenames_to_delete)
    # Then do Workspace Tokens
    tokens_to_remove = []
    # First Datasources
    for datasource_name in datasourcenames_to_delete:
        tokens_to_remove.append("_".join([
            config.TINYBIRD_CONFLUENT_CONNECTION_NAME,
            datasource_name
            ]))
    # Then add Pipe Tokens
    tokens_to_remove += get_token_names_from_pipes(pipes_to_delete)
    logger.info(f"Requesting Delete of Tokens: {tokens_to_remove}")
    # Do token removal
    tokens_delete(set(tokens_to_remove))
    if include_connector:
        # Do Connectors
        connector = connectors_get(name=config.TINYBIRD_CONFLUENT_CONNECTION_NAME)
        if connector:
            connection_delete(connector[0]['id'])
        else:
            logger.info(f"Tinybird Confluent Connection not found for name: {config.TINYBIRD_CONFLUENT_CONNECTION_NAME}")

def get_token_names_from_pipes(pipes):
    token_names = []
    token_pattern = "TOKEN"
    for pipe_file in pipes:
        with open(pipe_file, 'r') as file:
            for line in file:
                if line.startswith(token_pattern):
                    token_name = line.split()[1].strip('\"')  # remove quotes from token name
                    token_names.append(token_name)
    return token_names

def tokens_list():
    logger.info(f"Listing Tinybird Tokens...")
    resp = requests.get(
        config.TB_BASE_URL + "tokens",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    logger.info(f"Found {len(resp.json()['tokens'])} Tinybird Tokens.")
    return resp.json()['tokens']

def tokens_delete(names):
    token_list = tokens_list()
    for name in names:
        if name not in [x['name'] for x in token_list]:
            logger.info(f"Tinybird Token {name} not found.")
        else:
            logger.info(f"Deleting Tinybird Token {name}")
            resp = requests.delete(
                config.TB_BASE_URL + f"tokens/{name}",
                headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
            )
            resp.raise_for_status()
            logger.info(f"Deleted Tinybird Token {name}")

