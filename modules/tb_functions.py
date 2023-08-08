# tb_functions.py

import requests
import json
import time
import os
import glob

from .utils import Config
from . import cc_functions

config = Config()

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
    print(f"Created Kafka Connector named {kafka_connection_name} with id {resp.json()['id']}")

def connection_list():
    print("Listing Tinybird Connections...")
    resp = requests.get(
        config.TB_BASE_URL + "connectors",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Found {len(resp.json()['connectors'])} Tinybird Connections.")
    return resp.json()['connectors']

def connection_delete(connection_id):
    print(f"Deleting Kafka Connector with id {connection_id}")
    resp = requests.delete(
        config.TB_BASE_URL + "connectors/" + connection_id,
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Deleted Kafka Connector with id {connection_id}")

def connectors_get(name, enforce_unique=True):
    # Note that connection names are not enforced unique by the service
    print(f"Getting Tinybird Connectors with name {name}...")
    connectors = connection_list()
    out = [connector for connector in connectors if connector['name'] == name]
    if enforce_unique:
        if len(out) > 1:
            raise Exception(f"Found {len(out)} Tinybird Connectors with name {name}, expected 1.")
    if out:
        print(f"Found {len(out)} Tinybird Connectors with name {name}.")
        return out
    print(f"Tinybird Connector(s) with name {name} not found.")
    return []

def connection_test():
    connectors = connectors_get(name=config.TINYBIRD_CONFLUENT_CONNECTION_NAME)
    if not connectors:
        print(f"Tinybird Confluent Connection {config.TINYBIRD_CONFLUENT_CONNECTION_NAME} not found.")
        return 0
    resp = requests.get(
        config.TB_BASE_URL + f"connectors/{connectors[0]['id']}/preview?preview_activity=false",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
        )
    if not resp.ok:
        print(f"Tinybird Confluent Connection {config.TINYBIRD_CONFLUENT_CONNECTION_NAME} not working: {resp.text}")
        return -1
    
    print(f"Tinybird Confluent Connection has {len(resp.json()['preview'])} Topics.")
    return 1

def ensure_kafka_connection():
    status = connection_test()
    if status == 0:
        print("Tinybird Confluent Connection not found. Creating...")
        connection_create_kafka(
            kafka_bootstrap_servers=config.CONFLUENT_BOOTSTRAP_SERVERS,
            kafka_key=config.CONFLUENT_UNAME,
            kafka_secret=config.CONFLUENT_SECRET,
            kafka_connection_name=config.TINYBIRD_CONFLUENT_CONNECTION_NAME,
        )
    elif status == -1:
        print("Tinybird Confluent Connection not working. Deleting and creating...")
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
        print(f"Tinybird Confluent Connection {config.TINYBIRD_CONFLUENT_CONNECTION_NAME} found.")

def datasources_list():
    print(f"Listing Tinybird Datasources...")
    resp = requests.get(
        config.TB_BASE_URL + "datasources",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Found {len(resp.json()['datasources'])} Tinybird Datasources.")
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
            print(f"Tinybird Datasource {ds_name} not found.")
        else:
            print(f"Truncating Tinybird Datasource {ds_name}")
            resp = requests.post(
                config.TB_BASE_URL + f"datasources/{ds_name}/truncate",
                headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
            )
            resp.raise_for_status()
            print(f"Truncated Tinybird Datasource {ds_name}")

def datasources_delete(names):
    ds_list = datasources_list()
    for name in names:
        if name not in [x['name'] for x in ds_list]:
            print(f"Tinybird Datasource {name} not found.")
        else:
            print(f"Deleting Tinybird Datasource {name}")
            resp = requests.delete(
                config.TB_BASE_URL + f"datasources/{name}",
                headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
            )
            resp.raise_for_status()
            print(f"Deleted Tinybird Datasource {name}")

def update_datasource_info(files, kafka_topic):
    for filepath in files:
        if 'raw.datasource' in filepath:
            # Replace the Kafka Group ID in the definition files
            filename = os.path.basename(filepath)
            with open(filepath, 'r') as file:
                file_content = file.read()

            print(f"Generating new Kafka Group ID in {filename}...")
            # Replace the KAFKA_GROUP_ID line
            start_index = file_content.find("KAFKA_GROUP_ID '") + len("KAFKA_GROUP_ID '")
            end_index = file_content.find("'", start_index)
            old_group_id = file_content[start_index:end_index]
            # Generate new group_id with current unix time
            new_group_id = old_group_id[:old_group_id.rfind("_")+1] + str(int(time.time()))
            file_content = file_content.replace(old_group_id, new_group_id)
            print(f"Generated new Kafka Group ID: {new_group_id}")

            # Ensure KAFKA_TOPIC is correct for database and table name
            print(f"Updating Kafka Topic to {kafka_topic}...")
            topic_start_index = file_content.find("KAFKA_TOPIC '") + len("KAFKA_TOPIC '")
            topic_end_index = file_content.find("'", topic_start_index)
            old_topic = file_content[topic_start_index:topic_end_index]
            topics = [x for x in cc_functions.k_topic_list() if kafka_topic in x]
            if len(topics) != 1:
                raise Exception(f"Found {len(topics)} topics matching {kafka_topic}, expected 1.")
            else:
                print(f"Found Kafka Topic {topics[0]} matching {kafka_topic}")
                new_topic = topics[0]
            file_content = file_content.replace(old_topic, new_topic)
            print(f"Updated Kafka Topic to {new_topic}")
            
            # Fix KAFKA_AUTO_OFFSET_RESET as well
            print(f"Updating Kafka Auto Offset Reset to {config.CONFLUENT_OFFSET_RESET}...")
            offset_start_index = file_content.find("KAFKA_AUTO_OFFSET_RESET '") + len("KAFKA_AUTO_OFFSET_RESET '")
            offset_end_index = file_content.find("'", offset_start_index)
            old_offset = file_content[offset_start_index:offset_end_index]
            file_content = file_content.replace(old_offset, config.CONFLUENT_OFFSET_RESET)
            print(f"Updated Kafka Auto Offset Reset to {config.CONFLUENT_OFFSET_RESET}")

            # Fix KAFKA_CONNECTION_NAME
            print(f"Updating Kafka Connection Name to {config.TINYBIRD_CONFLUENT_CONNECTION_NAME}...")
            connection_start_index = file_content.find("KAFKA_CONNECTION_NAME '") + len("KAFKA_CONNECTION_NAME '")
            connection_end_index = file_content.find("'", connection_start_index)
            old_connection = file_content[connection_start_index:connection_end_index]
            file_content = file_content.replace(old_connection, config.TINYBIRD_CONFLUENT_CONNECTION_NAME)
            print(f"Updated Kafka Connection Name to {config.TINYBIRD_CONFLUENT_CONNECTION_NAME}")

            # Write the modified content back to the file
            with open(filepath, 'w') as file:
                file.write(file_content)

def upload_def_file(filepath):
    url = config.TB_BASE_URL + "datafiles"
    filename = os.path.basename(filepath)
    print(f"Reading in file {filepath}...")
    file_data = {filename: open(filepath, 'rb')}
    headers = {
        "Authorization": f"Bearer {config.TINYBIRD_API_KEY}",
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

def get_def_files_for_db(source_db):
    print(f"Getting Tinybird definitions for {source_db}...")
    files_to_get = [
        filepath 
        for directory in ["./datasources", "./pipes"]
        for filepath in glob.glob(os.path.join(directory, f"{source_db.lower()}_*"))
    ]
    print(f"Got {len(files_to_get)} Tinybird definitions for {source_db}.")
    return files_to_get

def upload_def_for_db(files):
    # We deliberately use the Tinybird definition files here to replicate what a user would typically do in the CLI.
    # It avoids the hassle of converting the schemas and definitions into plain python objects to POST with requests.
    # Upload files
    _ = [upload_def_file(filepath) for filepath in files]
    print(f"Uploaded {len(files)} Tinybird definitions.")

def pipes_list():
    print(f"Listing Tinybird Pipes...")
    resp = requests.get(
        config.TB_BASE_URL + "pipes",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Found {len(resp.json()['pipes'])} Tinybird Pipes.")
    return resp.json()['pipes']

def endpoint_fetch(endpoint_name):
    # e.g. https://api.tinybird.co/v0/pipes/pg_users_api_rmt.json
    print(f"Fetching Tinybird Endpoint {endpoint_name}...")
    resp = requests.get(
        config.TB_BASE_URL + f"pipes/{endpoint_name}",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    return resp.json()

def pipes_delete(names):
    pipes_listing = pipes_list()
    for name in names:
        if name not in [x['name'] for x in pipes_listing]:
            print(f"Tinybird Pipe {name} not found.")
        else:
            print(f"Deleting Tinybird Pipe {name}")
            resp = requests.delete(
                config.TB_BASE_URL + f"pipes/{name}",
                headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
            )
            resp.raise_for_status()
            print(f"Deleted Tinybird Pipe {name}")

def clean_workspace(source_db, include_connector=False):
    print(f"Cleaning Tinybird workspace for {source_db}...")
    files = get_def_files_for_db(source_db)
    pipes_to_delete = [file for file in files if os.path.dirname(file) == './pipes']
    datasources_to_delete = [file for file in files if os.path.dirname(file) == './datasources']
    # Do Pipes first
    current_pipe_names = [x['name'] for x in pipes_list()]
    pipenames_to_delete = [os.path.basename(file).split('.')[0] for file in pipes_to_delete]
    pipes_delete([x for x in pipenames_to_delete if x in current_pipe_names])
    # Then do Datasources
    current_datasource_names = [x['name'] for x in datasources_list()]
    datasourcenames_to_delete = [os.path.basename(file).split('.')[0] for file in datasources_to_delete]
    datasources_delete([x for x in datasourcenames_to_delete if x in current_datasource_names])
    # Then do Dataspace Tokens
    current_tokens = tokens_list()
    current_token_names = [x['name'] for x in current_tokens]
    kafka_token_patten = "_".join([
        config.TINYBIRD_CONFLUENT_CONNECTION_NAME,
        source_db.lower(),
        config.USERS_TABLE_NAME
        ])
    tokens_to_remove = [
        token_name for token_name in current_token_names if token_name.startswith(kafka_token_patten)
        ]
    # Then do Pipe Tokens
    tokens_to_remove += [
        x for x in get_token_names_from_pipes(pipes_to_delete) if x in current_token_names
        ]
    print(f"Found {len(tokens_to_remove)} Tinybird Tokens to remove for {source_db}.")
    # Do token removal
    _ = [tokens_delete(token_name) for token_name in set(tokens_to_remove)]
    if include_connector:
        # Do Connectors
        connector = connectors_get(name=config.TINYBIRD_CONFLUENT_CONNECTION_NAME)
        if connector:
            connection_delete(connector[0]['id'])
        else:
            print(f"Tinybird Confluent Connection not found for name: {config.TINYBIRD_CONFLUENT_CONNECTION_NAME}")

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
    print(f"Listing Tinybird Tokens...")
    resp = requests.get(
        config.TB_BASE_URL + "tokens",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Found {len(resp.json()['tokens'])} Tinybird Tokens.")
    return resp.json()['tokens']

def tokens_delete(name):
    print(f"Deleting Tinybird Token {name}")
    resp = requests.delete(
        config.TB_BASE_URL + f"tokens/{name}",
        headers={'Authorization': f'Bearer {config.TINYBIRD_API_KEY}'}
    )
    resp.raise_for_status()
    print(f"Deleted Tinybird Token {name}")

