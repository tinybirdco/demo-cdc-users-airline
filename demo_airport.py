#! /usr/bin/env python3

from modules import tb_functions
from modules import cc_functions
from modules import db_functions
from modules import utils

import random
import time
import faker
import click
from datetime import datetime

config = utils.Config()
# Fake data generator
fake = faker.Faker()
# Logging facility
logger = utils.setup_logging(print_to_console=True)

# Demo Constants

FLIGHTS_TABLE_NAME = 'flights'
FLIGHTS_TABLE_CREATE_QUERY = f'''
    CREATE TABLE IF NOT EXISTS {FLIGHTS_TABLE_NAME} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        flight_number VARCHAR(10) NOT NULL,
        passenger_count INT NOT NULL,
        status ENUM('open', 'closed', 'boarding', 'boarded', 'departed', 'arrived', 'offboarding', 'completed') NOT NULL,
        closed_at DATETIME,
        boarding_at DATETIME,
        boarded_at DATETIME,
        departed_at DATETIME,
        arrived_at DATETIME,
        offboarded_at DATETIME,
        completed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    '''

PASSENGER_TABLE_NAME = 'passengers'
PASSENGER_TABLE_CREATE_QUERY = f'''
    CREATE TABLE IF NOT EXISTS {PASSENGER_TABLE_NAME} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        flight_id INT NOT NULL,
        status ENUM('checkedin', 'onboarded', 'notboarded', 'departed', 'arrived', 'completed') NOT NULL,
        onboarded_at DATETIME,
        notboarded_at DATETIME,
        departed_at DATETIME,
        arrived_at DATETIME,
        complated_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    '''

BAGGAGE_TABLE_NAME = 'baggage'
BAGGAGE_TABLE_CREATE_QUERY = f'''
    CREATE TABLE IF NOT EXISTS {BAGGAGE_TABLE_NAME} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        passenger_id INT NOT NULL,
        flight_id INT NOT NULL,
        weight DOUBLE NOT NULL,
        status ENUM('checkedin', 'loaded', 'arrived', 'offloaded', 'missing', 'completed') NOT NULL,
        loaded_at DATETIME,
        arrived_at DATETIME,
        offloaded_at DATETIME,
        missing_at DATETIME,
        completed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (passenger_id) REFERENCES passengers(id) ON DELETE CASCADE,
        FOREIGN KEY (flight_id) REFERENCES flights(id) ON DELETE CASCADE
        )
    '''

TABLES_TO_REPLICATE = [FLIGHTS_TABLE_NAME, PASSENGER_TABLE_NAME, BAGGAGE_TABLE_NAME]

# All times in seconds as this is a fast simulation
MAX_ACTIVE_FLIGHTS = 25
NEW_FLIGHT_PERCENTAGE = 15
MIN_PASSENGER_COUNT = 50
MAX_PASSENGER_COUNT = 320
MIN_BAGGAGE_WEIGHT = 5.0
AVG_BAGGAGE_WEIGHT = 15.0
STEP_BAGGAGE_WEIGHT = 3.0
MAX_BAGGAGE_WEIGHT = 32.0
AVG_BAGS_PER_PASSENGER = 1.3
STEP_BAGS_PER_PASSENGER = 0.5
MAX_BAGS_PER_PASSENGER = 5
CLOSE_FLIGHT_PERCENTAGE = 95
MIN_BOARDING_WAIT = 10
START_BOARDING_PERCENTAGE = 20
MAX_BOARDING_WAIT = 30
MAX_BOARDING_BATCH = 20
CLOSE_BOARDING_PERCENTAGE = 98
BOARDING_DENIED_PERCENTAGE = 3
START_DEPARTURE_PERCENTAGE = 20
MIN_DEPARTURE_WAIT = 5
MAX_DEPARTURE_WAIT = 20

def get_active_flights(conn):
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT * FROM {FLIGHTS_TABLE_NAME} WHERE status NOT IN ('completed', 'departed')")
    resp = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]  # get column names from the cursor description
    
    flights = [dict(zip(column_names, row)) for row in resp]  # Convert each row into a dictionary
    return flights

def generate_flight(conn):
    flight_number = str(fake.bothify(text='??###')).upper()
    passenger_count = random.randint(MIN_PASSENGER_COUNT, MAX_PASSENGER_COUNT)
    status = 'open'
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {FLIGHTS_TABLE_NAME} (flight_number, passenger_count, status) VALUES (%s, %s, %s)", (flight_number, passenger_count, status))
    conn.commit()
    return cursor.lastrowid

def generate_passenger(conn, flight_id):
    """Generate a passenger check-in for a given flight."""
    name = fake.name()
    status = 'checkedin'
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {PASSENGER_TABLE_NAME} (name, flight_id, status) VALUES (%s, %s, %s)", (name, flight_id, status))
    conn.commit()
    return cursor.lastrowid

def get_checked_in_passenger_count(conn, flight_id):
    """Returns the number of passengers that have already checked in for a given flight."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {PASSENGER_TABLE_NAME} WHERE flight_id=%s AND status='checkedin'", (flight_id,))
    return cursor.fetchone()[0]

def generate_baggage(conn, passenger_id, flight_id):
    """Generate luggage details for a given passenger."""
    weight = random.gauss(AVG_BAGGAGE_WEIGHT, STEP_BAGGAGE_WEIGHT)
    weight = round(max(MIN_BAGGAGE_WEIGHT, min(weight, MAX_BAGGAGE_WEIGHT)),2)
    status = 'checkedin'
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {BAGGAGE_TABLE_NAME} (passenger_id, flight_id, weight, status) VALUES (%s, %s, %s, %s)", (passenger_id, flight_id, weight, status))
    conn.commit()
    return cursor.lastrowid

def check_start_boarding(conn, flight):
    # Decides whether to commence boarding between a min and max time after the flight has closed
    current_time = datetime.now()
    closed_at = flight['closed_at']
    elapsed_time = (current_time - closed_at).seconds
    if elapsed_time >= MAX_BOARDING_WAIT:
        start_boarding(conn, flight)
    elif elapsed_time > MIN_BOARDING_WAIT:
        # Start boarding now some of the time
        if random.randint(1, 100) <= START_BOARDING_PERCENTAGE:
            start_boarding(conn, flight)

def start_boarding(conn, flight):
    """Handles the start of the boarding process for a flight."""
    cursor = conn.cursor()

    # Update the flight status to 'boarding' and set the closed_at timestamp
    current_time = datetime.now()
    cursor.execute(f"UPDATE {FLIGHTS_TABLE_NAME} SET status='boarding', boarding_at=%s WHERE id=%s", (current_time, flight['id'],))
    conn.commit()

    logger.info(f"Flight {flight['id']} is now boarding.")

def check_conclude_boarding(conn, flight):
    """Checks if boarding should be concluded for a flight."""
    cursor = conn.cursor()

    # Fetch the boarding_at timestamp for the flight, which notes when boarding started
    cursor.execute(f"SELECT boarding_at FROM {FLIGHTS_TABLE_NAME} WHERE id=%s", (flight['id'],))
    boarding_opened_at = cursor.fetchone()[0]
    logger.info(f"Boarding opened at {boarding_opened_at}")

    # Calculate elapsed time since boarding started
    current_time = datetime.now()
    elapsed_time = (current_time - boarding_opened_at).seconds
    logger.info(f"Elapsed time since boarding opened: {elapsed_time} seconds")

    # Check the percentage of passengers that have boarded
    cursor.execute(f"SELECT COUNT(*) FROM {PASSENGER_TABLE_NAME} WHERE flight_id=%s AND status='onboarded'", (flight['id'],))
    boarded_count = cursor.fetchone()[0]
    logger.info(f"Number of passengers boarded: {boarded_count}")
    boarding_percentage = (boarded_count / flight['passenger_count']) * 100
    logger.info(f"Boarding percentage: {boarding_percentage}%")

    if boarding_percentage >= CLOSE_BOARDING_PERCENTAGE or elapsed_time >= MAX_BOARDING_WAIT:
        logger.info(f"Concluding boarding for Flight {flight['id']} at {boarding_percentage}% boarded in {elapsed_time} seconds.")
        conclude_boarding(conn, flight)

def conclude_boarding(conn, flight):
    """Handles the conclusion of the boarding process for a flight."""
    cursor = conn.cursor()

    # Conclude boarding by updating the flight status to 'boarded' and setting the boarded_at timestamp
    current_time = datetime.now()
    cursor.execute(f"UPDATE {FLIGHTS_TABLE_NAME} SET status='boarded', boarded_at=%s WHERE id=%s", (current_time, flight['id']))
    conn.commit()
    logger.info(f"Flight {flight['id']} has concluded boarding.")

def process_boarding(conn, flight):
    """Processes the boarding of passengers for a flight."""
    cursor = conn.cursor()

    # Select passengers that have checked in but not yet boarded
    cursor.execute(f"SELECT id FROM {PASSENGER_TABLE_NAME} WHERE flight_id=%s AND status='checkedin'", (flight['id'],))
    passengers = cursor.fetchall()

    # Shuffle the list of passengers
    random.shuffle(passengers)

    # Choose a subset of passengers to process for boarding
    boarding_batch_percentage = random.randint(1, MAX_BOARDING_BATCH)
    batch_size = int(boarding_batch_percentage / 100 * len(passengers))
    passengers_batch = passengers[:batch_size]

    for passenger in passengers_batch:
        if random.randint(1, 100) <= BOARDING_DENIED_PERCENTAGE:
            # This passenger won't board at this time
            continue

        # This passenger boards
        cursor.execute(f"UPDATE {PASSENGER_TABLE_NAME} SET status='onboarded' WHERE id=%s", (passenger[0],))
        logger.info(f"Passenger {passenger[0]} boarded Flight {flight['id']}.")

def conclude_checkin(conn, flight):
    cursor = conn.cursor()
    current_time = datetime.now()
    cursor.execute(f"UPDATE {FLIGHTS_TABLE_NAME} SET status='closed', closed_at=%s WHERE id=%s", (current_time, flight['id']))
    conn.commit()
    logger.info(f"Flight {flight['id']} is now closed for check-ins.")

def check_conclude_checkin(conn, flight):
    checked_in_count = get_checked_in_passenger_count(conn, flight['id'])
    percent_filled = (checked_in_count / flight['passenger_count']) * 100
    if percent_filled > CLOSE_FLIGHT_PERCENTAGE:
        conclude_checkin(conn, flight)

def process_checkin(conn, flight):
    # Get the number of already checked-in passengers
    checked_in_count = get_checked_in_passenger_count(conn, flight['id'])

    # Calculate the remaining seats
    remaining_seats = flight['passenger_count'] - checked_in_count

    # Generate a random number of passengers to check in, but not exceeding the remaining seats
    num_passengers_to_checkin = random.randint(1, remaining_seats)

    for _ in range(num_passengers_to_checkin):
        passenger_id = generate_passenger(conn, flight['id'])
        num_bags = round(random.gauss(AVG_BAGS_PER_PASSENGER, STEP_BAGS_PER_PASSENGER))
        num_bags = max(0, min(num_bags, MAX_BAGS_PER_PASSENGER)) 
        for _ in range(num_bags):
            bag_id = generate_baggage(conn, passenger_id, flight['id'])
            logger.info(f"Baggage {bag_id} checked in for Passenger {passenger_id} on Flight {flight['id']}.")
        logger.info(f"Passenger {passenger_id} completed check in for Flight {flight['id']}.")

def process_notboarded_passengers(conn, flight):
    """Handles passengers that didn't board the flight, and their luggage."""
    cursor = conn.cursor()

    # Step 1: Identify passengers who still have a status of 'checkedin' after the flight status has changed to 'boarded'
    cursor.execute(f"SELECT id FROM {PASSENGER_TABLE_NAME} WHERE flight_id=%s AND status='checkedin'", (flight['id'],))
    notboarded_passengers = cursor.fetchall()

    # Step 2: Change their status to 'notboarded'
    for passenger in notboarded_passengers:
        current_time = datetime.now()
        cursor.execute(f"UPDATE {PASSENGER_TABLE_NAME} SET status='notboarded', notboarded_at=%s WHERE id=%s", (current_time, passenger[0]))
        logger.info(f"Passenger {passenger[0]} did not board Flight {flight['id']}.")

        # Step 3: Identify the baggage associated with these passengers
        cursor.execute(f"SELECT id FROM {BAGGAGE_TABLE_NAME} WHERE passenger_id=%s AND flight_id=%s AND status='checkedin'", (passenger[0], flight['id']))
        baggage_items = cursor.fetchall()

        # Step 4: Change the baggage status to 'offloaded'
        for baggage in baggage_items:
            current_time = datetime.now()
            cursor.execute(f"UPDATE {BAGGAGE_TABLE_NAME} SET status='offloaded', offloaded_at=%s WHERE id=%s", (current_time, baggage[0]))
            logger.info(f"Baggage {baggage[0]} offloaded for Passenger {passenger[0]} from Flight {flight['id']}.")

    conn.commit()

def conclude_load_bags(conn, flight):
    """Handles the conclusion of the baggage loading process for a flight."""
    cursor = conn.cursor()

    # Find all passengers who completed boarding
    cursor.execute(f"SELECT id FROM {PASSENGER_TABLE_NAME} WHERE flight_id=%s AND status='onboarded'", (flight['id'],))
    onboarded_passengers = cursor.fetchall()

    for passenger in onboarded_passengers:
        # Update the status of each bag belonging to the onboarded passenger to 'loaded'
        current_time = datetime.now()
        cursor.execute(f"UPDATE {BAGGAGE_TABLE_NAME} SET status='loaded', loaded_at=%s WHERE passenger_id=%s AND flight_id=%s", (current_time, passenger[0], flight['id']))

    conn.commit()

    logger.info(f"Flight {flight['id']} has concluded loading bags.")

def check_departure(conn, flight):
    """Decides whether to commence departure for the flight."""
    current_time = datetime.now()
    elapsed_time = (current_time - flight['boarded_at']).seconds

    if elapsed_time >= MAX_DEPARTURE_WAIT:
        start_departure(conn, flight)
    elif elapsed_time > MIN_DEPARTURE_WAIT:
        # Decide to start departure based on the percentage
        if random.randint(1, 100) <= START_DEPARTURE_PERCENTAGE:
            start_departure(conn, flight)

def start_departure(conn, flight):
    """Marks the flight as departed."""
    cursor = conn.cursor()

    # Update the flight status to 'departed' and set the departed_at timestamp
    current_time = datetime.now()
    cursor.execute(f"UPDATE {FLIGHTS_TABLE_NAME} SET status='departed', departed_at=%s WHERE id=%s", (current_time, flight['id']))
    conn.commit()

    logger.info(f"Flight {flight['id']} has departed.")

def process_active_flights(conn):
    active_flights = get_active_flights(conn)
    remaining_slots = MAX_ACTIVE_FLIGHTS - len(active_flights)
    ratio = remaining_slots / MAX_ACTIVE_FLIGHTS
    adjusted_percentage = NEW_FLIGHT_PERCENTAGE * ratio
    if random.randint(1, 100) <= adjusted_percentage * 100:
        flight_id = generate_flight(conn)
        logger.info(f"Flight {flight_id} created.")

def generate_events(conn):
    while True:
        process_active_flights(conn)
        active_flights = get_active_flights(conn)
        for flight in active_flights:
            if flight['status'] == 'open':
                process_checkin(conn, flight)
                check_conclude_checkin(conn, flight)
            elif flight['status'] == 'closed':
                check_start_boarding(conn, flight)
            elif flight['status'] == 'boarding':
                process_boarding(conn, flight)
                check_conclude_boarding(conn, flight)
            elif flight['status'] == 'boarded':
                process_notboarded_passengers(conn, flight)
                conclude_load_bags(conn, flight)
                check_departure(conn, flight)
            elif flight['status'] == 'departed':
                pass
            elif flight['status'] == 'arrived':
                pass
            elif flight['status'] == 'offboarding':
                pass
            elif flight['status'] == 'completed':
                pass
            else:
                pass

@click.command()
@click.option('--remove-pipeline', is_flag=True, help='Reset the pipeline. Will Drop source table, remove debezium connector, drop the topic, and clean the Tinybird workspace')
@click.option('--create-pipeline', is_flag=True, help='Create the Pipeline. Will create the table, a few initial user events, create debezium connector and topic, and the Tinybird Confluent connection.')
def main(remove_pipeline, create_pipeline):
    project_kit_path = 'kits/airport'
    source_db = 'MYSQL'
    config.set_source_db(source_db)
    debezium_connector_name = config.MYSQL_CONFLUENT_CONNECTOR_NAME
    conn = db_functions.mysql_connect_db()
    config.set_kafka_topics(source_db, TABLES_TO_REPLICATE)
    table_include_list = ','.join([f"{config.MYSQL_DB_NAME}.{x}" for x in TABLES_TO_REPLICATE])

    if remove_pipeline:
        logger.info(f"Resetting the Tinybird pipeline from {source_db}...")
        cc_functions.connector_delete(name=debezium_connector_name, env_name=config.CONFLUENT_ENV_NAME, cluster_name=config.CONFLUENT_CLUSTER_NAME)
        cc_functions.k_topic_cleanup()
        db_functions.mysql_database_drop(conn)
        logger.info(f"Cleaning Tinybird workspace for {source_db}...")
        files_to_clean = utils.get_all_files_in_directory(project_kit_path)
        tb_functions.clean_workspace(files=files_to_clean, include_connector=False)
        logger.info("Pipeline Removed.")
    elif create_pipeline:
        for table_name, table_query in [
            (FLIGHTS_TABLE_NAME, FLIGHTS_TABLE_CREATE_QUERY),
            (PASSENGER_TABLE_NAME, PASSENGER_TABLE_CREATE_QUERY),
            (BAGGAGE_TABLE_NAME, BAGGAGE_TABLE_CREATE_QUERY)
        ]:
            db_functions.table_create(conn, table_name=table_name, query=table_query)
        
        cc_functions.connector_create(name=debezium_connector_name, source_db=source_db, env_name=config.CONFLUENT_ENV_NAME, cluster_name=config.CONFLUENT_CLUSTER_NAME,
        table_include_list=table_include_list                              )
        tb_functions.ensure_kafka_connection()
        files_to_upload = utils.get_all_files_in_directory(project_kit_path)
        tb_functions.update_datasource_info(files_to_upload)
        tb_functions.upload_def_for_db(files_to_upload)       
        time.sleep(3)  # Give it a few seconds to warm up
        generate_events(conn)
    else:
        try:
            generate_events(conn)
        except Exception as e:
            logger.info(f'Error: {e}')
        finally:
            if conn:
                conn.close()

if __name__ == '__main__':
    main()
