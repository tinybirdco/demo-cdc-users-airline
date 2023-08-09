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
        status ENUM('idle', 'checkedin', 'onboarded', 'notboarded', 'departed', 'arrived', 'completed') NOT NULL,
        checkedin_at DATETIME,
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
        flight_id INT,
        weight DOUBLE NOT NULL,
        status ENUM('idle', 'checkedin', 'loaded', 'arrived', 'offloaded', 'missing', 'completed') NOT NULL,
        checkedin_at DATETIME,
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
MAX_ACTIVE_FLIGHTS = 100
MIN_MISSED_FLIGHTS = 2
MAX_MISSED_FLIGHTS = 7
NEW_FLIGHT_PERCENTAGE = 50
MIN_PASSENGER_COUNT = 50
MAX_PASSENGER_COUNT = 320
TARGET_PASSENGER_POOL = 5 * MAX_PASSENGER_COUNT
MIN_BAGGAGE_WEIGHT = 5.0
AVG_BAGGAGE_WEIGHT = 15.0
STEP_BAGGAGE_WEIGHT = 3.0
MAX_BAGGAGE_WEIGHT = 32.0
AVG_BAGS_PER_PASSENGER = 1.3
STEP_BAGS_PER_PASSENGER = 0.5
MAX_BAGS_PER_PASSENGER = 5
CLOSE_FLIGHT_PERCENTAGE = 95
MIN_BOARDING_WAIT = 20
START_BOARDING_PERCENTAGE = 20
MAX_BOARDING_WAIT = 60
MAX_CHECKIN_BATCH = .2 * MAX_PASSENGER_COUNT
MAX_BOARDING_BATCH = .15 * MAX_PASSENGER_COUNT
CLOSE_BOARDING_PERCENTAGE = 98
BOARDING_DENIED_PERCENTAGE = 2
START_DEPARTURE_PERCENTAGE = 20
MIN_DEPARTURE_WAIT = 20
MAX_DEPARTURE_WAIT = 60

def get_active_flights(conn):
    with conn.cursor() as cursor:    
        cursor.execute(f"SELECT * FROM {FLIGHTS_TABLE_NAME} WHERE status NOT IN ('completed', 'departed')")
        resp = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        flights = [dict(zip(column_names, row)) for row in resp]
    return flights

def generate_flight(conn):
    logger.info("Attempting to generate a new flight...")
    flight_number = str(fake.bothify(text='??###')).upper()
    passenger_count = random.randint(MIN_PASSENGER_COUNT, MAX_PASSENGER_COUNT)
    status = 'open'
    with conn.cursor() as cursor:
        cursor.execute(f"INSERT INTO {FLIGHTS_TABLE_NAME} (flight_number, passenger_count, status) VALUES (%s, %s, %s)", (flight_number, passenger_count, status))
        conn.commit()
    return cursor.lastrowid

def get_checked_in_passenger_count(conn, flight_id):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {PASSENGER_TABLE_NAME} WHERE flight_id=%s AND status='checkedin'", (flight_id,))
        return cursor.fetchone()[0]

def check_start_boarding(conn, flight):
    # Decides whether to commence boarding between a min and max time after the flight has closed
    current_time = datetime.utcnow()
    closed_at = flight['closed_at']
    elapsed_time = (current_time - closed_at).seconds
    if elapsed_time >= MAX_BOARDING_WAIT:
        start_boarding(conn, flight)
    elif elapsed_time > MIN_BOARDING_WAIT:
        # Start boarding now some of the time
        if random.randint(1, 100) <= START_BOARDING_PERCENTAGE:
            start_boarding(conn, flight)

def start_boarding(conn, flight):
    current_time = datetime.utcnow()
    with conn.cursor() as cursor:
        cursor.execute(f"UPDATE {FLIGHTS_TABLE_NAME} SET status='boarding', boarding_at=%s WHERE id=%s", (current_time, flight['id'],))
    conn.commit()
    logger.info(f"Flight {flight['id']} is now boarding.")

def check_conclude_boarding(conn, flight):
    """Checks if boarding should be concluded for a flight."""
    cursor = conn.cursor()

    # Fetch the boarding_at timestamp for the flight, which notes when boarding started
    cursor.execute(f"SELECT boarding_at FROM {FLIGHTS_TABLE_NAME} WHERE id=%s", (flight['id'],))
    boarding_opened_at = cursor.fetchone()[0]

    # Calculate elapsed time since boarding started
    current_time = datetime.utcnow()
    elapsed_time = (current_time - boarding_opened_at).seconds
    logger.info(f"Elapsed time since Flight {flight['id']} started boarding: {elapsed_time} seconds")

    # Check the percentage of passengers that have boarded
    cursor.execute(f"SELECT COUNT(*) FROM {PASSENGER_TABLE_NAME} WHERE flight_id=%s AND status='onboarded'", (flight['id'],))
    boarded_count = cursor.fetchone()[0]
    boarding_percentage = round((boarded_count / flight['passenger_count']) * 100, 2)
    logger.info(f"Boarding completion: {boarding_percentage}% of {flight['passenger_count']} passengers.")

    if boarding_percentage >= CLOSE_BOARDING_PERCENTAGE or elapsed_time >= MAX_BOARDING_WAIT:
        logger.info(f"Concluding boarding for Flight {flight['id']} at {boarding_percentage}% boarded in {elapsed_time} seconds.")
        conclude_boarding(conn, flight)

def conclude_boarding(conn, flight):
    current_time = datetime.utcnow()
    with conn.cursor() as cursor:
        cursor.execute(f"UPDATE {FLIGHTS_TABLE_NAME} SET status='boarded', boarded_at=%s WHERE id=%s", (current_time, flight['id']))
    conn.commit()
    logger.info(f"Flight {flight['id']} has concluded boarding.")

def process_boarding(conn, flight):
    """Processes the boarding of passengers for a flight."""
    cursor = conn.cursor()

    # Select passengers that have checked in but not yet boarded
    cursor.execute(f"SELECT id FROM {PASSENGER_TABLE_NAME} WHERE flight_id=%s AND status='checkedin'", (flight['id'],))
    passengers = [row[0] for row in cursor.fetchall()]

    # Shuffle the list of passengers
    random.shuffle(passengers)

    # Choose a subset of passengers to process for boarding
    boarding_batch_percentage = random.randint(1, MAX_BOARDING_BATCH)
    batch_size = int(boarding_batch_percentage / 100 * len(passengers))
    passengers_batch = passengers[:batch_size]

    # Filter passengers based on BOARDING_DENIED_PERCENTAGE
    passengers_to_board = [p for p in passengers_batch if random.randint(1, 100) > BOARDING_DENIED_PERCENTAGE]

    # Update the status of all chosen passengers in a single batch
    if passengers_to_board:
        placeholders = ', '.join(['%s'] * len(passengers_to_board))
        current_time = datetime.utcnow()
        cursor.execute(f"UPDATE {PASSENGER_TABLE_NAME} SET status='onboarded', onboarded_at=%s WHERE id IN ({placeholders})", [current_time] + passengers_to_board)
        conn.commit()
        logger.info(f"{len(passengers_to_board)} passengers boarded Flight {flight['id']} in this batch.")


def conclude_checkin(conn, flight):
    current_time = datetime.utcnow()
    with conn.cursor() as cursor:
        cursor.execute(f"UPDATE {FLIGHTS_TABLE_NAME} SET status='closed', closed_at=%s WHERE id=%s", (current_time, flight['id']))
    conn.commit()
    logger.info(f"Flight {flight['id']} is now closed for check-ins.")

def check_conclude_checkin(conn, flight):
    checked_in_count = get_checked_in_passenger_count(conn, flight['id'])
    percent_filled = round((checked_in_count / flight['passenger_count']) * 100, 2)
    if percent_filled > CLOSE_FLIGHT_PERCENTAGE:
        logger.info(f"Flight {flight['id']} is {percent_filled}% full. Concluding check-in.")
        conclude_checkin(conn, flight)
    else:
        logger.info(f"Flight {flight['id']} is {percent_filled}% full. Continuing check-in.")

def process_checkin(conn, flight):
    timer_start = time.time()
    checked_in_count = get_checked_in_passenger_count(conn, flight['id'])
    remaining_seats = flight['passenger_count'] - checked_in_count
    logger.info(f"Flight {flight['id']} is {flight['status']} with {remaining_seats} remaining seats.")
    num_passengers_to_checkin = random.randint(1, remaining_seats)
    checkin_batch = int(min(num_passengers_to_checkin, MAX_CHECKIN_BATCH))

    with conn.cursor() as cursor:
        # Select 'idle' passengers and limit them to the number we want to check-in
        cursor.execute(f"SELECT id FROM {PASSENGER_TABLE_NAME} WHERE status='idle' LIMIT %s", (checkin_batch,))
        passenger_ids = [row[0] for row in cursor.fetchall()]

        if passenger_ids:
            current_time = datetime.utcnow()
            # Update passenger status to 'checkedin' and associate them with the flight using batch operation
            passenger_ids_str = ",".join(map(str, passenger_ids))
            cursor.execute(f"UPDATE {PASSENGER_TABLE_NAME} SET status='checkedin', flight_id=%s, checkedin_at=%s WHERE id IN ({passenger_ids_str})", (flight['id'], current_time))

            # Update the status of the baggage to 'checkedin' and associate them with the flight using batch operation
            cursor.execute(f"UPDATE {BAGGAGE_TABLE_NAME} SET status='checkedin', flight_id=%s, checkedin_at=%s WHERE passenger_id IN ({passenger_ids_str})", (flight['id'], current_time))

    conn.commit()
    timer_end = time.time()
    time_delta = timer_end - timer_start
    logger.info(f"Checked in {len(passenger_ids)} passengers for Flight {flight['id']} in {round(time_delta,1)} seconds.")

def process_notboarded_passengers(conn, flight):
    """Handles passengers that didn't board the flight, and their luggage."""
    cursor = conn.cursor()

    # Step 1: Identify passengers who still have a status of 'checkedin' after the flight status has changed to 'boarded'
    cursor.execute(f"SELECT id FROM {PASSENGER_TABLE_NAME} WHERE flight_id=%s AND status='checkedin'", (flight['id'],))
    notboarded_passengers = [row[0] for row in cursor.fetchall()]

    # Step 2: Change their status to 'notboarded' in a batch
    current_time = datetime.utcnow()
    if notboarded_passengers:
        placeholders = ', '.join(['%s'] * len(notboarded_passengers))
        cursor.execute(f"UPDATE {PASSENGER_TABLE_NAME} SET status='notboarded', notboarded_at=%s WHERE id IN ({placeholders})", [current_time] + notboarded_passengers)
        logger.info(f"{len(notboarded_passengers)} passengers did not board Flight {flight['id']}.")

        # Step 3: Identify the baggage associated with these passengers in one query
        cursor.execute(f"SELECT id FROM {BAGGAGE_TABLE_NAME} WHERE passenger_id IN ({placeholders}) AND flight_id=%s AND status='checkedin'", notboarded_passengers + [flight['id']])
        baggage_items = [row[0] for row in cursor.fetchall()]

        # Step 4: Change the baggage status to 'offloaded' in a batch
        if baggage_items:
            baggage_placeholders = ', '.join(['%s'] * len(baggage_items))
            cursor.execute(f"UPDATE {BAGGAGE_TABLE_NAME} SET status='offloaded', offloaded_at=%s WHERE id IN ({baggage_placeholders})", [current_time] + baggage_items)
            logger.info(f"{len(baggage_items)} baggage items offloaded from Flight {flight['id']}.")

    conn.commit()

def conclude_load_bags(conn, flight):
    """Handles the conclusion of the baggage loading process for a flight."""
    timer_start = time.time()

    # Update the status of bags belonging to onboarded passengers of the given flight to 'loaded'
    current_time = datetime.utcnow()
    with conn.cursor() as cursor:
        cursor.execute(f"""
            UPDATE {BAGGAGE_TABLE_NAME} 
            SET status='loaded', loaded_at=%s 
            WHERE flight_id=%s 
            AND passenger_id IN (SELECT id FROM {PASSENGER_TABLE_NAME} WHERE flight_id=%s AND status='onboarded')
        """, (current_time, flight['id'], flight['id']))

        conn.commit()
    timer_end = time.time()
    time_delta = timer_end - timer_start

    logger.info(f"Flight {flight['id']} has concluded loading bags in {round(time_delta, 2)} seconds.")

def check_departure(conn, flight):
    """Decides whether to commence departure for the flight."""
    current_time = datetime.utcnow()
    elapsed_time = (current_time - flight['boarded_at']).seconds

    if elapsed_time >= MAX_DEPARTURE_WAIT:
        logger.info(f"Flight {flight['id']} has waited {elapsed_time} seconds, exceeding maximum. Commencing Departure.")
        start_departure(conn, flight)
    elif elapsed_time > MIN_DEPARTURE_WAIT:
        # Decide to start departure based on the percentage
        if random.randint(1, 100) <= START_DEPARTURE_PERCENTAGE:
            logger.info(f"Flight {flight['id']} has waited {elapsed_time} seconds, passing minimum. Commencing Departure.")
            start_departure(conn, flight)

def start_departure(conn, flight):
    """Marks the flight as departed."""
    process_notboarded_passengers(conn, flight)
    conclude_load_bags(conn, flight)
    cursor = conn.cursor()

    # Update the flight status to 'departed' and set the departed_at timestamp
    current_time = datetime.utcnow()
    cursor.execute(f"UPDATE {FLIGHTS_TABLE_NAME} SET status='departed', departed_at=%s WHERE id=%s", (current_time, flight['id']))
    conn.commit()

    logger.info(f"Flight {flight['id']} has departed.")

def process_active_flights(conn):
    active_flights = get_active_flights(conn)
    logger.info(f"There are currently {len(active_flights)} of {MAX_ACTIVE_FLIGHTS} active flights.")
    
    data = tb_functions.endpoint_fetch('flights_missed_pct_minute.json')['data']
    sorted_data = sorted(data, key=lambda x: x['time_interval'], reverse=True)
    miss_rate_last_interval = sorted_data[0]['flights_missed_pct']
    
    create_new_flight = False
    if miss_rate_last_interval < MIN_MISSED_FLIGHTS:
        logger.info(f"Miss rate is {miss_rate_last_interval}%, which is under the minimum target of {MIN_MISSED_FLIGHTS}%. Creating a new flight.")
        create_new_flight = True
    elif MIN_MISSED_FLIGHTS <= miss_rate_last_interval <= MAX_MISSED_FLIGHTS:
        logger.info(f"Miss rate is {miss_rate_last_interval}%, which is in the mid-range. {NEW_FLIGHT_PERCENTAGE}% chance to create a new flight.")
        create_new_flight = random.randint(1, 100) <= NEW_FLIGHT_PERCENTAGE
    else:
        logger.info(f"Miss rate is {miss_rate_last_interval}%, which is over the maximum target of {MAX_MISSED_FLIGHTS}%. Not creating a new flight.")
    
    if create_new_flight:
        flight_id = generate_flight(conn)
        logger.info(f"Flight {flight_id} created.")
    else:
        logger.info("No new flight created.")

def process_passenger_pool(conn):
    with conn.cursor() as cursor:
        
        cursor.execute(f"SELECT COUNT(*) FROM {PASSENGER_TABLE_NAME} WHERE status='idle'")
        idle_count = cursor.fetchone()[0]

        passengers_to_generate = max(0, TARGET_PASSENGER_POOL - idle_count)
        logger.info(f"Passenger pool has {idle_count} idle passengers. Requesting {passengers_to_generate} new idle passengers.")
        generate_passengers(conn, passengers_to_generate)

def generate_passengers(conn, passengers_to_generate):
    """Generate new passengers with an 'idle' status and their associated luggage."""
    with conn.cursor() as cursor:
        passenger_values = []
        baggage_values = []
        bag_counts = []  # List to store the number of bags for each passenger

        for _ in range(passengers_to_generate):
            name = fake.name()
            passenger_values.append((name, 'idle'))

            num_bags = round(random.gauss(AVG_BAGS_PER_PASSENGER, STEP_BAGS_PER_PASSENGER))
            num_bags = max(0, min(num_bags, MAX_BAGS_PER_PASSENGER))
            bag_counts.append(num_bags)

            for _ in range(num_bags):
                weight = random.gauss(AVG_BAGGAGE_WEIGHT, STEP_BAGGAGE_WEIGHT)
                weight = round(max(MIN_BAGGAGE_WEIGHT, min(weight, MAX_BAGGAGE_WEIGHT)), 2)
                baggage_values.append((None, weight, 'idle'))

        # Batch insert passengers
        cursor.executemany(f"INSERT INTO {PASSENGER_TABLE_NAME} (name, status) VALUES (%s, %s)", passenger_values)

        # Get the starting id of the inserted passengers
        start_id = cursor.lastrowid

        # Update passenger_id placeholders in baggage_values
        current_passenger_id = start_id
        current_bag_count = 0
        passenger_index = 0  # Index to track the current passenger in the bag_counts list

        for i, baggage_tuple in enumerate(baggage_values):
            baggage_values[i] = (current_passenger_id,) + baggage_tuple[1:]
            
            current_bag_count += 1
            if current_bag_count >= bag_counts[passenger_index]:
                current_passenger_id += 1
                passenger_index += 1
                current_bag_count = 0

        # Batch insert luggage
        cursor.executemany(f"INSERT INTO {BAGGAGE_TABLE_NAME} (passenger_id, weight, status) VALUES (%s, %s, %s)", baggage_values)

    conn.commit()
    logger.info(f"Generated {passengers_to_generate} new passengers with {len(baggage_values)} pieces of luggage.")


def generate_events(conn):
    logger.info("Starting the event generation loop...")
    while True:
        loop_start_time = time.time()
        process_passenger_pool(conn)
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
        loop_end_time = time.time()
        loop_time_delta = loop_end_time - loop_start_time
        logger.info(f"Event generation loop completed in {round(loop_time_delta, 1)} seconds.")

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
