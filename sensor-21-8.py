import logging
import threading
import time
import usb.core
import socket
from queue import Queue, Empty
import sqlite3
import json
import requests
from flask import Flask, jsonify
from typing import Optional
import socket
import math
import numpy as np
import os

# Global variables
client_socket = None
unsent_data = []
data_queue = Queue()  # Queue for incoming data

VENDOR_ID = 0x2291
PRODUCT_ID = 0x0110
ENDPOINT_OUT = 0x01
ENDPOINT_IN = 0x81

SOCKET_HOST = '192.168.1.12'
SOCKET_PORT = 12345
FLASK_URL = 'http://192.168.1.12:5000/state'
LOG_FILE = 'device_reader.log'
PARAMS_FILE_PATH = 'last_parameters.json'

MAX_RETRIES = 5
TIMEOUT = 20000

app = Flask(__name__)
data_queue = Queue()
database_paths = {}
unsent_data = []

client_socket = None
sensor_state = False
connected = True
state_lock = threading.Lock()
THRESHOLD_DB_DEFAULT = 25

if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)
    logging.basicConfig(level=logging.DEBUG,
                       format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()])

def stop_acquisition(dev):
    """Stop data acquisition from the device."""
    try:
        logging.info("Attempting to stop acquisition...")
        dev.write(ENDPOINT_OUT, b'stop_acq\n')
        logging.info("Acquisition stop command sent.")
    except usb.core.USBError as e:
        logging.error(f"Error sending stop_acq command: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while stopping acquisition: {e}")

def save_unsent_data():
    """Save unsent data to a JSON file."""
    global unsent_data
    try:
        with open('unsent_data.txt', 'w') as f:
            json.dump(unsent_data, f)
            logging.info("Unsent data saved successfully.")
    except Exception as e:
        logging.error(f"Error saving unsent data: {e}")

def send_data(timestamped_data: tuple) -> bool:
    global connected, client_socket

    if client_socket is None or not client_socket.connected:
        client_socket = connect_to_gateway()
        if client_socket is None:
            logging.error("Failed to connect to gateway. Data will be queued.")
            return False

    timestamp, data = timestamped_data
    try:
        formatted_data = f"{data}\n"
        logging.info(f"{formatted_data}send")

        client_socket.sendall(formatted_data.encode('utf-8'))
        logging.debug(f"Data sent successfully: {formatted_data.strip()}")
        connected = True
        return True
    except (BrokenPipeError, socket.error) as e:
        logging.error(f"Failed to send data: {e}")
        connected = False
        client_socket = None  # Close the socket to initiate reconnection

        # Add data to unsent queue or persistent storage
        unsent_data.append(timestamped_data)
        save_unsent_data()

        return False  

def format_row_for_sending(processed_data: tuple) -> str:
    """Format processed data for sending."""
    timestamp, T, A, amplitude_db, rise_time, duration, counts, energy, rms, TRAI, flags = processed_data
    file_name = os.path.basename(database_paths['pridb'])
    channel = 1  # Assuming single channel, adjust if needed

    formatted_data = f"{file_name} {channel} {A} {counts} {duration} {rise_time} {rms} {energy} {timestamp} {amplitude_db}"
    return  formatted_data  # Using the default threshold, adjust if needed

def save_to_database(db_path: str, table_name: str, **kwargs) -> None:
    """Save data to the specified SQLite database table."""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Get the column names of the table
            cursor.execute(f"PRAGMA table_info({table_name})")
            table_columns = set(row[1] for row in cursor.fetchall())
            
            # Filter out kwargs that don't match table columns
            valid_kwargs = {k: v for k, v in kwargs.items() if k in table_columns}
            
            if not valid_kwargs:
                logging.warning(f"No valid columns to insert into {table_name}. Data: {kwargs}")
                return
            
            placeholders = ', '.join(['?'] * len(valid_kwargs))
            columns = ', '.join(valid_kwargs.keys())
            cursor.execute(f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})', list(valid_kwargs.values()))
            logging.debug(f"Saved data to {table_name}: {valid_kwargs}")
    except sqlite3.Error as e:
        logging.error(f"Error saving data to {table_name}: {e}")

def initialize_device(freq, threshold, dev, parameters) -> bool:
    """Initialize the USB device with specific settings."""
    decimation_value = {2000: 1, 1000: 2, 500: 4, 200: 8}.get(freq, 0)

    low_filter = parameters.get("low_filter", 0)
    high_filter = parameters.get("high_filter", 0)
    ddt = parameters.get("ddt", 0)
    pritrigger = parameters.get("pritrigger", 0)

    commands = [
        b'stop_acq\n',
        f'set_filter {low_filter} {high_filter} 4\n'.encode(),
        b'set_cont 0\n',
        f'set_thr {10 ** (threshold / 20):.2f}\n'.encode(),
        f'set_ddt {ddt}\n'.encode(),
        b'set_status_interval 1000.0\n',
        f'set_tr_enabled {1 if decimation_value > 0 else 0}\n'.encode(),
        f'set_tr_decimation {decimation_value}\n'.encode(),
        f'set_tr_pre_trig {pritrigger}\n'.encode(),
        b'set_tr_post_dur 64\n',
        b'set_cct_interval 0.00\n',
        b'start_acq\n',
    ]

    success = True
    try:
        for command in commands:
            for attempt in range(3):
                try:
                    dev.write(ENDPOINT_OUT, command)
                    time.sleep(0.1)
                    break  # Break out of the retry loop on success
                except usb.core.USBError as e:
                    if e.errno == 110:  # Timeout error
                        logging.warning(f"Timeout for command {command.strip()}. Retrying...")
                        time.sleep(1)
                    else:
                        logging.error(f"USBError occurred: {e}")
                        success = False
                        break  # Exit the retry loop on non-timeout errors
            else:  # This else corresponds to the for attempt loop
                logging.error(f"Failed to execute command {command.strip()} after 3 attempts")
                success = False

        if success:
            logging.info("Device initialized successfully.")
        return success
    except usb.core.USBError as e:
        logging.error(f"Error initializing device: {e}")
        return False

def process_chunk(chunk: dict, ae_db_path: str, tradb_path: str, threshold_microvolt: float) -> Optional[tuple]:
    try:
        logging.debug(f"Processing chunk: {chunk}")
        logging.debug(f"ae_db_path: {ae_db_path}, tradb_path: {tradb_path}, threshold_microvolt: {threshold_microvolt}")

        # Check if ae_db_path and tradb_path are set correctly
        if not ae_db_path or not tradb_path:
            logging.error("Database paths are not set properly.")
            return None

        timestamp = int(time.time() * 1000)

        for key, value in chunk.items():
            logging.debug(f"Processing key: {key}, value: {value}")
            if key.startswith('H '):
                parts = value.split()
                if len(parts) < 9:
                    logging.warning(f"Insufficient data in chunk: {parts}")
                    continue
                
                try:
                    # Extract values from parts
                    data = {part.split('=')[0]: part.split('=')[1] for part in parts[1:]}
                    T = int(data['T'])
                    A = int(data['A'])
                    R = int(data['R'])
                    D = int(data['D'])
                    C = int(data['C'])
                    E = float(data['E'])
                    TRAI = int(data['TRAI'])
                    flags = int(data['flags'])

                    logging.debug(f"Extracted values: T={T}, A={A}, R={R}, D={D}, C={C}, E={E}, TRAI={TRAI}, flags={flags}")

                    amplitude_db = 20 * math.log10(3.51 * A)
                    energy = E * (10**-4) * (3.51 ** 2) * 0.5
                    T *= 10**-6 / 2
                    rise_time = R * 10**-6 / 2
                    duration = D * 10**-6 / 2
                    counts = C
                    rms = (10**-6) * np.sqrt((1 / duration) * energy * (10**-6)) if duration > 0 else 0
                    
                    logging.debug(f"Calculated values: amplitude_db={amplitude_db}, energy={energy}, T={T}, rise_time={rise_time}, duration={duration}, counts={counts}, rms={rms}")

                    # Save to ae_db
                    save_to_database(ae_db_path, 'ae_data', 
                                     timestamp=timestamp, T=T, amplitude=A,
                                     amplitude_db=amplitude_db,
                                     rise_time=rise_time, duration=duration,
                                     counts=counts, energy=energy,
                                     rms=rms, TRAI=TRAI, flags=flags)

                    return (timestamp, T, A, amplitude_db, rise_time, duration, counts, energy, rms, TRAI, flags)

                except (ValueError, KeyError) as e:
                    logging.error(f"Error processing H data: {e}")
            
            elif key.startswith('TR '):
                tr_data = parse_tr_data(value.encode())
                if tr_data['T'] is not None:
                    save_to_database(tradb_path, 'tr_data',
                                     trai=tr_data['TRAI'],
                                     t=tr_data['T'],
                                     ns=tr_data['NS'],
                                     data=tr_data['DATA'])
                    logging.debug(f"Saved TR data: {tr_data}")

    except Exception as e:
        logging.error(f"Error in process_chunk: {e}")  
    return None

def db_to_microvolts(db: float) -> float:
    """Convert decibels to microvolts."""
    return 10 ** (db / 20)

def load_unsent_data():
    """Load unsent data from a JSON file."""
    global unsent_data
    unsent_data = []  # Initialize unsent_data to an empty list
    try:
        with open('unsent_data.txt', 'r') as f:
            unsent_data = json.load(f)
            logging.info(f"{unsent_data}")
            logging.info(f"{type(unsent_data)} type json")
            if unsent_data is None:
                unsent_data = []
                logging.info(f"{type(unsent_data)} type json")

            logging.info("Unsent data loaded successfully.")
    except FileNotFoundError:
        logging.warning("No unsent data file found. Starting with an empty list.")
    except Exception as e:
        logging.error(f"Error loading unsent data: {e}")

def save_last_parameters(parameters):
    """Save the last known parameters to a JSON file."""
    global PARAMS_FILE_PATH
    try:
        with open(PARAMS_FILE_PATH, 'w') as f:
            json.dump(parameters, f)
        logging.info("Last known parameters saved successfully.")
    except Exception as e:
        logging.error(f"Error saving last known parameters: {e}")

def get_parameters_from_flask(max_retries=5, retry_delay=5):
    """Get parameters from the Flask application with retry mechanism."""
    global last_known_parameters  # Access the global variable for last known parameters

    for attempt in range(max_retries):
        try:
            response = requests.get(FLASK_URL, timeout=10)
            if response.status_code == 200:
                data = response.json()
                logging.info(f"Received parameters: {data}")

                # Save the last known parameters
                save_last_parameters(data)
                last_known_parameters = data  # Update global variable
                return data
            else:
                logging.error(f"Failed to get parameters: {response.status_code} {response.text}")
        except requests.RequestException as e:
            logging.error(f"Error getting parameters (attempt {attempt + 1}/{max_retries}): {e}")

        if attempt < max_retries - 1:
            logging.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    # If all attempts fail, load last known parameters
    if last_known_parameters is not None:
        logging.info("Using last known parameters due to connection issues.")
        return last_known_parameters
    else:
        logging.error("Max retries exceeded. Unable to get parameters from Flask server and no last known parameters available.")
        return None
    
def read_data(dev, stop_event, data_queue, command) -> None:
    """Read data from the USB device and put it in the queue."""
    buffer = ""
    while not stop_event.is_set():
        for attempt in range(MAX_RETRIES):
            try:
                dev.write(ENDPOINT_OUT, command)
                raw_data = dev.read(ENDPOINT_IN, 512, TIMEOUT)
                if raw_data:
                    data_string = raw_data.tobytes().decode('utf-8', errors='ignore')
                    logging.debug(f"Raw data received: {data_string}")

                    buffer += data_string
                    lines = buffer.split('\n')
                    buffer = lines.pop()  # Keep the last incomplete line in the buffer

                    data_dict = {}
                    for line in lines:
                          if line.startswith('H T='):
                            data_dict['H T='] = line.strip()
                          elif line.startswith('H '):
                            data_dict['H'] = line[2:].strip()  # Remove 'H ' prefix
                          elif line.startswith('S temp='):
                               data_dict['S temp='] = line.strip()

                    if data_dict:
                        logging.debug(f"Parsed data: {data_dict}")
                        data_queue.put(data_dict)
                        logging.debug("data put")
                    else:
                        logging.warning("No valid data parsed from complete lines.")

                    break
                else:
                    logging.warning("No data received from USB device.")
            except usb.core.USBError as e:
                if e.errno == 110:
                    logging.warning("Timeout occurred, resetting device...")
                    dev.reset()
                    time.sleep(2)
                    continue
                else:
                    logging.error(f"USB error: {e}")
                    break
        else:
            logging.warning("Failed to read data after retries.")
        time.sleep(0.1)  # Shorter sleep to reduce latency

def resend_unsent_data() -> None:
    """Resend any unsent telemetry data."""
    global unsent_data  # Access the global list of unsent data

    # Iterate over a copy of the unsent_data list to avoid modifying it while iterating
    for entry in unsent_data[:]:
        timestamp, formatted_data = entry  # Unpack the entry tuple
        if send_data_without_ack((timestamp, formatted_data)):  # Attempt to send without waiting for ACK
            logging.info(f"Successfully resent queued data: {formatted_data}")
            unsent_data.remove(entry)  # Remove successfully sent entry from the list
        else:
            logging.warning(f"Failed to resend queued data: {formatted_data}")

def process_data_from_queue(stop_event: threading.Event) -> None:
    """Process data from the queue and send it to the gateway."""
    global unsent_data , client_socket
    database_paths = {
        'pridb': '/home/hp/najari/lib/python3.11/site-packages/test.pridb',
        'tradb': '/home/hp/najari/lib/python3.11/site-packages/test.tradb'
    }

    while not stop_event.is_set():  # Check if stop_event is set
        try:
            chunk = data_queue.get(timeout=1)  # Timeout to avoid blocking indefinitely
            logging.info(f"Chunk received for processing: {chunk}")

            # Attempt to get parameters from Flask
            parameters = get_parameters_from_flask()  # Fetch parameters from Flask
            if parameters is None:
                parameters = load_last_parameters()  # Load last known parameters if Flask is down

            if parameters and parameters.get('sensor_state', False):
                threshold_db = parameters.get('threshold', THRESHOLD_DB_DEFAULT)
                threshold_microvolt = db_to_microvolts(threshold_db)

                if isinstance(chunk, dict):
                    logging.info("Processing chunk...")
                    processed_data = process_chunk(chunk, database_paths['pridb'], database_paths['tradb'], threshold_microvolt)

                    if processed_data is None:
                        logging.warning("process_chunk returned None. Skipping this chunk.")
                        continue  # Skip to the next iteration

                    timestamp = int(time.time() * 1000)  # Get current timestamp
                    formatted_data = format_row_for_sending(processed_data)

                    if not send_data((timestamp, formatted_data)):
                        unsent_data.append((timestamp, formatted_data))  # Buffer unsent data
                        save_unsent_data()  # Save updated list of unsent data
                        logging.warning(f"Failed to send data. Added to unsent data: {formatted_data}")
                        if not connected:
                            logging.info(f"Old client socket {client_socket}")
                            client_socket = connect_to_gateway()  # Attempt to reconnect
                            logging.info(f"New client socket {client_socket}")
                else:
                    logging.error(f"Unexpected chunk type: {type(chunk)}")
            else:
                logging.warning("Sensor is not active. Skipping processing.")

            # Resend any unsent data after processing
            resend_unsent_data()

        except Empty:
            logging.debug("No data available in queue.")
            continue  # If no new chunk is available, continue the loop

        except Exception as e:
            logging.error(f"Error processing chunk: {e}")
            continue  # Continue to the next iteration

def monitor_flask_parameters(stop_event):
    """Monitor parameters from the Flask application and update device state."""
    global sensor_state, database_paths
    last_known_parameters = load_last_parameters()  # Load initial parameters

    # Initialize database paths only once
    initialized_database_paths = False

    while not stop_event.is_set():
        try:
            # Attempt to get parameters from Flask without blocking
            parameters = get_parameters_from_flask()
            if parameters:
                save_last_parameters(parameters)
                last_known_parameters = parameters

                # Set database paths based on received parameters only once
                if not initialized_database_paths:
                    file_name = parameters.get('filename', 'data')
                    database_paths['pridb'] = f'/home/hp/najari/lib/python3.11/site-packages/{file_name}.pridb'
                    database_paths['tradb'] = f'/home/hp/najari/lib/python3.11/site-packages/{file_name}.tradb'
                    logging.info(f"Setting database paths: pridb={database_paths['pridb']}, tradb={database_paths['tradb']}")
                    initialized_database_paths = True  # Mark as initialized

            elif last_known_parameters:
                logging.warning("Using last known parameters due to connection issues.")
                parameters = last_known_parameters
            else:
                logging.error("No parameters available. Skipping device update.")
                time.sleep(5)  # Wait before retrying
                continue

            # Update device state based on received parameters
            with state_lock:
                new_sensor_state = parameters.get('sensor_state', sensor_state)
                if new_sensor_state != sensor_state:
                    sensor_state = new_sensor_state
                    logging.info(f"Updated sensor state: {sensor_state}")
                    if sensor_state:
                        #activate_sensor_for_duration(dev, duration=300)
                        setup_database(database_paths['pridb'], database_paths['tradb'])
                        if initialize_device(parameters.get('freq', 1000),
                                             parameters.get('threshold', THRESHOLD_DB_DEFAULT),
                                             dev,
                                             parameters):
                            logging.info("Device initialized successfully.")

        except Exception as e:
            logging.error(f"Error monitoring Flask parameters: {e}")
            time.sleep(5)  # Wait before retrying

def load_last_parameters():
    """Load the last known parameters from a JSON file."""
    global last_known_parameters, PARAMS_FILE_PATH
    try:
        with open(PARAMS_FILE_PATH, 'r') as f:
            last_known_parameters = json.load(f)
        logging.info("Last known parameters loaded successfully.")
    except FileNotFoundError:
        logging.warning("No previous parameter file found. Starting with empty parameters.")
        last_known_parameters = None
    except Exception as e:
        logging.error(f"Error loading last known parameters: {e}")

def connect_to_gateway(max_retries=5, retry_delay=5):
    global client_socket

    for attempt in range(max_retries):
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(10)  # Set a timeout for the connection attempt
            client_socket.connect((SOCKET_HOST, SOCKET_PORT))
            logging.info("Connected to the gateway.")
            return client_socket
        except (socket.timeout, socket.error, ConnectionRefusedError) as e:
            logging.warning(f"Connection to gateway failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    logging.error("Failed to connect to gateway after multiple attempts.")
    return None
    
def detach_kernel_driver(dev) -> None:
    """Detach the kernel driver from the USB device if active."""
    try:
        if dev.is_kernel_driver_active(0):
            dev.detach_kernel_driver(0)
        usb.util.dispose_resources(dev)
        logging.info("Kernel driver detached from the USB device.")
    except Exception as e:
        logging.error(f"Error detaching kernel driver: {e}")

def setup_database(ae_db_path: str, tradb_path: str) -> None:
    """Setup the SQLite databases for storing data."""
    try:
        with sqlite3.connect(ae_db_path) as ae_conn, sqlite3.connect(tradb_path) as tr_conn:
            ae_conn.execute("DROP TABLE IF EXISTS ae_data;")
            ae_conn.execute('''CREATE TABLE IF NOT EXISTS ae_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER,
                T REAL,
                amplitude REAL,
                amplitude_db REAL,
                rise_time REAL,
                duration REAL,
                counts INTEGER,
                energy REAL,
                rms REAL,
                TRAI INTEGER,
                flags INTEGER
            )''')
            tr_conn.execute("DROP TABLE IF EXISTS tr_data;")
            tr_conn.execute('''CREATE TABLE IF NOT EXISTS tr_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trai INTEGER,
                t REAL,
                ns INTEGER,
                data TEXT
            )''')
        logging.info(f"Databases setup complete. AE: {ae_db_path}, TR: {tradb_path}")
    except sqlite3.Error as e:
        logging.error(f"SQLite error during database setup: {e}")

def main():
    """Main function to initialize everything and start threads."""
    global client_socket

    database_paths = {
        'pridb': '/home/hp/najari/lib/python3.11/site-packages/test.pridb',
        'tradb': '/home/hp/najari/lib/python3.11/site-packages/test.tradb'
    }

    setup_database(database_paths['pridb'], database_paths['tradb'])

    dev = usb.core.find(idVendor=VENDOR_ID, idProduct=PRODUCT_ID)
    if dev is None:
        raise ValueError("Device not found.")

    detach_kernel_driver(dev)
    dev.set_configuration()
    logging.info("USB device initialized.")

    stop_event = threading.Event()  # Create a stop event

    load_last_parameters()  # Load any previously saved parameters
    
    client_socket = connect_to_gateway()

    try:
        # Start threads with stop_event passed as an argument
        flask_monitor_thread = threading.Thread(target=monitor_flask_parameters, args=(stop_event,))
        flask_app_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000))
        data_processing_thread = threading.Thread(target=process_data_from_queue, args=(stop_event,))
        read_data_thread = threading.Thread(target=read_data, args=(dev, stop_event, data_queue, b'get_ae_data\n'))

        threads = [flask_monitor_thread, flask_app_thread, read_data_thread, data_processing_thread]

        for thread in threads:
            thread.daemon = True  # Allow threads to exit when the main program exits
            thread.start()

       # start_sending_unsent_data()  # Start sending unsent data
        
        while True:
            time.sleep(5)  # Main loop can handle other tasks or checks

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Shutting down...")
    
    finally:
        stop_event.set()  # Signal all threads to stop
        for thread in threads:
            thread.join(timeout=5)  # Give threads time to finish
        if client_socket:
            client_socket.close()
        logging.info("Main function terminated gracefully.")