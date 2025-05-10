import configparser
import os
import psycopg2

def read_db_config() -> dict:
    """
    Read database configuration parameters from a config file.

    Reads the 'database' section from 'config.ini' located one directory above
    this module and returns a dictionary of connection parameters.

    Returns:
        dict: Database connection parameters with keys:
            - dbname (str): Name of the database.
            - user (str): Username for authentication.
            - password (str): Password for authentication.
            - host (str): Database server host.
            - port (str): Database server port.
    """
    # Initialize a parser and read the configuration file
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
    config.read(config_path)

    # Extract parameters from the 'database' section
    db_config = config['database']

    return {
        'dbname':   db_config['dbname'],
        'user':     db_config['user'],
        'password': db_config['password'],
        'host':     db_config['host'],
        'port':     db_config['port'],
    }


def get_connection() -> tuple:
    """
    Establish a new connection to the PostgreSQL database.

    Utilizes configuration from read_db_config() to connect and returns
    both the connection object and a cursor for executing queries.

    Returns:
        tuple:
            - connection (psycopg2.extensions.connection): Active database connection.
            - cursor (psycopg2.extensions.cursor): Cursor for executing SQL statements.
    """
    # Load connection parameters
    config = read_db_config()

    # Create a new database connection
    connection = psycopg2.connect(
        dbname   = config['dbname'],
        user     = config['user'],
        password = config['password'],
        host     = config['host'],
        port     = config['port'],
    )

    # Obtain a cursor from the connection
    cursor = connection.cursor()

    return connection, cursor
