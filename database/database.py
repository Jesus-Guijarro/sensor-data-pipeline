import configparser
import os
import psycopg2

def read_db_config():
    """
    Reads database configuration parameters from config.ini file
    """
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
    config.read(config_path)

    # Extract database configuration parameters
    db_config = config['database']

    return {
        'dbname': db_config['dbname'],
        'user': db_config['user'],
        'password': db_config['password'],
        'host': db_config['host'],
        'port': db_config['port']
    }

def get_connection():
    """
    Establishes a connection to the PostgreSQL database
    """
    config = read_db_config()

    connection = psycopg2.connect(
        dbname=config['dbname'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port']
    )

    cursor = connection.cursor()

    return connection, cursor