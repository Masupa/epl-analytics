import os
import psycopg2
from dotenv import load_dotenv

# Take env variables from .env
load_dotenv()

def connect():
    """Connect to Postgres server, 
    and then return connection
    
    Args:
    -----
        None

    Returns:
    --------
        conn :
    """

    try:
        # Connect to DB
        conn = psycopg2.connect(
            host="localhost",
            database="analytics_db",
            user="user",
            password="root",
            port="5432"
            )
    except psycopg2.OperationalError as ex:
        # TODO: Handle this exception by asking the user to enter the right details
        print(ex)
        return "error", ex
    except Exception as ex:
        print(f"{ex.args}")
        return "error", ex
    else:
        # create a cursor
        return "okay", conn
