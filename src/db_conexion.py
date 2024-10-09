import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

def establish_connection():
    # Get credentials from environment variables
    db = "WorkShop_02"
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    host = os.getenv('DB_HOST')
    port = 5432  # Default PostgreSQL port

    # Create a connection to the database
    connection = psycopg2.connect(
        dbname=db,
        user=user,
        password=password,
        host=host,
        port=port
    )
    
    print("Successfully connected to the database")
    return connection  # Return the connection

def close_connection(connection):
    connection.close()
    print("Database connection closed")

# Establish the connection
connection = establish_connection()

# Close the connection when done
close_connection(connection)
