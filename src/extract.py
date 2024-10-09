import pandas as pd
from db_conexion import establish_connection, close_connection
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

def read_db():
    # Establish the connection
    connection = establish_connection()

    # SQL query to select data
    query = "SELECT * FROM grammy_awards"

    # Read the data into a pandas DataFrame
    try:
        # Use pandas to read directly from the connection
        grammy = pd.read_sql_query(query, con=connection)
        print("Data successfully loaded")
    except Exception as e:
        print(f"Error reading the database: {e}")
        grammy = pd.DataFrame()  # Return an empty DataFrame in case of an error

    # Close the connection
    close_connection(connection)
    
    return grammy  # Return the DataFrame

