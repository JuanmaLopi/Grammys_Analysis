def upload_to_postgres():
    import pandas as pd
    from sqlalchemy import create_engine
    import os
    from dotenv import load_dotenv

    load_dotenv()

    # Step 1: Read the CSV
    df = pd.read_csv(os.path.join('data', 'Grammy_And_Spotify_Merged.csv'))  # Read the CSV into a DataFrame

    # Step 2: Set up the connection to PostgreSQL
    usuario = "postgres"  # Replace with your PostgreSQL user
    password = "root"  # Replace with your password
    host = "localhost"  # Replace if your database is on another server
    puerto = 5432  # Default PostgreSQL port
    db = "WorkShop_02"  # Replace with your database name

    # Create the connection string
    engine = create_engine(f'postgresql://{usuario}:{password}@{host}:{puerto}/{db}')

    # Step 3: Upload the data from the CSV to a PostgreSQL table
    # You can specify the table name and whether to replace or append the data
    nombre_tabla = "merged"  # Replace with your table name
    df.to_sql(nombre_tabla, engine, if_exists='replace', index=False)

    print(f'Data uploaded to table {nombre_tabla} successfully')

if __name__ == "__main__":
    upload_to_postgres()


