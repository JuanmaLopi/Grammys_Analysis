import pandas as pd
from db_conexion import establecer_conexion, cerrar_conexion
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def read_db():
    # Establece la conexión usando SQLAlchemy
    engine, session = establecer_conexion()

    # Consulta SQL para seleccionar los datos
    query = "SELECT * FROM grammy_awards"

    # Leer los datos en un DataFrame de pandas
    grammy = pd.read_sql_query(query, con=engine)

    # Cierra la sesión y la conexión
    cerrar_conexion(session)
    print("Datos cargados con éxito")
    return grammy  # Retornar el DataFrame


if __name__ == "__main__":
    read_db()