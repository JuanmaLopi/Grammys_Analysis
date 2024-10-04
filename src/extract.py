import pandas as pd
from db_conexion import establecer_conexion, cerrar_conexion
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def read_db():
    # Establecer la conexión
    connection = establecer_conexion()

    # Consulta SQL para seleccionar los datos
    query = "SELECT * FROM grammy_awards"

    # Leer los datos en un DataFrame de pandas
    try:
        # Usar pandas para leer directamente desde la conexión
        grammy = pd.read_sql_query(query, con=connection)
        print("Datos cargados con éxito")
    except Exception as e:
        print(f"Error al leer la base de datos: {e}")
        grammy = pd.DataFrame()  # Retornar un DataFrame vacío en caso de error

    # Cerrar la conexión
    cerrar_conexion(connection)
    
    return grammy  # Retornar el DataFrame
