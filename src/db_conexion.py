import psycopg2
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def establecer_conexion():
    # Obtener las credenciales desde las variables de entorno
    db = "WorkShop_02"
    usuario = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    host = os.getenv('DB_HOST')
    puerto = 5432  # Puerto por defecto de PostgreSQL

    # Crear una conexión a la base de datos
    connection = psycopg2.connect(
        dbname=db,
        user=usuario,
        password=password,
        host=host,
        port=puerto
    )
    
    print("Conexión exitosa a la base de datos")
    return connection  # Devolver la conexión

def cerrar_conexion(connection):
    connection.close()
    print("Conexión cerrada a la base de datos")

# Establecer la conexión
connection = establecer_conexion()

# Cerrar la conexión al finalizar
cerrar_conexion(connection)
