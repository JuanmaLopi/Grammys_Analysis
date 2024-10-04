from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()



def establecer_conexion():
    # Obtener las credenciales desde las variables de entorno
    db = "WorkShop_02"
    usuario = "postgres"
    password = "root"
    host ="localhost"
    puerto = 5432
    # Crear la cadena de conexión para SQLAlchemy
    connection_string = f'postgresql+psycopg2://{usuario}:{password}@{host}:{puerto}/{db}'
    engine = create_engine(connection_string)
    
    # Crear una sesión (opcional, si necesitas trabajar con ORM más adelante)
    Session = sessionmaker(bind=engine)
    session = Session()

    print("Conexión exitosa a la base de datos")
    return engine, session  # Devolver tanto el engine como la session si es necesario

def cerrar_conexion(session):
    session.close()
    print("Conexión cerrada a la base de datos")

# Establecer la conexión
engine, session = establecer_conexion()

# Ahora puedes usar `engine` con pandas sin problema