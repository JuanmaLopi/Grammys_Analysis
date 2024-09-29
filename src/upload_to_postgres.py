import pandas as pd
from sqlalchemy import create_engine
from Credentials import usuario,password,host,puerto,db

# Paso 1: Lee el CSV
csv_file = 'D:/QuintoSemestre/ETL/Grammy_Analysis/data/merged.csv'  # Reemplaza con la ruta a tu archivo CSV
df = pd.read_csv(csv_file)

# Paso 2: Configura la conexión a PostgreSQL
usuario = usuario  # Reemplaza con tu usuario de PostgreSQL
password = password  # Reemplaza con tu contraseña
host = host  # Reemplaza si tu base de datos está en otro servidor
puerto = puerto  # Puerto por defecto de PostgreSQL
db = db  # Reemplaza con el nombre de tu base de datos

# Crea la cadena de conexión
engine = create_engine(f'postgresql://{usuario}:{password}@{host}:{puerto}/{db}')

# Paso 3: Subir los datos del CSV a una tabla de PostgreSQL
# Puedes especificar el nombre de la tabla y si deseas reemplazar o añadir los datos
nombre_tabla = "merged"   # Reemplaza con el nombre de tu tabla
df.to_sql(nombre_tabla, engine, if_exists='replace', index=False)

print(f'Datos subidos a la tabla {nombre_tabla} correctamente')
