import pandas as pd
import matplotlib.pyplot as plt

from db_conexion import establecer_conexion

conn, cursor = establecer_conexion()

# Consulta SQL para seleccionar los datos
query = "SELECT * FROM grammy_awards"

# Leer los datos en un DataFrame de pandas
df = pd.read_sql_query(query, conn)
df

# Reemplazar los valores nulos de Worker, por trabajador desconocido
df['workers'] = df['workers'].fillna("Unknown Worker")
print(df['workers'].isnull().sum())

# Reemplazar los valor nulos de nominee, por desconocido
df['nominee'] = df['nominee'].fillna("Unknown")
print(df['nominee'].isnull().sum())