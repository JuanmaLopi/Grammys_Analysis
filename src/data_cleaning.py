import pandas as pd
import matplotlib.pyplot as plt

from db_conexion import establecer_conexion

conn, cursor = establecer_conexion()

# Consulta SQL para seleccionar los datos
query = "SELECT * FROM grammy_awards"

# Leer los datos en un DataFrame de pandas
grammy = pd.read_sql_query(query, conn)
grammy

# Reemplazar los valores nulos de Worker, por trabajador desconocido
grammy['workers'] = grammy['workers'].fillna("Unknown Worker")
print(grammy['workers'].isnull().sum())

# Reemplazar los valor nulos de nominee, por desconocido
grammy['nominee'] = grammy['nominee'].fillna("Unknown")
print(grammy['nominee'].isnull().sum())